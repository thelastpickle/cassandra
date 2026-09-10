#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Layer 3: the console runbook's "paste this and see whether the output looks right" steps, as assertions.
# Each was a paragraph ending in a judgement by eye, which is what produced the runbook's four wrong facts.
#
# Run it through ../Makefile, which fills the environment from `tofu output`:
#
#   make -C .jenkins/k8s/eks smoke
#   make -C .jenkins/k8s/eks smoke SMOKE_ARGS=--build
#
# Reads only, except --build, which submits a real build.
#
# --build passes $RUN_CI_ARGS to .build/run-ci, so a branch tracking no remote can still submit one:
#   RUN_CI_ARGS='-r https://github.com/apache/cassandra -b trunk'

set -o nounset
set -o pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cassandra_dir="$(cd "${here}/../../../.." && pwd)"

with_build=0
build_timeout=900
sample_seconds=0
# Set by --build's scale-up wait, read by the sampling section, which runs without --build too.  Declared
# here because `set -o nounset` aborts on an unset name.
scaled=""
# Where --build writes run-ci's output and the load samples.  $TMPDIR carries a trailing slash on macOS,
# and a path printed as /var/folders/…/T//jenkins-load.abc reads as a fault.
tmp_dir="${TMPDIR:-/tmp}"
tmp_dir="${tmp_dir%/}"
# The Jenkins chart's own label for the controller pod.  By label, not name, so a rename still matches.
jenkins_namespace="${JENKINS_NAMESPACE:-default}"
jenkins_controller_selector='app.kubernetes.io/component=jenkins-controller'
while [ $# -gt 0 ]; do
    case "$1" in
        --build) with_build=1; shift ;;
        --build-timeout) build_timeout="$2"; shift 2 ;;
        --sample-seconds) sample_seconds="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: smoke-test.sh [--build] [--build-timeout <seconds>] [--sample-seconds <seconds>]"
            echo
            echo "  --build           submit a real build through .build/run-ci and watch a pool scale from zero."
            echo "                    \$RUN_CI_ARGS is passed to run-ci, which needs -r and -b when the"
            echo "                    current branch tracks no remote."
            echo "  --build-timeout   how long to wait for that scale-up (default ${build_timeout}s)"
            echo "  --sample-seconds  record executors, queue depth and the controller's CPU and memory for"
            echo "                    this long (default ${sample_seconds}, meaning not at all). Needs no"
            echo "                    --build; use it alone to measure a build someone else started. This is"
            echo "                    how many agents the controller can drive. See README.md."
            exit 0 ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

# Everything ../Makefile exports.  Checked up front: a check comparing against an empty string always passes.
for name in EKS_CLUSTER_NAME AWS_REGION EKS_ADDON_NAMES EKS_AGENT_POOLS_FILE \
            EKS_CLUSTER_AUTOSCALER_NAMESPACE EKS_CONTROLLER_NODE_LABEL EKS_AGENT_NODE_GROUP_NAMES \
            EKS_ONDEMAND_VCPU_QUOTA EKS_ONDEMAND_VCPU_DEMAND EKS_MAX_NODES_TOTAL EKS_MAX_AGENT_NODES; do
    if [ -z "${!name:-}" ]; then
        echo "${name} is not set. Run this through ../Makefile, which fills it from \`tofu output\`." >&2
        exit 2
    fi
done

passed=0
failed=0

pass() { echo "PASS  $1"; passed=$((passed + 1)); }
fail() { echo "FAIL  $1"; failed=$((failed + 1)); }

echo "Cluster ${EKS_CLUSTER_NAME} in ${AWS_REGION}"
echo

# ---------------------------------------------------------------------------------------------------
# 1. The kubectl context is this cluster
# ---------------------------------------------------------------------------------------------------

# First, because no check below names the cluster it read: the wrong context answers all of them correctly,
# about something else.
context="$(kubectl config current-context 2>/dev/null)"
if [ -n "${context}" ] && [[ "${context}" == *"${EKS_CLUSTER_NAME}"* ]]; then
    pass "kubectl context names this cluster (${context})"
else
    fail "kubectl context is '${context:-none}', which does not name ${EKS_CLUSTER_NAME}"
    echo
    echo "Every check below would read another cluster. Stopping."
    echo "  aws eks update-kubeconfig --region ${AWS_REGION} --name ${EKS_CLUSTER_NAME} --alias ${EKS_CLUSTER_NAME}"
    exit 1
fi

# ---------------------------------------------------------------------------------------------------
# 2. A Ready node carries the controller label
# ---------------------------------------------------------------------------------------------------

# Not "a node exists".  The controller StatefulSet selects this label, so a NotReady or unlabelled node
# leaves the controller Pending on node affinity, with nothing wrong in Jenkins itself.
#
# `<name> <Ready status>` per node, counted by grep.  The filter cannot be nested: kubectl's jsonpath
# answers a nested filter with `unterminated filter` on stderr and nothing on stdout, which read here once
# as zero Ready nodes on a cluster that had one.
controller_nodes="$(kubectl get nodes -l "${EKS_CONTROLLER_NODE_LABEL}" \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{range .status.conditions[?(@.type=="Ready")]}{.status}{end}{"\n"}{end}' \
    | grep -c ' True$' || true)"
if [ "${controller_nodes}" -ge 1 ]; then
    pass "${controller_nodes} Ready node(s) carry ${EKS_CONTROLLER_NODE_LABEL}"
else
    fail "no Ready node carries ${EKS_CONTROLLER_NODE_LABEL}; the Jenkins controller cannot be scheduled"
fi

# ---------------------------------------------------------------------------------------------------
# 3. Every add-on is ACTIVE
# ---------------------------------------------------------------------------------------------------

# DEGRADED is what the runbook recorded and waited out.  ../1-cluster/addons.tf orders the add-ons so it
# does not happen, so a DEGRADED add-on here is a fact about this cluster, not about creation order.
for addon in ${EKS_ADDON_NAMES}; do
    status="$(aws eks describe-addon --region "${AWS_REGION}" --cluster-name "${EKS_CLUSTER_NAME}" \
        --addon-name "${addon}" --query 'addon.status' --output text 2>/dev/null)"
    if [ "${status}" = "ACTIVE" ]; then
        pass "add-on ${addon} is ACTIVE"
    else
        fail "add-on ${addon} is ${status:-unreadable}"
    fi
done

# ---------------------------------------------------------------------------------------------------
# 4. The cluster autoscaler is available
# ---------------------------------------------------------------------------------------------------

# By label, not name: the chart names the Deployment after its release, which ../2-platform/helmfile.yaml
# may rename.
autoscaler_available="$(kubectl -n "${EKS_CLUSTER_AUTOSCALER_NAMESPACE}" get deployment \
    -l app.kubernetes.io/name=aws-cluster-autoscaler \
    -o jsonpath='{.items[*].status.conditions[?(@.type=="Available")].status}' 2>/dev/null)"
if [[ "${autoscaler_available}" == *"True"* ]]; then
    pass "the cluster autoscaler Deployment is Available"
else
    fail "the cluster autoscaler Deployment is not Available (${autoscaler_available:-not found})"
fi

# ---------------------------------------------------------------------------------------------------
# 5. The status configmap names every agent pool
# ---------------------------------------------------------------------------------------------------

# An interface, not a diagnostic: `.build/run-ci` reads each pool's ceiling from it to check an instanceCap
# is reachable, and treats a pool it cannot find as unchecked rather than failed.  A pool missing here
# therefore drops a safety check silently.
#
# Matched as a substring, as run-ci does: the autoscaler names a group `eks-<nodegroup>-<uuid>`.
status="$(kubectl -n kube-system get configmap cluster-autoscaler-status \
    -o jsonpath='{.data.status}' 2>/dev/null)"
if [ -z "${status}" ]; then
    fail "the cluster-autoscaler-status configmap in kube-system is missing or empty"
    echo "        Without it .build/run-ci cannot establish any pool's ceiling, and leaves every"
    echo "        instanceCap unchecked. Confirm write-status-configmap in ../2-platform."
else
    for group in ${EKS_AGENT_NODE_GROUP_NAMES}; do
        if [[ "${status}" == *"${group}"* ]]; then
            pass "the autoscaler has discovered ${group}"
        else
            fail "the autoscaler has not discovered ${group}; check the ASG tags in ../1-cluster"
        fi
    done
fi

# ---------------------------------------------------------------------------------------------------
# 6. The public name, when there is one
# ---------------------------------------------------------------------------------------------------

# Skipped whole without jenkins_hostname in ../1-cluster: that cluster is reachable at its load balancer's
# own name, needing no DNS and no certificate.
#
# `:-` on every variable because an .eks-env written before these three outputs existed omits them, and
# `set -o nounset` would abort the whole run rather than report one check.
jenkins_hostname="${EKS_JENKINS_HOSTNAME:-}"
if [ -n "${jenkins_hostname}" ]; then
    echo

    # The certificate first: it gates whether ../Makefile configures TLS at all, so anything short of
    # ISSUED explains a Jenkins answering only plain HTTP.
    certificate_arn="${EKS_TLS_CERTIFICATE_ARN:-}"
    if [ -z "${certificate_arn}" ]; then
        fail "${jenkins_hostname} is configured but no certificate was created; re-run \`make apply\`"
    else
        # An empty answer is the AWS call failing; a status is the certificate's state.  Different messages,
        # because reported as one this said "delegation failure" for an expired credential, sending the
        # reader to a registrar that was already correct.
        certificate_status="$(aws acm describe-certificate --region "${AWS_REGION}" \
            --certificate-arn "${certificate_arn}" --query 'Certificate.Status' --output text 2>/dev/null)"
        if [ "${certificate_status}" = "ISSUED" ]; then
            pass "the certificate for ${jenkins_hostname} is ISSUED"
        elif [ -z "${certificate_status}" ]; then
            fail "the certificate for ${jenkins_hostname} could not be read, so its state is unknown"
            echo "        The AWS call itself failed. Check the credential before anything else; an expired"
            echo "        session is the common cause, and it answers every check in this section the same way."
            echo "        aws sts get-caller-identity"
        else
            fail "the certificate for ${jenkins_hostname} is ${certificate_status}, so Jenkins serves plain HTTP"
            echo "        ACM validates it by resolving a record it asked for, which works only once the"
            echo "        hosted zone is delegated at the registrar that holds the domain."
            echo "        tofu -chdir=1-cluster output dns_nameservers"
        fi
    fi

    # Read from Route 53, not from a resolver, so this asserts external-dns's one job: the record comes from
    # an annotation on the Jenkins Service, so its absence is a broken external-dns and not slow propagation.
    #
    # Three answers, not two.  `--output text` on `| [0].Name` prints the name, or the literal `None` when
    # nothing matched, or nothing at all when the call failed.  Only the middle is external-dns's fault.
    zone_id="${EKS_DNS_HOSTED_ZONE_ID:-}"
    if [ -z "${zone_id}" ]; then
        fail "EKS_DNS_HOSTED_ZONE_ID is empty, so the record for ${jenkins_hostname} cannot be checked"
    else
        record="$(aws route53 list-resource-record-sets --hosted-zone-id "${zone_id}" \
            --query "ResourceRecordSets[?Name=='${jenkins_hostname}.' && Type=='A'] | [0].Name" \
            --output text 2>/dev/null)"
        if [ "${record}" = "${jenkins_hostname}." ]; then
            pass "Route 53 holds an A record for ${jenkins_hostname}"
        elif [ -z "${record}" ]; then
            fail "the records in zone ${zone_id} could not be read, so the one for ${jenkins_hostname} is unknown"
            echo "        The AWS call itself failed. Check the credential, and then that it may read the zone."
            echo "        aws sts get-caller-identity"
        else
            fail "Route 53 holds no A record for ${jenkins_hostname}; external-dns has not written one"
            echo "        external-dns writes it from the Service annotation, which \`make jenkins\` puts there."
            echo "        Run that first if it has not run since the hostname was set, and then check both:"
            echo "        kubectl get pods -A -l app.kubernetes.io/name=external-dns   # then logs, in that namespace"
            # By label, not by name: the Service is named after the Helm release, which run-ci chooses.
            echo "        kubectl -n default get svc -l ${jenkins_controller_selector} -o yaml | grep external-dns"
        fi
    fi

    # The delegation, which every other check in this section waits on.
    #
    # Asked as "do the public NS records name this zone's own nameservers", not "does the hostname resolve".
    # A domain the registrar still answers for resolves, to a parking page or to whatever it pointed at
    # before; asked that way once, this passed on an undelegated domain whose certificate beside it was
    # PENDING_VALIDATION for want of exactly that delegation.
    #
    # Skipped without a resolver on this machine, which is not a fault in the cluster.
    if ! command -v dig >/dev/null 2>&1; then
        echo "      dig is not installed, so the delegation of ${jenkins_hostname} was not checked."
    elif [ -n "${zone_id}" ]; then
        zone_name="$(aws route53 get-hosted-zone --id "${zone_id}" \
            --query 'HostedZone.Name' --output text 2>/dev/null)"
        # Tab separated by `--output text`, and without the trailing dot that dig prints.
        zone_nameservers="$(aws route53 get-hosted-zone --id "${zone_id}" \
            --query 'DelegationSet.NameServers' --output text 2>/dev/null)"

        if [ -z "${zone_name}" ] || [ -z "${zone_nameservers}" ]; then
            fail "zone ${zone_id} could not be read, so its delegation is unknown"
            echo "        The AWS call itself failed. Check the credential first."
            echo "        aws sts get-caller-identity"
        else
            # The whole answer, not `+short`: the header separates "not delegated" from "this resolver would
            # not tell me", and both are an empty `+short`.  A resolver that filters the domain, the case
            # this was written against, answers SERVFAIL or nothing.
            #
            # NOERROR and NXDOMAIN answer about the delegation.  SERVFAIL, REFUSED and no header decline to.
            answer="$(dig +time=3 +tries=1 NS "${zone_name}" 2>/dev/null)"
            resolver_status="$(printf '%s' "${answer}" | grep -o 'status: [A-Z]*' | head -1 \
                | cut -d' ' -f2)"
            resolver_used="this machine's resolver"

            # A resolver that declines has said nothing about the zone, so ask one outside this network's
            # filtering.  The delegation is a fact about the registrar; every resolver that answers agrees.
            if [ "${resolver_status}" != "NOERROR" ] && [ "${resolver_status}" != "NXDOMAIN" ]; then
                public_answer="$(dig +time=3 +tries=1 @8.8.8.8 NS "${zone_name}" 2>/dev/null)"
                public_status="$(printf '%s' "${public_answer}" | grep -o 'status: [A-Z]*' | head -1 \
                    | cut -d' ' -f2)"
                if [ "${public_status}" = "NOERROR" ] || [ "${public_status}" = "NXDOMAIN" ]; then
                    echo "      This machine's resolver would not answer for ${zone_name}" \
                         "(${resolver_status:-no reply}), so 8.8.8.8 was asked instead."
                    answer="${public_answer}"
                    resolver_status="${public_status}"
                    resolver_used="8.8.8.8"
                fi
            fi

            delegated=0
            for nameserver in ${zone_nameservers}; do
                case "${answer}" in
                    *"${nameserver}"*) delegated=1; break ;;
                esac
            done

            if [ "${delegated}" -eq 1 ]; then
                pass "the zone ${zone_name} is delegated to its Route 53 nameservers (per ${resolver_used})"
            elif [ "${resolver_status}" != "NOERROR" ] && [ "${resolver_status}" != "NXDOMAIN" ]; then
                # Not a failure: no reachable resolver would answer, so the delegation is unknown rather than
                # wrong.  Exiting 1 on every filtered network teaches the reader to ignore the exit code.
                echo "      No resolver would answer for ${zone_name} (${resolver_status:-no reply} from"
                echo "      this machine's resolver, and 8.8.8.8 did not answer either), so the delegation"
                echo "      was not checked. That is a fact about this network, not about the zone: a"
                echo "      resolver that filters the domain, or a proxy in front of it, answers this way."
                echo "      Settle it from a network that resolves: dig ${zone_name} NS"
            else
                fail "the zone ${zone_name} is not delegated; nothing else in this section can pass"
                echo "        NS records per ${resolver_used}: ${resolver_status}, and none of this zone's four."
                echo "        Set these at the registrar that holds the domain:"
                for nameserver in ${zone_nameservers}; do echo "          ${nameserver}"; done
            fi
        fi
    fi
fi

# ---------------------------------------------------------------------------------------------------
# 7. One agent fits on one node of its pool
# ---------------------------------------------------------------------------------------------------

# Measured against a running node where a pool has one, modelled where it sits at zero; check-pool-fit.py
# prints which.
#
# Three statuses, and the third is why this is not a plain `report`: 0 fits, 1 does not, 2 could not be read.
# A 2 reported as "does not fit" is a wrong fact, and pool sizes are what somebody would change over it.
echo
"${here}/check-pool-fit.py" --pools "${EKS_AGENT_POOLS_FILE}" --region "${AWS_REGION}" \
    --kubecontext "${context}"
fit_status=$?
if [ "${fit_status}" -eq 0 ]; then
    pass "every agent pool fits at least one agent"
elif [ "${fit_status}" -eq 2 ]; then
    fail "whether every agent pool fits could not be established; see the message above"
else
    fail "an agent pool fits no agent"
fi
echo

# ---------------------------------------------------------------------------------------------------
# 8. The account's vCPU quota holds the agents Jenkins may ask for
# ---------------------------------------------------------------------------------------------------

# The one ceiling no layer reports for itself: the node group knows max_size, the autoscaler knows the node
# group, Jenkins knows its instanceCap, and none of them knows the account's vCPU quota.  So a cluster
# written for 310 nodes in an account allowing 48 looks correct at every layer and stalls at runtime.
#
# Demand above the quota is reported and does not fail: that cluster runs, smaller than written for.  A quota
# too small for one agent does fail, no build being possible.
#
# The figures are the lower of the account's ceiling and the pools' own max_size, not the quota over a node.
# See ../vcpu-quota.py, which derives them; ../Makefile exports them.
echo
echo "Agent ceiling ${EKS_MAX_AGENT_NODES}, and ${EKS_MAX_NODES_TOTAL} nodes with the controller," \
     "against an account quota of ${EKS_ONDEMAND_VCPU_QUOTA} vCPU and a pool ask of" \
     "${EKS_ONDEMAND_VCPU_DEMAND} vCPU."
if [ "${EKS_MAX_AGENT_NODES}" -lt 1 ]; then
    fail "no agent node can be created, so no build can run"
    echo "        \`make quota\` says whether that is the account's quota or a pool at max_size 0."
elif [ "${EKS_ONDEMAND_VCPU_DEMAND}" -gt "${EKS_ONDEMAND_VCPU_QUOTA}" ]; then
    pass "the vCPU quota holds ${EKS_MAX_AGENT_NODES} agent node(s)"
    echo "      The node groups may ask for ${EKS_ONDEMAND_VCPU_DEMAND} vCPU, which is more than the"
    echo "      account's ${EKS_ONDEMAND_VCPU_QUOTA}, so the pools cannot all reach max_size. What a build"
    echo "      sees is a pod that stays Pending. README.md has the request command; \`make quota\` shows"
    echo "      the arithmetic."
else
    pass "the vCPU quota holds every node the pools may create"
fi

# ---------------------------------------------------------------------------------------------------
# 9. The Metrics API answers
# ---------------------------------------------------------------------------------------------------

# `kubectl top` is the only way to read the controller's CPU and memory here, and EKS installs nothing that
# serves it; ../2-platform/helmfile.yaml adds metrics-server for this.
#
# Asserted rather than assumed because its absence is quiet: without it, "how many agents can this controller
# drive" is unmeasurable and only the JVM's GC log is left.
if kubectl top node --no-headers >/dev/null 2>&1; then
    pass "the Metrics API answers, so \`kubectl top\` can measure the controller"
else
    fail "the Metrics API does not answer; \`kubectl top\` reports nothing about this cluster"
    echo "        metrics-server is what serves it. Confirm the release in ../2-platform/helmfile.yaml,"
    echo "        and then why its pod is unready:"
    echo "        kubectl -n kube-system get deploy metrics-server"
    echo "        kubectl -n kube-system logs deploy/metrics-server"
fi

# ---------------------------------------------------------------------------------------------------
# 10. The spend guard, and whether it has stopped the pools
# ---------------------------------------------------------------------------------------------------

# Two questions, and the second is the one that saves an afternoon.
#
# Is anything watching what this cluster spends?  No quota is a limit on money, and these pools are a few
# hundred dollars an hour at full size.
#
# And is the brake on right now?  A braked cluster has every symptom of a broken autoscaler: pods Pending,
# node groups at zero, nothing launching, and Auto Scaling activities that say nothing at all, because a
# suspended Launch produces no failed activity to read.  Diagnosed from the cluster it looks like a fault; it
# is the guard doing its job, and this is where that is stated.
#
# `:-` on every variable, because an .eks-env written before these outputs existed omits them and
# `set -o nounset` would abort the whole run rather than skip one section.
echo
if [ -z "${EKS_SPEND_CAPS_PARAMETER:-}" ]; then
    echo "      No spend cap is set, so nothing watches what this cluster spends. That is a choice and"
    echo "      not a fault, and it is not counted below. \`make caps\` asks for the caps."
else
    # The function first: everything else in this section is about a Lambda that has to exist.
    guard_state="$(aws lambda get-function-configuration --region "${AWS_REGION}" \
        --function-name "${EKS_SPEND_GUARD_FUNCTION:-}" --query 'State' --output text 2>/dev/null)"
    if [ "${guard_state}" = "Active" ]; then
        pass "the spend guard ${EKS_SPEND_GUARD_FUNCTION} is Active"
    else
        fail "the spend guard is ${guard_state:-unreadable}, so nothing is comparing spend against the caps"
        echo "        The caps are in ${EKS_SPEND_CAPS_PARAMETER}, and \`make apply\` is what creates the"
        echo "        function that reads them."
    fi

    # A rule that exists and is DISABLED is the failure worth naming: the function is there, the caps are
    # there, and nothing invokes it.  Nothing in the console shows that as an error.
    rule_state="$(aws events describe-rule --region "${AWS_REGION}" \
        --name "${EKS_SPEND_GUARD_FUNCTION:-}" --query 'State' --output text 2>/dev/null)"
    if [ "${rule_state}" = "ENABLED" ]; then
        pass "its schedule is ENABLED, every ${EKS_SPEND_GUARD_INTERVAL_MINUTES:-?} minutes"
    else
        fail "its schedule is ${rule_state:-unreadable}, so the guard is not being invoked"
        echo "        aws events describe-rule --name ${EKS_SPEND_GUARD_FUNCTION:-} --region ${AWS_REGION}"
    fi

    # The alarm on the guard's own heartbeat, which is how a guard that has stopped running is told apart
    # from a cluster that has no guard.  INSUFFICIENT_DATA is separated from ALARM: the first is a guard that
    # has never reported, which is what a deploy in the last half hour looks like.
    alarm_state="$(aws cloudwatch describe-alarms --region "${AWS_REGION}" \
        --alarm-names "${EKS_SPEND_STALLED_ALARM:-}" --query 'MetricAlarms[0].StateValue' \
        --output text 2>/dev/null)"
    case "${alarm_state}" in
        OK)
            pass "the guard has reported a spend figure inside the last 30 minutes" ;;
        INSUFFICIENT_DATA)
            fail "the guard has not yet reported a spend figure, so the caps are not yet in force"
            echo "        Half an hour after \`make apply\` this is a fault. Sooner than that it is the"
            echo "        alarm waiting for its first datapoint. Read the function's own log:"
            echo "        aws logs tail /aws/lambda/${EKS_SPEND_GUARD_FUNCTION:-} --since 30m" ;;
        ALARM)
            fail "the guard has reported nothing for 30 minutes, so nothing is watching spend"
            echo "        aws logs tail /aws/lambda/${EKS_SPEND_GUARD_FUNCTION:-} --since 1h" ;;
        *)
            fail "the alarm ${EKS_SPEND_STALLED_ALARM:-} is ${alarm_state:-unreadable}, so whether the guard is running is unknown"
            echo "        The AWS call itself failed. Check the credential first: aws sts get-caller-identity" ;;
    esac

    # The figures, from the same script and the same arithmetic the guard brakes on.  Three statuses, as with
    # check-pool-fit.py: 0 under every cap, 1 over one or braked, 2 spend could not be established.
    echo
    "${here}/../spend-guard.py" --report
    spend_status=$?
    if [ "${spend_status}" -eq 0 ]; then
        pass "spend is under every cap, and the agent pools are not stopped"
    elif [ "${spend_status}" -eq 2 ]; then
        fail "what this account has spent could not be established; see the message above"
        echo "        The guard treats that as over the cap and brakes the pools, so this stops builds."
    else
        fail "a cap is met, or the agent pools are stopped; see the figures above"
        echo "        This is the guard working rather than a fault in the cluster. No agent node will"
        echo "        start until the window rolls over or a cap is raised:"
        echo "        aws ssm put-parameter --name ${EKS_SPEND_CAPS_PARAMETER} --overwrite \\"
        echo "            --type String --value '{\"daily\":<usd>,\"weekly\":<usd>,\"monthly\":<usd>}'"
        echo "        A cap raised that way is put back by the next \`make apply\`; \`make caps\` is what"
        echo "        changes it for good."
    fi
fi

# ---------------------------------------------------------------------------------------------------
# 11. Opt in: a real build, and a pool scaling up from zero
# ---------------------------------------------------------------------------------------------------

if [ "${with_build}" -eq 1 ]; then
    # A full path with the template at the end, which BSD and GNU mktemp read alike: BSD reads `-t` as a
    # prefix and appends its own suffix, leaving a literal XXXXXX in the name.
    log="$(mktemp "${tmp_dir}/run-ci-smoke.XXXXXX")"
    echo "Submitting a build through .build/run-ci, logging to ${log}"
    # ${RUN_CI_ARGS} unquoted on purpose: it carries several words, as `-r <url> -b <branch>`.  Passed through
    # because run-ci cannot detect what to build on a branch tracking no remote, and would exit without
    # submitting; requiring a pushed branch is the wrong constraint on a cluster test.
    #
    # A direct child of this script, not `nohup ... & echo $!` in a subshell.  That form orphans run-ci when
    # the subshell exits, and an orphaned child on macOS fails every outbound connection with `OSError:
    # [Errno 9] Bad file descriptor`, a traceback from run-ci's first EKS call that reads as an expired
    # credential.  Reproduced with a three-line Python probe, so it is the launch and not run-ci.  `exec`
    # keeps $! naming run-ci rather than the subshell that chdirs for it.
    ( cd "${cassandra_dir}" && exec .build/run-ci ${RUN_CI_ARGS:-} ) >"${log}" 2>&1 &
    build_pid=$!
    run_ci_exited=0

    # Nodes, not pods.  A pending pod proves Jenkins asked; a node appearing proves the autoscaler understood,
    # which is what the ASG tags in ../1-cluster and the image tag in ../2-platform decide.  Scale-up from
    # zero is the one path no offline check reaches.
    echo "Waiting up to ${build_timeout}s for an agent pool to scale up from zero"
    scaled=""
    deadline=$((SECONDS + build_timeout))
    while [ "${SECONDS}" -lt "${deadline}" ]; do
        scaled="$(kubectl get nodes -l cassandra.jenkins.agent=true -o name 2>/dev/null | head -1)"
        [ -n "${scaled}" ] && break
        if ! kill -0 "${build_pid}" 2>/dev/null; then
            run_ci_exited=1
            break
        fi
        sleep 15
    done

    if [ -n "${scaled}" ]; then
        pass "an agent pool scaled up from zero (${scaled})"
    elif [ "${run_ci_exited}" -eq 1 ]; then
        # Separate from the timeout below, saying nothing about the cluster: run-ci refused the submission and
        # this machine never asked Jenkins anything.  Its reason is printed, not named as a path.
        fail "run-ci exited before it submitted a build, so nothing here tested the cluster"
        echo "        Its last lines:"
        # `\r` to newlines first, then escape sequences and the spinner out.  run-ci draws progress by
        # rewriting one physical line, so `tail` on the raw file returns one line holding every redraw and
        # hides the line that says why run-ci stopped.
        tr '\r' '\n' < "${log}" \
            | sed $'s/\033\\[[0-9;?]*[a-zA-Z]//g' \
            | grep -v -e '^[[:space:]]*$' -e 'Waiting for build to complete' \
            | tail -12 | sed 's/^/        | /'
        echo "        A branch that tracks no remote is the usual cause. Name what to build instead:"
        echo "        make smoke SMOKE_ARGS=--build \\"
        echo "          RUN_CI_ARGS='-r https://github.com/apache/cassandra -b trunk'"
    else
        fail "no agent node appeared within ${build_timeout}s"
        echo "        Read ${log} first: a build that never queued an agent is a Jenkins problem, and a"
        echo "        pod stuck Pending with a scale-up message is an autoscaler one."
        echo "        kubectl -n ${EKS_CLUSTER_AUTOSCALER_NAMESPACE} logs -l app.kubernetes.io/name=aws-cluster-autoscaler"
    fi

    # Left running: a skinny profile takes hours, and killing run-ci would abandon the Jenkins build and
    # strand its agent pods.
    if kill -0 "${build_pid}" 2>/dev/null; then
        echo
        echo "The build is still running as pid ${build_pid}. Follow it with \`tail -f ${log}\`,"
        echo "or abort it in Jenkins; do not kill run-ci, which would leave the build and its agents behind."
    fi
fi

# ---------------------------------------------------------------------------------------------------
# 12. Opt in: how many executors the controller actually drives
# ---------------------------------------------------------------------------------------------------

# "Do we need more agents" cannot be answered from the cluster's shape.  It needs three numbers through a
# real build: busy executors, queue depth behind them, and the controller's own CPU and memory while both.
# Jenkins publishes the first two, the Metrics API the third, and nothing records any of them.
#
# Outside section 11 so that --sample-seconds works without --build: the build that drives the controller
# near its ceiling is one an operator starts in Jenkins.  With --build, the scale-up must have happened
# first, or a build that never got a node records as an idle cluster.
#
# Read anonymously, which jenkins-deployment.yaml's allowAnonymousRead permits.  A cluster without it is
# not sampled, rather than prompting for a credential mid-run.
if [ "${sample_seconds}" -gt 0 ] && { [ "${with_build}" -eq 0 ] || [ -n "${scaled}" ]; }; then
    jenkins_address="$(kubectl -n "${jenkins_namespace}" get svc \
        -l "${jenkins_controller_selector}" \
        -o jsonpath='{.items[0].status.loadBalancer.ingress[0].hostname}' 2>/dev/null)"
    samples="$(mktemp "${tmp_dir}/jenkins-load.XXXXXX")"
    if [ -z "${jenkins_address}" ]; then
        echo "      The Jenkins Service has no load balancer address yet, so nothing was sampled."
    else
        echo
        echo "Sampling executors, queue depth and the controller for ${sample_seconds}s -> ${samples}"
        echo "seconds,busy_executors,total_executors,queue_length,controller_cpu,controller_memory" \
            > "${samples}"
        sample_deadline=$((SECONDS + sample_seconds))
        peak_busy=0 peak_total=0 peak_queue=0
        # Once each, not every fifteen seconds for two hours.  A blank column and a column of zeros look
        # alike in the file, and the difference is the answer: zero executors is an idle cluster, blank is
        # nothing asked.
        warned_jenkins=0 warned_top=0
        # The controller is read once a minute against the Jenkins figures' fifteen seconds.  Every `kubectl`
        # runs the kubeconfig's exec credential helper, and four AWS token requests a minute made
        # CreateOAuth2Token answer `429 Rate exceeded`, which then denied every other kubectl against this
        # cluster for minutes.  One minute is finer than anything read from these two columns.
        #
        # By elapsed time, not iteration count: an iteration is 15s only when Jenkins answers at once, and a
        # loaded controller can hold two 45s requests, stretching a four-iteration rule to seven minutes.
        top_min_interval=60
        next_top=0
        while [ "${SECONDS}" -lt "${sample_deadline}" ]; do
            # One call for both figures, so the pair is consistent.  `tr` and `grep` rather than a JSON
            # parser: jq is not a dependency here, and both fields are top-level integers.
            #
            # 45 seconds, not 10.  Rendering /computer enumerates every connected agent, and at 263 of them
            # the controller took over 10s, so these columns went blank on the rows that mattered most while
            # the queue column beside them read fine.
            computer="$(curl -fsS --max-time 45 \
                "http://${jenkins_address}/computer/api/json?tree=busyExecutors,totalExecutors" \
                2>/dev/null)"
            busy="$(printf '%s' "${computer}" | tr ',{}' '\n\n\n' \
                | grep -o '"busyExecutors":[0-9]*' | cut -d: -f2)"
            total="$(printf '%s' "${computer}" | tr ',{}' '\n\n\n' \
                | grep -o '"totalExecutors":[0-9]*' | cut -d: -f2)"
            # The queue length, which `busyExecutors` cannot show: a saturated controller and an idle one
            # both report every executor busy.
            #
            # -g, because curl reads `[` and `]` in a URL as a glob range whatever the shell quoting.  Without
            # it curl exits 3 before sending anything and the count reads 0 for the whole sample.
            #
            # curl's status decides whether the count is written, `grep -c` being read only after it: a failed
            # request leaves grep nothing to count, so it prints 0, which reads afterwards as a drained queue.
            # That is how a file recorded the queue emptying four times while it never fell below a thousand.
            #
            # `items[id]` not `task[name]`: the names put this past 30s at 1,200 queued items, against 1.5s.
            queue=""
            if queue_json="$(curl -fsS -g --max-time 45 \
                "http://${jenkins_address}/queue/api/json?tree=items[id]" 2>/dev/null)"; then
                queue="$(printf '%s' "${queue_json}" | tr ',' '\n' | grep -c '"id"')"
            fi
            # Blank on the rows that do not read it; see top_min_interval.  $top_read keeps the warning below
            # from firing there, where a blank is intended.
            cpu="" memory="" top_read=0
            if [ "${SECONDS}" -ge "${next_top}" ]; then
                top_read=1
                next_top=$((SECONDS + top_min_interval))
                # Tab separated by kubectl, and absent when the Metrics API is not serving.
                top="$(kubectl -n "${jenkins_namespace}" --request-timeout=30s \
                    top pod -l "${jenkins_controller_selector}" --no-headers 2>/dev/null | head -1)"
                cpu="$(printf '%s' "${top}" | awk '{print $2}')"
                memory="$(printf '%s' "${top}" | awk '{print $3}')"
            fi
            echo "${SECONDS},${busy:-},${total:-},${queue},${cpu},${memory}" >> "${samples}"

            if { [ -z "${busy:-}" ] || [ -z "${queue}" ]; } && [ "${warned_jenkins}" -eq 0 ]; then
                warned_jenkins=1
                echo "      WARNING: Jenkins did not answer, so a column is blank on that row. A blank is not"
                echo "               a zero; if this repeats, read the rows and not the peak below."
                echo "               curl -s 'http://${jenkins_address}/computer/api/json'"
            fi
            if [ "${top_read}" -eq 1 ] && [ -z "${cpu}" ] && [ "${warned_top}" -eq 0 ]; then
                warned_top=1
                echo "      WARNING: \`kubectl top pod\` returned nothing, so the controller columns are blank"
                echo "               on the rows that read it. Usually an expired credential. Sampling"
                echo "               continues, rather than stopping a build already running."
                echo "               aws sts get-caller-identity"
            fi

            [ -n "${busy:-}" ] && [ "${busy}" -gt "${peak_busy}" ] && peak_busy="${busy}"
            [ -n "${total:-}" ] && [ "${total}" -gt "${peak_total}" ] && peak_total="${total}"
            [ -n "${queue}" ] && [ "${queue}" -gt "${peak_queue}" ] && peak_queue="${queue}"
            sleep 15
        done
        echo "      Peak ${peak_busy} of ${peak_total} executors busy, with ${peak_queue} task(s)" \
             "queued behind them."
        echo "      A queue that stays deep while every executor is busy wants more agents. A shallow queue"
        echo "      with idle executors does not; look at the pipeline instead."
        echo "      The controller columns are filled once a minute, so three rows in four are blank there."
        echo "      Keep sampling past this window with:"
        echo "        while sleep 15; do curl -s 'http://${jenkins_address}/computer/api/json?tree=busyExecutors,totalExecutors'; echo; done"
    fi
fi

# ---------------------------------------------------------------------------------------------------

echo
echo "${passed} passed, ${failed} failed."
[ "${failed}" -eq 0 ] || exit 1
