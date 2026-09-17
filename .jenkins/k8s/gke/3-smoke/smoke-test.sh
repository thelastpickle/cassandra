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

for name in GKE_CLUSTER_NAME GOOGLE_PROJECT GOOGLE_REGION GKE_LOCATION GKE_AGENT_POOLS_FILE \
            GKE_CONTROLLER_NODE_LABEL GKE_AGENT_NODE_POOL_NAMES \
            GKE_VCPU_QUOTA GKE_VCPU_QUOTA_METRIC GKE_VCPU_DEMAND \
            GKE_MAX_NODES_TOTAL GKE_MAX_AGENT_NODES; do
    if [ -z "${!name:-}" ]; then
        echo "${name} is not set. Run this through ../Makefile, which fills it from \`tofu output\`." >&2
        exit 2
    fi
done

passed=0
failed=0

pass() { echo "PASS  $1"; passed=$((passed + 1)); }
fail() { echo "FAIL  $1"; failed=$((failed + 1)); }

echo "Cluster ${GKE_CLUSTER_NAME} in ${GOOGLE_REGION}, project ${GOOGLE_PROJECT}"
echo

context="$(kubectl config current-context 2>/dev/null)"
expected_context="gke_${GOOGLE_PROJECT}_${GKE_LOCATION}_${GKE_CLUSTER_NAME}"
if [ "${context}" = "${expected_context}" ]; then
    pass "kubectl context names this cluster (${context})"
else
    fail "kubectl context is '${context:-none}', which does not name ${GKE_CLUSTER_NAME}"
    echo
    echo "Every check below would read another cluster. Stopping."
    echo "  gcloud container clusters get-credentials ${GKE_CLUSTER_NAME} --region ${GKE_LOCATION} --project ${GOOGLE_PROJECT}"
    exit 1
fi

controller_nodes="$(kubectl --context "${context}" get nodes -l "${GKE_CONTROLLER_NODE_LABEL}" \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{range .status.conditions[?(@.type=="Ready")]}{.status}{end}{"\n"}{end}' \
    | grep -c ' True$' || true)"
if [ "${controller_nodes}" -ge 1 ]; then
    pass "${controller_nodes} Ready node(s) carry ${GKE_CONTROLLER_NODE_LABEL}"
else
    fail "no Ready node carries ${GKE_CONTROLLER_NODE_LABEL}; the Jenkins controller cannot be scheduled"
fi

cluster_facts="$(gcloud container clusters describe "${GKE_CLUSTER_NAME}" \
    --project "${GOOGLE_PROJECT}" --region "${GKE_LOCATION}" \
    --format='value(status,addonsConfig.httpLoadBalancing.disabled,addonsConfig.gcePersistentDiskCsiDriverConfig.enabled,privateClusterConfig.enablePrivateNodes,network,subnetwork)' \
    2>/dev/null)"
cluster_status="$(printf '%s' "${cluster_facts}" | cut -f1)"
http_lb_disabled="$(printf '%s' "${cluster_facts}" | cut -f2)"
pd_csi_enabled="$(printf '%s' "${cluster_facts}" | cut -f3)"
private_nodes="$(printf '%s' "${cluster_facts}" | cut -f4)"
cluster_network="$(printf '%s' "${cluster_facts}" | cut -f5)"
cluster_subnetwork="$(printf '%s' "${cluster_facts}" | cut -f6)"

if [ -z "${cluster_status}" ]; then
    fail "cluster ${GKE_CLUSTER_NAME} could not be read, so neither its status nor its add-ons are known"
    echo "        The gcloud call itself failed. Check the credential before anything else; an expired one"
    echo "        answers every gcloud check in this script the same way."
    echo "        gcloud auth list && gcloud config get-value project"
elif [ "${cluster_status}" = "RUNNING" ]; then
    pass "cluster ${GKE_CLUSTER_NAME} is RUNNING"
else
    fail "cluster ${GKE_CLUSTER_NAME} is ${cluster_status}, so the control plane is not serving normally"
    echo "        RECONCILING is an upgrade or a resize in flight and clears itself; DEGRADED and ERROR do"
    echo "        not. Neither is anything this configuration can fix:"
    echo "        gcloud container clusters describe ${GKE_CLUSTER_NAME} --region ${GKE_LOCATION} --format='value(status,statusMessage)'"
fi

if [ -n "${cluster_status}" ]; then
    # The GKE values use a Service load balancer. Only an actual GCE Ingress needs this add-on.
    if ! ingress_facts="$(kubectl --context "${context}" -n "${jenkins_namespace}" get ingress \
        -l "${jenkins_controller_selector}" -o json 2>/dev/null)" \
        || ! ingress_count="$(printf '%s' "${ingress_facts}" | jq -er '[.items[] |
            select((.metadata.annotations["kubernetes.io/ingress.class"] // .spec.ingressClassName // "")
                   | . == "" or . == "gce" or . == "gce-internal")] | length' 2>/dev/null)"; then
        fail "Jenkins Ingress resources could not be read, so the HTTP load balancing requirement is unknown"
    elif [ "${ingress_count}" -eq 0 ]; then
        pass "no Jenkins Ingress requires the HTTP load balancing add-on"
    elif [ "${http_lb_disabled}" = "True" ]; then
        fail "the HTTP load balancing add-on is disabled, so the Jenkins Ingress has no controller"
        echo "        Run \`make jenkins\` to remove the unused Ingress with the current GKE values."
        echo "        Jenkins uses the Service load balancer. A custom GCE Ingress needs HttpLoadBalancing enabled."
    else
        pass "the HTTP load balancing add-on is enabled"
    fi

    if [ "${pd_csi_enabled}" = "True" ]; then
        pass "the persistent disk CSI driver add-on is enabled"
    else
        fail "the persistent disk CSI driver add-on is disabled, so no PersistentVolume can be provisioned"
        echo "        The controller's 500Gi jenkins_home claim and the pd-ssd storage class in ../2-platform"
        echo "        both need it. The claim stays Pending, and the controller pod with it."
        echo "        gcloud container clusters update ${GKE_CLUSTER_NAME} --region ${GKE_LOCATION} --update-addons GcePersistentDiskCsiDriver=ENABLED"
    fi
fi

node_pools="$(gcloud container node-pools list --cluster "${GKE_CLUSTER_NAME}" \
    --project "${GOOGLE_PROJECT}" --region "${GKE_LOCATION}" \
    --format='value(name,autoscaling.enabled,autoscaling.maxNodeCount)' 2>/dev/null)"

if [ -z "${node_pools}" ]; then
    fail "the node pools of ${GKE_CLUSTER_NAME} could not be listed, so their autoscaling is unknown"
    echo "        The gcloud call itself failed. Check the credential first:"
    echo "        gcloud container node-pools list --cluster ${GKE_CLUSTER_NAME} --region ${GKE_LOCATION}"
else
    for pool in ${GKE_AGENT_NODE_POOL_NAMES}; do
        pool_autoscaling="$(printf '%s\n' "${node_pools}" | awk -F'\t' -v want="${pool}" \
            '$1 == want { print $2 }')"
        if [ "${pool_autoscaling}" = "True" ]; then
            pass "node pool ${pool} has autoscaling enabled"
        else
            fail "node pool ${pool} has autoscaling ${pool_autoscaling:-off or absent}, so it will not grow"
            echo "        An agent pool rests at zero nodes, so autoscaling off means every agent pod for it"
            echo "        stays Pending with no scale-up ever attempted."
            echo "        gcloud container node-pools describe ${pool} --cluster ${GKE_CLUSTER_NAME} --region ${GKE_LOCATION}"
        fi
    done
fi

if [ -n "${node_pools}" ]; then
    for pool in ${GKE_AGENT_NODE_POOL_NAMES}; do
        pool_max="$(printf '%s\n' "${node_pools}" | awk -F'\t' -v want="${pool}" '$1 == want { print $3 }')"
        if [ -z "${pool_max}" ]; then
            fail "node pool ${pool} is not among this cluster's pools, or has no autoscaling maximum"
            echo "        ../Makefile took the name from \`tofu output\`, so a pool missing here is a pool"
            echo "        ../1-cluster believes it created. Compare the two:"
            echo "        gcloud container node-pools list --cluster ${GKE_CLUSTER_NAME} --region ${GKE_LOCATION}"
        elif [ "${pool_max}" -ge 1 ]; then
            pass "node pool ${pool} may reach ${pool_max} node(s)"
        else
            fail "node pool ${pool} has an autoscaling maximum of ${pool_max}, so it can hold no agent"
            echo "        A maximum of zero is what the spend guard's brake sets. If no cap is in force,"
            echo "        \`make quota\` says whether the project's quota clamped the pool to nothing."
            echo "        ../spend-guard.py --report"
        fi
    done
fi

jenkins_hostname="${GKE_JENKINS_HOSTNAME:-}"
if [ -n "${jenkins_hostname}" ]; then
    echo

    # The certificate first: it gates whether ../Makefile configures TLS at all, so anything short of ACTIVE
    # explains a Jenkins answering only plain HTTP.
    certificate_map="${GKE_TLS_CERTIFICATE_MAP:-}"
    certificate="${GKE_TLS_CERTIFICATE_NAME:-}"
    if [ -z "${certificate_map}" ] && [ -z "${certificate}" ]; then
        fail "${jenkins_hostname} is configured but no certificate was created; re-run \`make apply\`"
    else
        if [ -z "${certificate}" ]; then
            certificate="$(gcloud certificate-manager maps entries list --map="${certificate_map}" \
                --location=global --project "${GOOGLE_PROJECT}" --format='value(certificates)' 2>/dev/null \
                | head -1 | tr ';' '\n' | head -1)"
            certificate="${certificate##*/}"
            [ -n "${certificate}" ] || certificate="${certificate_map}"
        fi

        certificate_state="$(gcloud certificate-manager certificates describe "${certificate}" \
            --location=global --project "${GOOGLE_PROJECT}" --format='value(managed.state)' 2>/dev/null)"
        if [ "${certificate_state}" = "ACTIVE" ]; then
            pass "the certificate for ${jenkins_hostname} is ACTIVE"
        elif [ -z "${certificate_state}" ]; then
            fail "the certificate for ${jenkins_hostname} could not be read, so its state is unknown"
            echo "        The gcloud call itself failed. Check the credential before anything else; an"
            echo "        expired one answers every check in this section the same way."
            echo "        gcloud auth list"
            echo "        gcloud certificate-manager maps entries list --map=${certificate_map} --location=global"
        else
            fail "the certificate for ${jenkins_hostname} is ${certificate_state}, so Jenkins serves plain HTTP"
            echo "        Certificate Manager validates a managed certificate by resolving a record it asked"
            echo "        for, which works only once the zone is delegated at the registrar that holds the"
            echo "        domain. PROVISIONING that never becomes ACTIVE is that delegation missing."
            echo "        tofu -chdir=1-cluster output dns_nameservers"
        fi
    fi

    dns_zone="${GKE_DNS_MANAGED_ZONE:-}"
    if [ -z "${dns_zone}" ]; then
        fail "GKE_DNS_MANAGED_ZONE is empty, so the record for ${jenkins_hostname} cannot be checked"
    else
        if record="$(gcloud dns record-sets list --zone "${dns_zone}" --project "${GOOGLE_PROJECT}" \
                --name "${jenkins_hostname}." --type A --format='value(name)' 2>/dev/null)"; then
            if [ -n "${record}" ]; then
                pass "Cloud DNS holds an A record for ${jenkins_hostname}"
            else
                fail "Cloud DNS holds no A record for ${jenkins_hostname}; external-dns has not written one"
                echo "        external-dns writes it from the Service annotation, which \`make jenkins\` puts"
                echo "        there. Run that first if it has not run since the hostname was set, then check both:"
                echo "        kubectl get pods -A -l app.kubernetes.io/name=external-dns   # then logs, in that namespace"
                # By label, not by name: the Service is named after the Helm release, which run-ci chooses.
                echo "        kubectl -n ${jenkins_namespace} get svc -l ${jenkins_controller_selector} -o yaml | grep external-dns"
            fi
        else
            fail "the records in zone ${dns_zone} could not be read, so the one for ${jenkins_hostname} is unknown"
            echo "        The gcloud call itself failed. Check the credential, and then that it may read the zone."
            echo "        gcloud dns managed-zones describe ${dns_zone}"
        fi
    fi

    if ! command -v dig >/dev/null 2>&1; then
        echo "      dig is not installed, so the delegation of ${jenkins_hostname} was not checked."
    elif [ -n "${dns_zone}" ]; then
        zone_facts="$(gcloud dns managed-zones describe "${dns_zone}" --project "${GOOGLE_PROJECT}" \
            --format='value(dnsName,nameServers)' 2>/dev/null)"
        zone_name="$(printf '%s' "${zone_facts}" | cut -f1)"
        zone_nameservers="$(printf '%s' "${zone_facts}" | cut -f2 | tr ';' ' ')"

        if [ -z "${zone_name}" ] || [ -z "${zone_nameservers}" ]; then
            fail "zone ${dns_zone} could not be read, so its delegation is unknown"
            echo "        The gcloud call itself failed. Check the credential first."
            echo "        gcloud auth list"
        else
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
                pass "the zone ${zone_name} is delegated to its Cloud DNS nameservers (per ${resolver_used})"
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
                echo "        NS records per ${resolver_used}: ${resolver_status}, and none of this zone's own."
                echo "        Set these at the registrar that holds the domain:"
                for nameserver in ${zone_nameservers}; do echo "          ${nameserver}"; done
            fi
        fi
    fi
fi

echo
"${here}/check-pool-fit.py" --pools "${GKE_AGENT_POOLS_FILE}" --region "${GKE_LOCATION}" \
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

echo
echo "Agent ceiling ${GKE_MAX_AGENT_NODES}, and ${GKE_MAX_NODES_TOTAL} nodes with the controller," \
     "against a ${GKE_VCPU_QUOTA_METRIC} quota of ${GKE_VCPU_QUOTA} vCPU and a pool ask of" \
     "${GKE_VCPU_DEMAND} vCPU."
if [ "${GKE_MAX_AGENT_NODES}" -lt 1 ]; then
    fail "no agent node can be created, so no build can run"
    echo "        \`make quota\` says whether that is the project's quota or a pool at max_size 0."
elif [ "${GKE_VCPU_DEMAND}" -gt "${GKE_VCPU_QUOTA}" ]; then
    pass "the vCPU quota holds ${GKE_MAX_AGENT_NODES} agent node(s)"
    echo "      The node pools may ask for ${GKE_VCPU_DEMAND} vCPU, which is more than the project's"
    echo "      ${GKE_VCPU_QUOTA} on ${GKE_VCPU_QUOTA_METRIC}, so the pools cannot all reach their maxima."
    echo "      What a build sees is a pod that stays Pending. README.md has the request command;"
    echo "      \`make quota\` shows the arithmetic."
else
    pass "the vCPU quota holds every node the pools may create"
fi

if kubectl --context "${context}" top node --no-headers >/dev/null 2>&1; then
    pass "the Metrics API answers, so \`kubectl top\` can measure the controller"
else
    fail "the Metrics API does not answer; \`kubectl top\` reports nothing about this cluster"
    echo "        GKE runs metrics-server itself and nothing in ../2-platform installs one, so this is the"
    echo "        cluster's own add-on being unready. On a cluster whose only node pool has just scaled,"
    echo "        metrics-server has no node to run on yet:"
    echo "        kubectl -n kube-system get deploy,pod -l k8s-app=metrics-server"
    echo "        kubectl -n kube-system logs -l k8s-app=metrics-server"
fi

echo
if [ -z "${cluster_status}" ]; then
    echo "      The cluster could not be read, so whether its nodes need a Cloud NAT was not checked."
    echo "      Not counted below: see the credential message in section 3."
elif [ "${private_nodes}" != "True" ]; then
    echo "      The nodes carry external addresses, so they reach apache.jfrog.io without a Cloud NAT."
    echo "      That is a choice made in ../1-cluster and not a fault, and it is not counted below."
else
    # A NAT is attached to a Cloud Router, and a router belongs to a network and a region.  Both are read off
    # the cluster rather than from the environment, so a cluster moved to another network is followed here.
    routers="$(gcloud compute routers list --project "${GOOGLE_PROJECT}" \
        --filter="region:(${GKE_LOCATION}) AND network:(${cluster_network})" \
        --format='value(name)' 2>/dev/null)"
    if [ -z "${routers}" ]; then
        fail "no Cloud Router was found in ${GKE_LOCATION} on network ${cluster_network:-unknown}"
        echo "        The nodes are private, so without a NAT every agent pod stays in ImagePullBackOff."
        echo "        If the call failed rather than the router being absent, check the credential first."
        echo "        gcloud compute routers list --filter=\"region:(${GKE_LOCATION})\""
        echo "        gcloud compute routers create ${GKE_CLUSTER_NAME}-nat-router --network ${cluster_network:-default} --region ${GKE_LOCATION}"
        echo "        gcloud compute routers nats create ${GKE_CLUSTER_NAME}-nat --router ${GKE_CLUSTER_NAME}-nat-router \\"
        echo "            --router-region ${GKE_LOCATION} --auto-allocate-nat-external-ip --nat-all-subnet-ip-ranges"
    else
        nat_covering=""
        for router in ${routers}; do
            nats="$(gcloud compute routers nats list --router="${router}" \
                --router-region="${GKE_LOCATION}" --project "${GOOGLE_PROJECT}" --format=json 2>/dev/null)"
            nat_name="$(printf '%s' "${nats}" | python3 "${here}/nat-coverage.py" "${cluster_subnetwork}" "${GOOGLE_PROJECT}" "${GKE_LOCATION}")"
            [ -z "${nat_name}" ] || nat_covering="${router}/${nat_name}"
            [ -n "${nat_covering}" ] && break
        done

        if [ -n "${nat_covering}" ]; then
            pass "a Cloud NAT covers ${cluster_subnetwork} in ${GKE_LOCATION} (${nat_covering})"
        else
            fail "the routers in ${GKE_LOCATION} carry no NAT covering ${cluster_subnetwork}"
            echo "        The nodes are private, so every agent pod will sit in ImagePullBackOff pulling"
            echo "        apache.jfrog.io, and be deleted at slaveConnectTimeout. Add the NAT to a router"
            echo "        that is already there, or create both:"
            echo "        gcloud compute routers create ${GKE_CLUSTER_NAME}-nat-router --network ${cluster_network} --region ${GKE_LOCATION}"
            echo "        gcloud compute routers nats create ${GKE_CLUSTER_NAME}-nat --router ${GKE_CLUSTER_NAME}-nat-router \\"
            echo "            --router-region ${GKE_LOCATION} --auto-allocate-nat-external-ip --nat-all-subnet-ip-ranges"
        fi
    fi
fi

echo
if [ -z "${GKE_SPEND_CAPS_SECRET:-}" ]; then
    echo "      No spend cap is set, so nothing watches what this cluster spends. That is a choice and"
    echo "      not a fault, and it is not counted below. \`make caps\` asks for the caps."
else
    guard_state="$(gcloud functions describe "${GKE_SPEND_GUARD_FUNCTION:-}" --gen2 \
        --region "${GKE_LOCATION}" --project "${GOOGLE_PROJECT}" --format='value(state)' 2>/dev/null)"
    if [ "${guard_state}" = "ACTIVE" ]; then
        pass "the spend guard ${GKE_SPEND_GUARD_FUNCTION:-} is ACTIVE"
    else
        fail "the spend guard is ${guard_state:-unreadable}, so nothing is comparing spend against the caps"
        echo "        The caps are in Secret Manager as ${GKE_SPEND_CAPS_SECRET}, and \`make apply\` is what"
        echo "        creates the function that reads them."
        echo "        gcloud functions describe ${GKE_SPEND_GUARD_FUNCTION:-} --gen2 --region ${GKE_LOCATION}"
    fi

    scheduler_region="${GKE_SPEND_GUARD_SCHEDULER_REGION:-${GKE_LOCATION}}"
    job_state="$(gcloud scheduler jobs describe "${GKE_SPEND_GUARD_SCHEDULER_JOB:-}" \
        --location "${scheduler_region}" --project "${GOOGLE_PROJECT}" --format='value(state)' 2>/dev/null)"
    case "${job_state}" in
        ENABLED)
            pass "its schedule is ENABLED, every ${GKE_SPEND_GUARD_INTERVAL_MINUTES:-?} minutes" ;;
        PAUSED)
            fail "its schedule is PAUSED, so the guard is not being invoked and spend is not being read"
            echo "        Paused is not an error anywhere in the console, and it survives \`make apply\`."
            echo "        gcloud scheduler jobs resume ${GKE_SPEND_GUARD_SCHEDULER_JOB:-} --location ${scheduler_region}" ;;
        *)
            fail "its schedule is ${job_state:-unreadable}, so the guard is not being invoked"
            echo "        gcloud scheduler jobs describe ${GKE_SPEND_GUARD_SCHEDULER_JOB:-} --location ${scheduler_region}" ;;
    esac

    if ! policy_facts="$(gcloud alpha monitoring policies list --project "${GOOGLE_PROJECT}" \
        --filter="displayName='${GKE_SPEND_STALLED_ALERT_POLICY:-}'" --format=json 2>/dev/null)"; then
        fail "the alert '${GKE_SPEND_STALLED_ALERT_POLICY:-}' could not be read"
    else
        policy_enabled="$(printf '%s' "${policy_facts}" | jq -r '
            if type != "array" then "unknown"
            elif length == 0 then "absent"
            elif length != 1 then "unknown"
            else .[0].enabled | if type == "object" then .value else . end |
                if . == true then "true" elif . == false then "false" else "unknown" end
            end' 2>/dev/null)"
        case "${policy_enabled}" in
            true) pass "the alert for a guard that has stopped reporting exists and is enabled" ;;
            false) fail "the alert '${GKE_SPEND_STALLED_ALERT_POLICY:-}' exists but is disabled, so a stalled guard is silent" ;;
            absent) fail "the alert '${GKE_SPEND_STALLED_ALERT_POLICY:-}' was not found, so nothing reports a stalled guard" ;;
            *) fail "the alert '${GKE_SPEND_STALLED_ALERT_POLICY:-}' enabled state could not be read" ;;
        esac
    fi

    # The figures, from the same script and the same arithmetic the guard brakes on.  Three statuses, as with
    # check-pool-fit.py: 0 under every cap, 1 over one or braked, 2 spend could not be established.
    echo
    "${here}/../spend-guard.py" --report
    spend_status=$?
    if [ "${spend_status}" -eq 0 ]; then
        pass "spend is under every cap, and the agent pools are not stopped"
    elif [ "${spend_status}" -eq 2 ]; then
        fail "what this project has spent could not be established; see the message above"
        echo "        This local report uses your credentials; the deployed guard uses its service account."
        echo "        Check the function logs before concluding that the guard also failed and braked the pools:"
        echo "        gcloud functions logs read ${GKE_SPEND_GUARD_FUNCTION:-} --gen2 --project ${GOOGLE_PROJECT} --region ${GKE_LOCATION} --limit 50"
    else
        fail "a cap is met, or the agent pools are stopped; see the figures above"
        echo "        This is the guard working rather than a fault in the cluster. No agent node will"
        echo "        start until the window rolls over or a cap is raised:"
        echo "        printf '{\"daily\":<usd>,\"weekly\":<usd>,\"monthly\":<usd>}' \\"
        echo "            | gcloud secrets versions add ${GKE_SPEND_CAPS_SECRET} --data-file=-"
        echo "        A cap raised that way is put back by the next \`make apply\`; \`make caps\` is what"
        echo "        changes it for good."
    fi
fi

if [ "${with_build}" -eq 1 ]; then
    # A full path with the template at the end, which BSD and GNU mktemp read alike: BSD reads `-t` as a
    # prefix and appends its own suffix, leaving a literal XXXXXX in the name.
    log="$(mktemp "${tmp_dir}/run-ci-smoke.XXXXXX")"
    echo "Submitting a build through .build/run-ci, logging to ${log}"
    baseline="$(kubectl --context "${context}" get nodes -l cassandra.jenkins.agent=true -o name 2>/dev/null)"
    ( cd "${cassandra_dir}" && PYTHONUNBUFFERED=1 exec .build/run-ci --kubecontext "${context}" ${RUN_CI_ARGS:-} ) >"${log}" 2>&1 &
    build_pid=$!
    run_ci_exited=0
    build_status=""

    echo "Waiting up to ${build_timeout}s for a confirmed build and a new agent node"
    scaled=""
    deadline=$((SECONDS + build_timeout))
    while [ "${SECONDS}" -lt "${deadline}" ]; do
        if [ -z "${build_status}" ] && ! kill -0 "${build_pid}" 2>/dev/null; then
            wait "${build_pid}"
            build_status=$?
            if [ "${build_status}" -ne 0 ]; then
                run_ci_exited=1
                break
            fi
        fi
        if grep -Eq '^Build number: [0-9]+' "${log}"; then
            current_nodes="$(kubectl --context "${context}" get nodes -l cassandra.jenkins.agent=true -o name 2>/dev/null)"
            scaled="$(comm -13 <(printf '%s\n' "${baseline}" | sort) <(printf '%s\n' "${current_nodes}" | sort) | head -1)"
            [ -n "${scaled}" ] && break
        fi
        sleep 1
    done

    if [ -n "${scaled}" ]; then
        pass "a build was submitted and an agent node appeared (${scaled})"
    elif [ "${run_ci_exited}" -eq 1 ]; then
        fail "run-ci exited with status ${build_status} before scale-up was confirmed"
        echo "        Its last lines:"
        tr '\r' '\n' < "${log}" \
            | sed $'s/\033\\[[0-9;?]*[a-zA-Z]//g' \
            | grep -v -e '^[[:space:]]*$' -e 'Waiting for build to complete' \
            | tail -12 | sed 's/^/        | /'
        echo "        A branch that tracks no remote is the usual cause. Name what to build instead:"
        echo "        make smoke SMOKE_ARGS=--build \\"
        echo "          RUN_CI_ARGS='-r https://github.com/apache/cassandra -b trunk'"
    else
        fail "no new agent node and confirmed build submission within ${build_timeout}s"
        echo "        Read ${log} first: a build that never queued an agent is a Jenkins problem, and a"
        echo "        pod stuck Pending with a scale-up message is an autoscaler one. There is no autoscaler"
        echo "        pod to read logs from here; GKE writes its decisions to Cloud Logging instead:"
        echo "        kubectl get events --sort-by=.lastTimestamp | tail -30"
        echo "        gcloud logging read 'resource.type=\"k8s_cluster\"" \
             "AND resource.labels.cluster_name=\"${GKE_CLUSTER_NAME}\"" \
             "AND jsonPayload.noDecisionStatus:*' --limit 20 --freshness=1h"
    fi

    # Left running: a skinny profile takes hours, and killing run-ci would abandon the Jenkins build and
    # strand its agent pods.
    if kill -0 "${build_pid}" 2>/dev/null; then
        echo
        echo "The build is still running as pid ${build_pid}. Follow it with \`tail -f ${log}\`,"
        echo "or abort it in Jenkins; do not kill run-ci, which would leave the build and its agents behind."
    fi
fi

if [ "${sample_seconds}" -gt 0 ] && { [ "${with_build}" -eq 0 ] || [ -n "${scaled}" ]; }; then
    jenkins_address="$(kubectl --context "${context}" -n "${jenkins_namespace}" get svc \
        -l "${jenkins_controller_selector}" \
        -o jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}' 2>/dev/null)"
    if [ -z "${jenkins_address}" ]; then
        jenkins_address="$(kubectl --context "${context}" -n "${jenkins_namespace}" get svc \
            -l "${jenkins_controller_selector}" \
            -o jsonpath='{.items[0].status.loadBalancer.ingress[0].hostname}' 2>/dev/null)"
    fi
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
        warned_jenkins=0 warned_top=0
        top_min_interval=60
        next_top=0
        while [ "${SECONDS}" -lt "${sample_deadline}" ]; do
            computer="$(curl -fsS --max-time 45 \
                "http://${jenkins_address}/computer/api/json?tree=busyExecutors,totalExecutors" \
                2>/dev/null)"
            busy="$(printf '%s' "${computer}" | tr ',{}' '\n\n\n' \
                | grep -o '"busyExecutors":[0-9]*' | cut -d: -f2)"
            total="$(printf '%s' "${computer}" | tr ',{}' '\n\n\n' \
                | grep -o '"totalExecutors":[0-9]*' | cut -d: -f2)"
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
                top="$(kubectl --context "${context}" -n "${jenkins_namespace}" --request-timeout=30s \
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
                echo "               gcloud auth print-access-token >/dev/null && echo credential ok"
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

echo
echo "${passed} passed, ${failed} failed."
[ "${failed}" -eq 0 ] || exit 1
