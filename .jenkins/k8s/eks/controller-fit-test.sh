#!/bin/bash
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

# Offline cases for controller-fit.py, against a stubbed `aws` CLI.
#
# What this exists to catch.  The controller is the one ceiling in this directory that costs money to raise
# while nothing runs, so the recommendation has to be right in both directions: silent when the configured
# pools fit, and specific about the size and the monthly bill when they do not.  A model that recommends an
# upgrade nobody needs costs the operator a few hundred dollars a month for nothing.
#
# The last section covers a different failure with the same inputs: a `requests` figure above the node's
# allocatable, which is a controller that never starts rather than one that runs slowly.  It also covers
# --overrides, without which this models the shared values file rather than the one that gets deployed.
#
# Needs no credentials.  The stub answers `ec2 describe-instance-types` from a table of the m7a family and
# `pricing get-products` from the published us-west-2 on-demand prices, and nothing else.

set -o pipefail
here="$(cd -- "$(dirname -- "$0")" && pwd)"
script="${here}/controller-fit.py"
work="$(mktemp -d)"
# Checked, because this script has no `set -e` and an unchecked failure here leaves ${work} empty: every
# fixture path then resolves to /, and the suite reports its own cases as failures.
[ -n "${work}" ] && [ -d "${work}" ] || { echo "mktemp -d failed, so there is nowhere to write the fixtures"; exit 1; }
trap 'rm -rf "${work}"' EXIT
failures=0

command -v python3 >/dev/null 2>&1 || { echo "python3 needs to be installed"; exit 1; }
python3 -c 'import yaml' 2>/dev/null || { echo "SKIP  controller-fit-test.sh needs PyYAML"; exit 0; }

# ---------------------------------------------------------------------------------------------------
# The stub
# ---------------------------------------------------------------------------------------------------

mkdir -p "${work}/bin"
cat > "${work}/bin/aws" <<'STUB'
#!/bin/bash
# Answers the two calls controller-fit.py makes.  Anything else is an error, so a new call cannot pass this
# test by being silently unanswered.
case "$1 $2 $3" in
  'ec2 describe-instance-types --region')
    for arg in "$@"; do
      case "${arg}" in m7a.*) want="${arg}" ;; esac
    done
    case "${want}" in
      m7a.large)    v=2  ; m=8192   ;;
      m7a.xlarge)   v=4  ; m=16384  ;;
      m7a.2xlarge)  v=8  ; m=32768  ;;
      m7a.4xlarge)  v=16 ; m=65536  ;;
      m7a.8xlarge)  v=32 ; m=131072 ;;
      m7a.12xlarge) v=48 ; m=196608 ;;
      m7a.16xlarge) v=64 ; m=262144 ;;
      m7a.24xlarge) v=96 ; m=393216 ;;
      *) echo '{"InstanceTypes": []}' ; exit 0 ;;
    esac
    printf '{"InstanceTypes":[{"InstanceType":"%s","VCpuInfo":{"DefaultVCpus":%s},"MemoryInfo":{"SizeInMiB":%s}}]}\n' \
        "${want}" "${v}" "${m}"
    ;;
  'pricing get-products --region')
    [ "x${STUB_NO_PRICING}" = "x" ] || { echo "AccessDeniedException" >&2 ; exit 254 ; }
    for arg in "$@"; do
      case "${arg}" in *instanceType,Value=m7a.*) want="${arg##*Value=}" ;; esac
    done
    case "${want}" in
      m7a.large)    p=0.11592 ;;
      m7a.xlarge)   p=0.23184 ;;
      m7a.2xlarge)  p=0.46368 ;;
      m7a.4xlarge)  p=0.92736 ;;
      m7a.8xlarge)  p=1.85472 ;;
      m7a.12xlarge) p=2.78208 ;;
      m7a.16xlarge) p=3.70944 ;;
      m7a.24xlarge) p=5.56416 ;;
      *) echo '{"PriceList": []}' ; exit 0 ;;
    esac
    printf '{"PriceList":["{\\"terms\\":{\\"OnDemand\\":{\\"a\\":{\\"priceDimensions\\":{\\"b\\":{\\"pricePerUnit\\":{\\"USD\\":\\"%s\\"}}}}}}}"]}\n' "${p}"
    ;;
  *)
    echo "stub aws: unexpected call: $*" >&2
    exit 64
    ;;
esac
STUB
chmod +x "${work}/bin/aws"
export PATH="${work}/bin:${PATH}"

# ---------------------------------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------------------------------

# The four pools, with max_size the only field this script reads from them.
pools_file() {
    cat > "${work}/pools.json" <<JSON
{
  "small":  { "max_size": $1, "instance_types": ["c7a.2xlarge"], "node_selector_label": "cassandra.jenkins.agent.small" },
  "medium": { "max_size": $2, "instance_types": ["c7a.2xlarge"], "node_selector_label": "cassandra.jenkins.agent.medium" },
  "large":  { "max_size": $3, "instance_types": ["m7a.2xlarge"], "node_selector_label": "cassandra.jenkins.agent.large" },
  "report": { "max_size": $4, "instance_types": ["m7a.2xlarge"], "node_selector_label": "cassandra.jenkins.agent.report" }
}
JSON
}

controller_file() {
    printf '{"instance_types": ["%s"], "availability_zone": "us-west-2a", "max_size": 1}\n' "$1" \
        > "${work}/controller.json"
}

# The controller's limits, its heap and one podTemplate carrying idleMinutes, which is all this script reads.
deployment_file() {
    local cpu="$1" memory="$2" xmx="$3" idle="$4"
    cat > "${work}/deployment.yaml" <<YAML
controller:
  javaOpts: -server -XX:+UseG1GC -Xms${xmx} -Xmx${xmx}
  resources:
    requests:
      cpu: "1"
      memory: "1G"
    limits:
      cpu: ${cpu}
      memory: ${memory}
agent:
  podTemplates:
    agent-dind-large: |
      - name: agent-dind-large
        idleMinutes: ${idle}
        instanceCap: 306
YAML
}

run() {
    "${script}" --pools "${work}/pools.json" --controller "${work}/controller.json" \
        --deployment "${work}/deployment.yaml" --region us-west-2 "$@" > "${work}/output" 2>&1
}

# A values file run-ci applies after the deployment, which is what 2-platform/jenkins-eks-overrides.yaml is.
# requests, because they are what decides whether the pod is schedulable at all.
overrides_file() {
    local cpu="$1" memory="$2" xmx="$3"
    cat > "${work}/overrides.yaml" <<YAML
controller:
  javaOpts: -server -XX:+UseG1GC -Xms${xmx} -Xmx${xmx}
  resources:
    requests:
      cpu: ${cpu}
      memory: ${memory}
YAML
}

run_merged() {
    "${script}" --pools "${work}/pools.json" --controller "${work}/controller.json" \
        --deployment "${work}/deployment.yaml" --overrides "${work}/overrides.yaml" \
        --region us-west-2 "$@" > "${work}/output" 2>&1
}

expect_says() {
    local what="$1" pattern="$2"
    if grep -qE "${pattern}" "${work}/output"; then
        echo "PASS  ${what}"
    else
        echo "FAIL  ${what}"
        echo "        expected to match: ${pattern}"
        sed 's/^/        /' "${work}/output"
        failures=$((failures + 1))
    fi
}

expect_silent_about() {
    local what="$1" pattern="$2"
    if grep -qE "${pattern}" "${work}/output"; then
        echo "FAIL  ${what}"
        sed 's/^/        /' "${work}/output"
        failures=$((failures + 1))
    else
        echo "PASS  ${what}"
    fi
}

expect_exit() {
    local want="$1" what="$2"; shift 2
    "$@" >/dev/null 2>&1
    local got=$?
    if [ "${got}" -eq "${want}" ]; then
        echo "PASS  ${what} (exit ${got})"
    else
        echo "FAIL  ${what}: wanted exit ${want}, got ${got}"
        sed 's/^/        /' "${work}/output"
        failures=$((failures + 1))
    fi
}

# ---------------------------------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------------------------------

# The pools as they were before the quota grant: 480 agent nodes on the committed controller.  This fits, and
# the check has to stay quiet about upgrading rather than sell one.
pools_file 20 150 306 4
controller_file m7a.2xlarge
deployment_file 8 20G 8G 5
run --check
expect_says "480 agents on m7a.2xlarge is reported as holding" 'controller holds this'
expect_silent_about "and recommends nothing" 'Recommended|WARNING'
expect_says "the monthly cost of what is running is stated" '\$338 a month'

# The pools after the grant: 1122 agent nodes, which is what pushed the live controller to its node ceiling.
pools_file 46 351 716 9
run --check
expect_says "1122 agents does not fit m7a.2xlarge" 'WARNING'
expect_says "and m7a.4xlarge is recommended" 'Recommended: m7a\.4xlarge'
# $338, not the $339 that subtracting two rounded monthly figures gives: the delta is computed from the
# hourly prices and rounded once, which is the honest order for a number an operator will act on.
expect_says "with the delta a month named" '\$338 a month more'
expect_says "the 24/7 cost is called out against the agents' billing" 'billed by the'
expect_says "idleMinutes is offered as the cheaper lever" 'idleMinutes'

# idleMinutes is the cpu half of the model, so raising it has to lower the requirement.  Same pools, same
# controller, idleMinutes doubled.
cpu_at_5="$(run && grep -oE '^ +needs +[0-9]+m' "${work}/output" | grep -oE '[0-9]+' | head -1)"
run --check
before="$(grep -oE 'needs +[0-9]+m' "${work}/output" | grep -oE '[0-9]+')"
deployment_file 8 20G 8G 10
run --check
after="$(grep -oE 'needs +[0-9]+m' "${work}/output" | grep -oE '[0-9]+')"
if [ -n "${before}" ] && [ -n "${after}" ] && [ "${after}" -lt "${before}" ]; then
    echo "PASS  doubling idleMinutes lowers the cpu requirement (${before}m -> ${after}m)"
else
    echo "FAIL  doubling idleMinutes lowers the cpu requirement: ${before:-?} -> ${after:-?}"
    failures=$((failures + 1))
fi

# Memory does not follow idleMinutes, because it follows the agent count.  Same two runs, same figure.
deployment_file 8 20G 8G 5
run --check
mem_at_5="$(grep -oE 'needs.* ([0-9]+) MiB' "${work}/output" | grep -oE '[0-9]+ MiB' | head -1)"
deployment_file 8 20G 8G 10
run --check
mem_at_10="$(grep -oE 'needs.* ([0-9]+) MiB' "${work}/output" | grep -oE '[0-9]+ MiB' | head -1)"
if [ "${mem_at_5}" = "${mem_at_10}" ] && [ -n "${mem_at_5}" ]; then
    echo "PASS  idleMinutes does not change the memory requirement (${mem_at_5})"
else
    echo "FAIL  idleMinutes must not change the memory requirement: ${mem_at_5} vs ${mem_at_10}"
    failures=$((failures + 1))
fi

# A heap raised in javaOpts raises the memory requirement by the same amount, since the model adds it.
deployment_file 8 20G 16G 5
run --check
expect_says "a larger heap raises the memory requirement" 'needs'

# A cluster already on a big controller is told it fits, and is not sold a bigger one.
pools_file 46 351 716 9
controller_file m7a.8xlarge
deployment_file 30 120G 32G 5
run --check
expect_says "1122 agents on m7a.8xlarge is reported as holding" 'controller holds this'
expect_silent_about "and no upgrade is recommended" 'Recommended:'

# Pools past what the largest size in the family can hold: say so and name the two levers, rather than
# recommending an instance type that does not exist.
pools_file 2000 20000 40000 100
controller_file m7a.2xlarge
deployment_file 8 20G 8G 5
run --check
expect_says "pools past the family's largest size are named as such" 'No size in the m7a family'
expect_says "and the two levers are given" 'max_size|idleMinutes'

# Without pricing the sizing still stands; only the cost figure is missing.
pools_file 46 351 716 9
STUB_NO_PRICING=1 run --check
expect_says "the recommendation survives an unreadable pricing API" 'Recommended: m7a\.4xlarge'
expect_says "and says why there is no cost figure" 'pricing:GetProducts'

# The export form, for the Makefile.
pools_file 46 351 716 9
deployment_file 8 20G 8G 5
run
expect_says "exports the recommendation" "EKS_CONTROLLER_RECOMMENDED='m7a.4xlarge'"
expect_says "exports that it does not fit" "EKS_CONTROLLER_FITS='false'"

# An under-sized controller refuses, because a warning was not enough; see controller-fit.py.  The
# operator who has read the figures passes --warn-only, which is what CONTROLLER_FIT_ARGS carries.
expect_exit 1 "an under-sized controller refuses" run --check
expect_exit 0 "and --warn-only deploys anyway" run --check --warn-only
pools_file 20 150 306 4
expect_exit 0 "a controller that fits exits 0" run --check

# Every pool at zero is the one case that cannot be answered.
pools_file 0 0 0 0
expect_exit 1 "every pool at max_size 0 fails" run --check

# ---------------------------------------------------------------------------------------------------
# --overrides, and the requests
# ---------------------------------------------------------------------------------------------------

# The case this section exists for.  A requests figure generated for a 1121-node account sat in
# 2-platform/jenkins-eks-overrides.yaml against a default controller of m7a.2xlarge, whose allocatable is
# 7910m: the pod could never be scheduled, and nothing here read that file or those requests.
pools_file 20 150 306 4
controller_file m7a.2xlarge
deployment_file 8 20G 8G 5
overrides_file 13765m 28G 16G
run_merged --check
expect_says "requests above allocatable are refused by name" 'requests do not fit m7a\.2xlarge'
expect_says "and the symptom is named rather than the cause guessed at" 'Pending'
expect_says "and the file to edit is named" 'jenkins-eks-overrides\.yaml'
expect_exit 1 "an unschedulable controller refuses" run_merged --check
# --warn-only covers a controller that will be slow. There is nothing to accept about one that never starts.
expect_exit 1 "and --warn-only does not cover it" run_merged --check --warn-only

# Requests inside allocatable say nothing about requests, and the sizing verdict is reached as before.
overrides_file 4000m 16G 8G
run_merged --check
expect_silent_about "requests inside allocatable are not mentioned" 'requests do not fit'
expect_says "and the sizing verdict is reached" 'controller holds this'

# The overrides file wins on the heap, which is the mismatch that hid the drift: the shared file says 8G and
# the deployed cluster ran 16G, so the memory requirement was under-read by the difference.
deployment_file 8 20G 8G 5
overrides_file 4000m 16G 8G
run_merged --check
heap_shared="$(grep -oE 'heap [0-9]+ MiB' "${work}/output" | grep -oE '[0-9]+')"
overrides_file 4000m 16G 16G
run_merged --check
heap_overridden="$(grep -oE 'heap [0-9]+ MiB' "${work}/output" | grep -oE '[0-9]+')"
if [ "${heap_shared}" = "8192" ] && [ "${heap_overridden}" = "16384" ]; then
    echo "PASS  --overrides decides the heap (${heap_shared} MiB -> ${heap_overridden} MiB)"
else
    echo "FAIL  --overrides must decide the heap: got ${heap_shared:-?} then ${heap_overridden:-?}"
    failures=$((failures + 1))
fi

# Without --overrides the shared file alone is modelled, which is the old behaviour and still correct for a
# site that passes no second values file.
run --check
expect_says "no --overrides models the deployment alone" 'heap 8192 MiB'

if [ "${failures}" -ne 0 ]; then
    echo "${failures} case(s) failed."
    exit 1
fi
echo "All cases behaved as recorded."
