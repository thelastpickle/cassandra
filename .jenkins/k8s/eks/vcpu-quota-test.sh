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
# Regression test for vcpu-quota.py.  Needs no cluster and no AWS credentials: it puts a stub `aws` on the
# PATH that answers `service-quotas get-service-quota` from $STUB_QUOTA and `ec2 describe-instance-types`
# from the type's name, and reads the numbers the script derives back off its `export` lines.
#
# The case it exists for is the one that cost a build 2 h 19 m: pools whose max_size sums to 480 nodes
# against an account quota of 384 vCPU, which is 48 nodes.  Nothing in the node group, the autoscaler
# or Jenkins reports that, so the arithmetic below is the only place it is stated.
#
#   ./vcpu-quota-test.sh
#
# Exits 0 when every case behaves as recorded below.

set -o errexit
set -o nounset
set -o pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
work="$(mktemp -d)"
# Checked, because this script has no `set -e` and an unchecked failure here leaves ${work} empty: every
# fixture path then resolves to /, and the suite reports its own cases as failures.
[ -n "${work}" ] && [ -d "${work}" ] || { echo "mktemp -d failed, so there is nowhere to write the fixtures"; exit 1; }
trap 'rm -rf "${work}"' EXIT

# ---------------------------------------------------------------------------------------------------
# The stub `aws`
# ---------------------------------------------------------------------------------------------------

# The vCPU count is taken from the type's own size word, so a case can name a type this file never listed.
# The quota comes from $STUB_QUOTA, which each case sets.
mkdir -p "${work}/bin"
cat > "${work}/bin/aws" <<'STUB'
#!/bin/bash
# Stub. Answers `service-quotas get-service-quota` and `ec2 describe-instance-types`, nothing else.
service="$1"; shift
operation="$1"; shift

case "${service} ${operation}" in
    'service-quotas get-service-quota')
        cat <<JSON
{ "Quota": { "QuotaCode": "L-1216C47A", "Value": ${STUB_QUOTA} } }
JSON
        exit 0
        ;;
    'ec2 describe-instance-types') ;;
    *) echo "stub aws: no canned answer for '${service} ${operation}'" >&2; exit 1 ;;
esac

instance_type=""
while [ $# -gt 0 ]; do
    case "$1" in
        --instance-types) instance_type="$2"; shift 2 ;;
        *) shift ;;
    esac
done
case "${instance_type##*.}" in
    large)    vcpus=2 ;;
    xlarge)   vcpus=4 ;;
    2xlarge)  vcpus=8 ;;
    4xlarge)  vcpus=16 ;;
    *) echo "stub aws: no canned answer for '${instance_type}'" >&2; exit 1 ;;
esac
cat <<JSON
{
  "InstanceTypes": [
    { "InstanceType": "${instance_type}", "VCpuInfo": { "DefaultVCpus": ${vcpus} } }
  ]
}
JSON
STUB
chmod +x "${work}/bin/aws"

export PATH="${work}/bin:${PATH}"

# ---------------------------------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------------------------------

# The defaults in 1-cluster/variables.tf, shaped like `tofu output -json agent_node_groups`.  Only the four
# keys vcpu-quota.py reads are needed, and the rest are left out so that a change to them cannot silently
# change what this test asserts.
cat > "${work}/defaults.json" <<'JSON'
{
  "small":  { "instance_types": ["c7a.2xlarge"], "max_size": 20,  "node_group_names": ["agents-small-a", "agents-small-b"],   "disk_gib": 100, "node_selector_label": "cassandra.jenkins.agent.small" },
  "medium": { "instance_types": ["c7a.2xlarge"], "max_size": 150, "node_group_names": ["agents-medium-a", "agents-medium-b"], "disk_gib": 100, "node_selector_label": "cassandra.jenkins.agent.medium" },
  "large":  { "instance_types": ["m7a.2xlarge"], "max_size": 306, "node_group_names": ["agents-large-a", "agents-large-b"],   "disk_gib": 100, "node_selector_label": "cassandra.jenkins.agent.large" },
  "report": { "instance_types": ["m7a.2xlarge"], "max_size": 4,   "node_group_names": ["agents-report-a", "agents-report-b"], "disk_gib": 100, "node_selector_label": "cassandra.jenkins.agent.report" }
}
JSON

# One pool listing two instance types, to pin which of them is charged.
cat > "${work}/mixed.json" <<'JSON'
{
  "medium": { "instance_types": ["c7a.xlarge", "c7a.4xlarge"], "max_size": 10, "node_group_names": ["agents-medium-a"], "disk_gib": 100, "node_selector_label": "cassandra.jenkins.agent.medium" }
}
JSON

# ---------------------------------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------------------------------

failures=0

# One run of the script, with the quota the case names.  Its `export` lines are sourced, so a case asserts
# on the variables 2-platform and 3-smoke will read rather than on the text around them.
run() {
    local quota="$1" pools="$2"
    shift 2
    STUB_QUOTA="${quota}" python3 "${here}/vcpu-quota.py" \
        --pools "${pools}" --region us-east-1 \
        --controller-instance-types "${CONTROLLER_TYPES:-m7a.2xlarge}" \
        --controller-max-size "${CONTROLLER_MAX:-1}" "$@"
}

expect() {
    local description="$1" expected="$2" actual="$3"
    if [ "${actual}" = "${expected}" ]; then
        echo "PASS  ${description} (${actual})"
    else
        echo "FAIL  ${description}: expected ${expected}, got ${actual}"
        failures=$((failures + 1))
    fi
}

expect_exit() {
    local expected="$1" description="$2"
    shift 2
    local actual=0
    "$@" > "${work}/output" 2>&1 || actual=$?
    if [ "${actual}" -eq "${expected}" ]; then
        echo "PASS  ${description} (exit ${actual})"
    else
        echo "FAIL  ${description}: expected exit ${expected}, got ${actual}"
        sed 's/^/        /' "${work}/output"
        failures=$((failures + 1))
    fi
}

# The quota this account started at, which build 7 ran against.  480 agent nodes plus the controller, all
# 8 vCPU, asks 3848 against 384 allowed; 384 / 8 is 48 nodes, one of them the controller's.  Build 7's own
# ask was 310 nodes, before the report pool and before build 4 raised medium and large to the quota.
eval "$(run 384 "${work}/defaults.json" 2>/dev/null)"
expect "the quota is reported as given"      384  "${EKS_ONDEMAND_VCPU_QUOTA}"
expect "the pools' full ask is summed"       3848 "${EKS_ONDEMAND_VCPU_DEMAND}"
expect "384 vCPU is 48 nodes"                48   "${EKS_MAX_NODES_TOTAL}"
expect "the controller takes one of them"    47   "${EKS_MAX_AGENT_NODES}"

# An over-ask is a warning and not a failure: the cluster runs, smaller than it is written for.
expect_exit 0 "an over-ask still exits 0" run 384 "${work}/defaults.json"
if ! grep -q 'may ask for 3848 vCPU and the account allows 384' "${work}/output"; then
    echo "FAIL  an over-ask is reported by name"
    sed 's/^/        /' "${work}/output"
    failures=$((failures + 1))
else
    echo "PASS  an over-ask is reported by name"
fi

# The quota this cluster asks for, which holds every node the pools may create.
eval "$(run 3848 "${work}/defaults.json" 2>/dev/null)"
expect "3848 vCPU is 481 nodes"              481  "${EKS_MAX_NODES_TOTAL}"
expect "which is every agent the pools want" 480  "${EKS_MAX_AGENT_NODES}"
expect_exit 0 "the quota it asks for is silent" run 3848 "${work}/defaults.json"
if grep -q 'WARNING' "${work}/output"; then
    echo "FAIL  the quota it asks for is silent"
    sed 's/^/        /' "${work}/output"
    failures=$((failures + 1))
else
    echo "PASS  the quota it asks for warns about nothing"
fi

# The quota AWS granted.  The ceiling must not follow it: the node groups create 480 agent nodes and no
# more, so an agent.containerCap of 621 would have Jenkins ask for pods that wait on a node no group will
# create.  That is build 7's churn loop with the node group in the account's place, and why this clamp exists.
eval "$(run 4976 "${work}/defaults.json" 2>/dev/null)"
expect "a quota above the ask does not raise the ceiling" 480 "${EKS_MAX_AGENT_NODES}"
expect "nor the autoscaler's node total"                  481 "${EKS_MAX_NODES_TOTAL}"
expect "and the quota is still reported as given"        4976 "${EKS_ONDEMAND_VCPU_QUOTA}"
expect_exit 0 "a quota above the ask exits 0" run 4976 "${work}/defaults.json" --check
if ! grep -q 'The node groups bind: 480 agents' "${work}/output"; then
    echo "FAIL  the binding ceiling is named"
    sed 's/^/        /' "${work}/output"
    failures=$((failures + 1))
else
    echo "PASS  the binding ceiling is named"
fi
if ! grep -q 'with the quota holding 141 more' "${work}/output"; then
    echo "FAIL  the spare quota is reported"
    sed 's/^/        /' "${work}/output"
    failures=$((failures + 1))
else
    echo "PASS  the spare quota is reported"
fi

# Every pool at max_size 0 is the same symptom as a quota too small, and different advice.
cat > "${work}/empty.json" <<'JSON'
{
  "small": { "instance_types": ["c7a.2xlarge"], "max_size": 0, "node_group_names": ["agents-small-a"], "disk_gib": 100, "node_selector_label": "cassandra.jenkins.agent.small" }
}
JSON
expect_exit 1 "every pool at max_size 0 fails" run 4976 "${work}/empty.json"
if ! grep -q 'every agent pool has max_size 0' "${work}/output"; then
    echo "FAIL  an empty pool is not blamed on the quota"
    sed 's/^/        /' "${work}/output"
    failures=$((failures + 1))
else
    echo "PASS  an empty pool is not blamed on the quota"
fi

# A pool listing two types is charged for the larger.  A managed node group may launch either, so the
# smaller would under-count the ceiling and let the pools exceed the quota again.
eval "$(run 1000 "${work}/mixed.json" 2>/dev/null)"
expect "a mixed pool is charged the larger type" 168 "${EKS_ONDEMAND_VCPU_DEMAND}"
expect "and one small pool binds the node total"  11 "${EKS_MAX_NODES_TOTAL}"
expect_exit 0 "the mixed pool reports" run 1000 "${work}/mixed.json" --check
if ! grep -q 'the quota holds 62 nodes' "${work}/output"; then
    echo "FAIL  the quota's own figure follows the largest node"
    sed 's/^/        /' "${work}/output"
    failures=$((failures + 1))
else
    echo "PASS  the quota's own figure follows the largest node"
fi

# The largest node in the cluster sets the ceiling, whichever group it is in.  Here the controller is the
# largest, so 1000 / 16 is 62 nodes and not 1000 / 8.
CONTROLLER_TYPES=m7a.4xlarge
eval "$(run 1000 "${work}/defaults.json" 2>/dev/null)"
expect "a larger controller lowers the ceiling" 62 "${EKS_MAX_NODES_TOTAL}"
unset CONTROLLER_TYPES

# A quota too small for one agent is the one case that stops a deploy: 8 vCPU holds one node, the
# controller's, and no build could ever run.
expect_exit 1 "a quota holding only the controller fails" run 8 "${work}/defaults.json"

# Layer 1 scales the pools to the lowest ceiling the account allows, so `max_size` is what the node groups
# were created with and `declared_max_size` is what var.agent_pools asked for.  When the two differ the
# ceiling arithmetic follows max_size, and the difference is reported rather than left silent.
cat > "${work}/scaled.json" <<'JSON'
{
  "medium": { "instance_types": ["c7a.2xlarge"], "max_size": 75, "declared_max_size": 150, "node_group_names": ["agents-medium-a"], "disk_gib": 100, "node_selector_label": "cassandra.jenkins.agent.medium" },
  "large":  { "instance_types": ["m7a.2xlarge"], "max_size": 153, "declared_max_size": 306, "node_group_names": ["agents-large-a"], "disk_gib": 100, "node_selector_label": "cassandra.jenkins.agent.large" }
}
JSON
eval "$(run 4976 "${work}/scaled.json" 2>/dev/null)"
expect "a scaled pool is summed at what it was created with" 1832 "${EKS_ONDEMAND_VCPU_DEMAND}"
expect "and the agent ceiling follows the scaled size"        228 "${EKS_MAX_AGENT_NODES}"
expect_exit 0 "a scaled pool reports" run 4976 "${work}/scaled.json" --check
if ! grep -q 'var.agent_pools asked for 456 agent nodes and the node groups were created with 228' "${work}/output"; then
    echo "FAIL  the scaling is reported"
    sed 's/^/        /' "${work}/output"
    failures=$((failures + 1))
else
    echo "PASS  the scaling is reported"
fi

# The same JSON without declared_max_size, which is what layer 1 printed before it scaled anything.  It has
# to read unchanged, or a state written by an older apply stops working.
eval "$(run 4976 "${work}/defaults.json" 2>/dev/null)"
expect_exit 0 "output without declared_max_size still reports" run 4976 "${work}/defaults.json" --check
if grep -q 'var.agent_pools asked for' "${work}/output"; then
    echo "FAIL  unscaled pools say nothing about scaling"
    sed 's/^/        /' "${work}/output"
    failures=$((failures + 1))
else
    echo "PASS  unscaled pools say nothing about scaling"
fi

# An instance family outside L-1216C47A's own list is counted against a quota that does not govern it, so
# the figures are wrong and the script has to say so rather than report them as facts.
cat > "${work}/gpu.json" <<'JSON'
{
  "large": { "instance_types": ["g5.2xlarge"], "max_size": 4, "node_group_names": ["agents-large-a"], "disk_gib": 100, "node_selector_label": "cassandra.jenkins.agent.large" }
}
JSON
expect_exit 0 "a non-standard family still reports" run 384 "${work}/gpu.json"
if ! grep -q 'g5.2xlarge is not a standard instance family' "${work}/output"; then
    echo "FAIL  a non-standard family is called out"
    sed 's/^/        /' "${work}/output"
    failures=$((failures + 1))
else
    echo "PASS  a non-standard family is called out"
fi

if [ "${failures}" -ne 0 ]; then
    echo "${failures} case(s) failed."
    exit 1
fi
echo "All cases behaved as recorded."
