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
# Regression test for check-pool-fit.py.  Needs no cluster and no AWS credentials: it puts a stub `aws`
# on the PATH that answers `ec2 describe-instance-types` from the published figures for two instance
# types, and runs the check in --estimate-only mode.
#
# The case it exists for is the one that was got wrong in the runbook this directory replaces, and which
# a reader cannot see by eye: agent-dind-large requests 16G for dind and 1G for jnlp.  Those are decimal
# G, so 17G is 15.83 GiB, which is more than a 16 GiB node has left after the kubelet's reservations.  A
# large pool on c7a.2xlarge fits no agents; on m7a.2xlarge it fits one.
#
#   ./check-pool-fit-test.sh
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

# 8 vCPU and 4 network interfaces of 15 addresses each for both, which is what gives EKS a maxPods of
# min(4 * 14 + 2, 110) = 58, a kubelet memory reservation of 11 * 58 + 255 = 893 MiB, and a further
# 100 MiB held back for the hard eviction threshold.  The two types differ only in memory.
mkdir -p "${work}/bin"
cat > "${work}/bin/aws" <<'STUB'
#!/bin/bash
# Stub. Answers only `ec2 describe-instance-types`, and only for the two types the test names.
instance_type=""
while [ $# -gt 0 ]; do
    case "$1" in
        --instance-types) instance_type="$2"; shift 2 ;;
        *) shift ;;
    esac
done
case "${instance_type}" in
    c7a.2xlarge) memory_mib=16384 ;;
    m7a.2xlarge) memory_mib=32768 ;;
    *) echo "stub aws: no canned answer for '${instance_type}'" >&2; exit 1 ;;
esac
cat <<JSON
{
  "InstanceTypes": [
    {
      "InstanceType": "${instance_type}",
      "VCpuInfo": { "DefaultVCpus": 8 },
      "MemoryInfo": { "SizeInMiB": ${memory_mib} },
      "NetworkInfo": { "MaximumNetworkInterfaces": 4, "Ipv4AddressesPerInterface": 15 }
    }
  ]
}
JSON
STUB
chmod +x "${work}/bin/aws"

export PATH="${work}/bin:${PATH}"

# ---------------------------------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------------------------------

# Shaped like `tofu output -json agent_node_groups` in ../1-cluster: one row per pool, and one node group
# per availability zone inside it.  Two zones here, which is the fewest EKS accepts; the default
# availability_zone_count is every zone in the region, and the count does not change what this checks.
pools_file() {
    local name="$1" size="$2" instance_type="$3"
    cat > "${work}/${name}.json" <<JSON
{
  "${size}": {
    "node_group_names": ["agents-${size}-a", "agents-${size}-b"],
    "asg_names": ["eks-agents-${size}-a-stub", "eks-agents-${size}-b-stub"],
    "availability_zones": ["us-east-1a", "us-east-1b"],
    "instance_types": ["${instance_type}"],
    "capacity_type": "SPOT",
    "disk_gib": 100,
    "min_size": 0,
    "max_size": 160,
    "node_selector_label": "cassandra.jenkins.agent.${size}"
  }
}
JSON
    echo "${work}/${name}.json"
}

failures=0

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

check() {
    python3 "${here}/check-pool-fit.py" --estimate-only --region us-east-1 --pools "$1"
}

expect_exit 1 "large pool on c7a.2xlarge fits no agent" \
    check "$(pools_file large-c7a large c7a.2xlarge)"

expect_exit 0 "large pool on m7a.2xlarge fits one agent" \
    check "$(pools_file large-m7a large m7a.2xlarge)"

# The defaults in ../1-cluster/variables.tf, all three pools at once.
cat > "${work}/defaults.json" <<'JSON'
{
  "small":  { "node_group_names": ["agents-small-a", "agents-small-b"],   "asg_names": ["eks-agents-small-a-stub", "eks-agents-small-b-stub"],   "availability_zones": ["us-east-1a", "us-east-1b"], "instance_types": ["c7a.2xlarge"], "capacity_type": "SPOT", "disk_gib": 100, "min_size": 0, "max_size": 20,  "node_selector_label": "cassandra.jenkins.agent.small" },
  "medium": { "node_group_names": ["agents-medium-a", "agents-medium-b"], "asg_names": ["eks-agents-medium-a-stub", "eks-agents-medium-b-stub"], "availability_zones": ["us-east-1a", "us-east-1b"], "instance_types": ["c7a.2xlarge"], "capacity_type": "SPOT", "disk_gib": 100, "min_size": 0, "max_size": 150, "node_selector_label": "cassandra.jenkins.agent.medium" },
  "large":  { "node_group_names": ["agents-large-a", "agents-large-b"],   "asg_names": ["eks-agents-large-a-stub", "eks-agents-large-b-stub"],   "availability_zones": ["us-east-1a", "us-east-1b"], "instance_types": ["m7a.2xlarge"], "capacity_type": "SPOT", "disk_gib": 100, "min_size": 0, "max_size": 306, "node_selector_label": "cassandra.jenkins.agent.large" },
  "report": { "node_group_names": ["agents-report-a", "agents-report-b"], "asg_names": ["eks-agents-report-a-stub", "eks-agents-report-b-stub"], "availability_zones": ["us-east-1a", "us-east-1b"], "instance_types": ["m7a.2xlarge"], "capacity_type": "SPOT", "disk_gib": 100, "min_size": 0, "max_size": 4,   "node_selector_label": "cassandra.jenkins.agent.report" }
}
JSON

expect_exit 0 "the agent_pools defaults all fit" check "${work}/defaults.json"

# A pool with no pod template selecting it: nothing would ever run on those nodes.
cat > "${work}/orphan.json" <<'JSON'
{
  "enormous": { "node_group_names": ["agents-enormous-a", "agents-enormous-b"], "asg_names": ["eks-agents-enormous-a-stub", "eks-agents-enormous-b-stub"], "availability_zones": ["us-east-1a", "us-east-1b"], "instance_types": ["m7a.2xlarge"], "capacity_type": "SPOT", "disk_gib": 100, "min_size": 0, "max_size": 4, "node_selector_label": "cassandra.jenkins.agent.enormous" }
}
JSON

expect_exit 1 "a pool no pod template selects is reported" check "${work}/orphan.json"

# ---------------------------------------------------------------------------------------------------
# The deadlines on a pod's wait for a node
# ---------------------------------------------------------------------------------------------------

# The cases above read the real ../../jenkins-deployment.yaml, so they already assert that its own
# deadlines clear the floor.  These use a fixture instead, because the interesting values are the ones that
# file must never hold again.
#
# What they are for: build cassandra #7 created and deleted 1,110 pods in 24 minutes against a 30 s deadline, connected
# no agent, and logged 483 `went below zero` warnings.  A deadline shorter than a cold node's time to ready
# does not queue, it churns.  Nothing in Jenkins, the node group or the autoscaler reports that.

# One small template, whose requests are small enough that the pool fit is never the thing under test.
# $2 is the template's slaveConnectTimeout, or empty to omit the key; $3 is agent.waitForPodSec.
values_file() {
    local name="$1" timeout="$2" wait_for_pod="$3"
    local path="${work}/${name}.yaml"
    {
        echo "agent:"
        echo "  waitForPodSec: \"${wait_for_pod}\""
        echo "  podTemplates:"
        echo "    agent-dind-small: |"
        echo "      - name: agent-dind-small"
        echo "        nodeSelector: cassandra.jenkins.agent.small=true"
        if [ -n "${timeout}" ]; then echo "        slaveConnectTimeout: '${timeout}'"; fi
        echo "        containers:"
        echo "          - name: jnlp"
        echo "            resourceRequestCpu: 500m"
        echo "            resourceRequestMemory: 1G"
        echo "            resourceRequestEphemeralStorage: 2G"
    } > "${path}"
    echo "${path}"
}

check_values() {
    python3 "${here}/check-pool-fit.py" --estimate-only --region us-east-1 \
        --pools "$1" --values "$2"
}

small_pools="$(pools_file small-c7a small c7a.2xlarge)"

expect_exit 1 "a 30s slaveConnectTimeout fails" \
    check_values "${small_pools}" "$(values_file churn 30 900)"
if ! grep -q 'podTemplate agent-dind-small slaveConnectTimeout is 30s' "${work}/output"; then
    echo "FAIL  the churning template is named"
    sed 's/^/        /' "${work}/output"
    failures=$((failures + 1))
else
    echo "PASS  the churning template is named"
fi

expect_exit 0 "a 600s slaveConnectTimeout passes" \
    check_values "${small_pools}" "$(values_file patient 600 900)"

# Silence is not neutral: the plugin applies 100 s to a template that names no deadline, which is under the
# floor.  A template that dropped the key would otherwise pass this check and churn exactly as build cassandra #7 did.
expect_exit 1 "an unset slaveConnectTimeout fails" \
    check_values "${small_pools}" "$(values_file unset '' 900)"
if ! grep -q 'names no slaveConnectTimeout' "${work}/output"; then
    echo "FAIL  an unset deadline says which value applies"
    sed 's/^/        /' "${work}/output"
    failures=$((failures + 1))
else
    echo "PASS  an unset deadline says which value applies"
fi

# The cloud's own deadline binds too, and is the shorter one here.  This is the case that makes raising only
# the templates a fix that does not work: 180 s ends the wait whatever the template says.
expect_exit 1 "a low agent.waitForPodSec fails despite a patient template" \
    check_values "${small_pools}" "$(values_file half-fixed 600 180)"
if ! grep -q 'agent.waitForPodSec is 180s' "${work}/output"; then
    echo "FAIL  the cloud deadline is named"
    sed 's/^/        /' "${work}/output"
    failures=$((failures + 1))
else
    echo "PASS  the cloud deadline is named"
fi

# ---------------------------------------------------------------------------------------------------
# Two pod templates selecting one pool
# ---------------------------------------------------------------------------------------------------

# Every template has a pool to itself in the committed values, so this case is a fixture: a site with no
# pool to spare points agent-dind-report at a pool it already has, and that is supported.  The question is
# one pod against one node, so the pool must be judged on the most demanding of the templates on it.  The
# demanding template is written first on purpose: reading the templates in order and keeping the last would
# judge this pool on the modest one and pass a pool that fits nothing.
cat > "${work}/two-templates.yaml" <<'YAML'
agent:
  waitForPodSec: "900"
  podTemplates:
    agent-dind-report: |
      - name: agent-dind-report
        nodeSelector: cassandra.jenkins.agent.small=true
        slaveConnectTimeout: '600'
        containers:
          - name: jnlp
            resourceRequestCpu: 40
            resourceRequestMemory: 1G
            resourceRequestEphemeralStorage: 2G
    agent-dind-small: |
      - name: agent-dind-small
        nodeSelector: cassandra.jenkins.agent.small=true
        slaveConnectTimeout: '600'
        containers:
          - name: jnlp
            resourceRequestCpu: 500m
            resourceRequestMemory: 1G
            resourceRequestEphemeralStorage: 2G
YAML

expect_exit 1 "the larger of two templates on one pool decides" \
    check_values "${small_pools}" "${work}/two-templates.yaml"
if ! grep -q 'pod template  agent-dind-report, agent-dind-small' "${work}/output"; then
    echo "FAIL  both templates sharing a pool are named"
    sed 's/^/        /' "${work}/output"
    failures=$((failures + 1))
else
    echo "PASS  both templates sharing a pool are named"
fi

if [ "${failures}" -ne 0 ]; then
    echo "${failures} case(s) failed."
    exit 1
fi
echo "All cases behaved as recorded."
