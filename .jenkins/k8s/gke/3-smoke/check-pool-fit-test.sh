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
# Regression test for check-pool-fit.py.  Needs no cluster, no GCP project and no credential: it puts a stub
# `gcloud` on the PATH that answers `compute machine-types describe` from the machine type's own name, and
# runs the check in --estimate-only mode.
#
# The case it exists for is the one that was got wrong in the runbook this directory replaces, and which a
# reader cannot see by eye: agent-dind-large requests 16G for dind and 1G for jnlp.  Those are decimal G, so
# 17G is 15.83 GiB, which is more than an 8 GiB node has left after GKE's reservations, and comfortably less
# than a 32 GiB one has.  A large pool on e2-highcpu-8 (8 GiB) fits no agents; on n2-standard-8 (32 GiB) it
# fits one.
#
#   ./check-pool-fit-test.sh
#
# Exits 0 when every case behaves as recorded below.

set -o pipefail

here="$(cd -- "$(dirname -- "$0")" && pwd)"
# A full path with the template last, which BSD and GNU mktemp read alike, rather than a bare `mktemp -d`.
# On macOS a bare `mktemp -d` ignores TMPDIR and uses the Darwin per-user temp directory, so a suite run
# where TMPDIR is the only writable temp location cannot create its fixtures at all.  3-smoke/smoke-test.sh
# spells its own mktemp with a full path too, though for a different reason: BSD reads `-t` as a prefix.
work="$(mktemp -d "${TMPDIR:-/tmp}/cassandra-gke-test.XXXXXX")"
# Checked, because this script has no `set -e` and an unchecked failure here leaves ${work} empty: every
# fixture path then resolves to /, and the suite reports its own cases as failures.
[ -n "${work}" ] && [ -d "${work}" ] || { echo "mktemp -d failed, so there is nowhere to write the fixtures"; exit 1; }
trap 'rm -rf "${work}"' EXIT

command -v python3 >/dev/null 2>&1 || { echo "python3 needs to be installed"; exit 1; }
python3 -c 'import yaml' 2>/dev/null || { echo "SKIP  check-pool-fit-test.sh needs PyYAML"; exit 0; }

# ---------------------------------------------------------------------------------------------------
# The stub `gcloud`
# ---------------------------------------------------------------------------------------------------

# vCPU and memory are derived from the machine type's name rather than listed, so a case may name a type this
# stub has never heard of: `<series>-<family>-<vcpus>`, with 4 GiB per vCPU for standard, 1 GiB for highcpu
# and 8 GiB for highmem.  Those are GCP's own ratios for the e2, n2 and c2 series, which is what makes the
# derived figure the published one: e2-highcpu-8 is 8 GiB and n2-standard-8 is 32 GiB.
#
# GKE's reservation on an 8 GiB node is then 255 MiB + 25% of 4 GiB + 20% of the remaining 4 GiB = 2098 MiB,
# plus 100 MiB held back for the hard eviction threshold, leaving 5.85 GiB for a pod.  On a 32 GiB node it is
# 255 + 1024 + 819 + 819 + 6% of the 16 GiB above 16 = 3900 MiB, plus the same 100, leaving 28.09 GiB.
mkdir -p "${work}/bin"
cat > "${work}/bin/gcloud" <<'STUB'
#!/bin/bash
# Stub. Answers only `compute machine-types describe`.
if [ "$1" = "compute" ] && [ "$2" = "machine-types" ] && [ "$3" = "describe" ]; then
    name="$4"
    zone=""
    while [ $# -gt 0 ]; do
        case "$1" in
            --zone) zone="$2"; shift 2 ;;
            --zone=*) zone="${1#--zone=}"; shift ;;
            *) shift ;;
        esac
    done
    # Refused rather than defaulted: machine types are zonal, and a lookup with no zone is the bug this
    # refusal exists to catch.
    if [ -z "${zone}" ]; then
        echo "stub gcloud: machine-types describe with no --zone, and machine types are zonal" >&2
        exit 64
    fi
    family="$(printf '%s' "${name}" | cut -d- -f2)"
    vcpus="$(printf '%s' "${name}" | cut -d- -f3)"
    case "${family}" in
        standard) mib_per_vcpu=4096 ;;
        highcpu)  mib_per_vcpu=1024 ;;
        highmem)  mib_per_vcpu=8192 ;;
        *) echo "stub gcloud: no memory ratio recorded for the '${family}' family of '${name}'" >&2; exit 64 ;;
    esac
    case "${vcpus}" in
        ''|*[!0-9]*) echo "stub gcloud: '${name}' does not end in a vCPU count" >&2; exit 64 ;;
    esac
    printf '{"name": "%s", "zone": "%s", "guestCpus": %s, "memoryMb": %s}\n' \
        "${name}" "${zone}" "${vcpus}" "$((vcpus * mib_per_vcpu))"
    exit 0
fi
# Anything else is an error, so a new call cannot pass this test by being silently unanswered.
echo "stub gcloud: unexpected call: $*" >&2
exit 64
STUB
chmod +x "${work}/bin/gcloud"

export PATH="${work}/bin:${PATH}"

# ---------------------------------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------------------------------

# Shaped like `tofu output -json agent_node_pools` in ../1-cluster: one row per pool size, and one node pool
# per zone inside it, each pool pinned to that one zone.  Two zones here; the default zone count is every zone
# in the region, and the count does not change what this checks.
#
# Every pool names its zones, so no case reaches the stub's `compute zones list`: the machine type is
# described in the pool's own first zone, which is a zone the pool was created in.
pools_file() {
    local name="$1" size="$2" machine_type="$3" disk_gb="${4:-200}"
    cat > "${work}/${name}.json" <<JSON
{
  "${size}": {
    "node_pool_names": ["agents-${size}-a", "agents-${size}-b"],
    "instance_group_urls": ["https://www.googleapis.com/compute/v1/projects/stub/zones/us-central1-a/instanceGroupManagers/gke-${size}-a-stub"],
    "zones": ["us-central1-a", "us-central1-b"],
    "machine_types": ["${machine_type}"],
    "spot": false,
    "disk_gb": ${disk_gb},
    "disk_type": "pd-ssd",
    "max_pods_per_node": 110,
    "min_size": 0,
    "max_size": 160,
    "declared_max_size": 160,
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
    python3 "${here}/check-pool-fit.py" --estimate-only --region us-central1 --pools "$1"
}

expect_exit 1 "large pool on e2-highcpu-8 fits no agent" \
    check "$(pools_file large-e2 large e2-highcpu-8)"

expect_exit 0 "large pool on n2-standard-8 fits one agent" \
    check "$(pools_file large-n2 large n2-standard-8)"

# The defaults in ../1-cluster/variables.tf, all four pools at once.  This case reads the real committed
# ../../jenkins-deployment.yaml, so it doubles as an assertion that the committed requests fit the committed
# machine types and that the committed deadlines clear the floor.
cat > "${work}/defaults.json" <<'JSON'
{
  "small":  { "node_pool_names": ["agents-small-a", "agents-small-b"],   "zones": ["us-central1-a", "us-central1-b"], "machine_types": ["e2-highcpu-8"], "spot": false, "disk_gb": 200, "disk_type": "pd-ssd", "max_pods_per_node": 110, "min_size": 0, "max_size": 10,  "declared_max_size": 10,  "node_selector_label": "cassandra.jenkins.agent.small" },
  "medium": { "node_pool_names": ["agents-medium-a", "agents-medium-b"], "zones": ["us-central1-a", "us-central1-b"], "machine_types": ["n2-highcpu-8"], "spot": false, "disk_gb": 200, "disk_type": "pd-ssd", "max_pods_per_node": 110, "min_size": 0, "max_size": 190, "declared_max_size": 190, "node_selector_label": "cassandra.jenkins.agent.medium" },
  "large":  { "node_pool_names": ["agents-large-a", "agents-large-b"],   "zones": ["us-central1-a", "us-central1-b"], "machine_types": ["n2-standard-8"], "spot": false, "disk_gb": 200, "disk_type": "pd-ssd", "max_pods_per_node": 110, "min_size": 0, "max_size": 276, "declared_max_size": 276, "node_selector_label": "cassandra.jenkins.agent.large" },
  "report": { "node_pool_names": ["agents-report-a", "agents-report-b"], "zones": ["us-central1-a", "us-central1-b"], "machine_types": ["n2-standard-8"], "spot": false, "disk_gb": 200, "disk_type": "pd-ssd", "max_pods_per_node": 110, "min_size": 0, "max_size": 4,   "declared_max_size": 4,   "node_selector_label": "cassandra.jenkins.agent.report" }
}
JSON

expect_exit 0 "the agent_pools defaults all fit" check "${work}/defaults.json"

# A single small agent fits physically but is held by the outer pipeline while its JAR task queues.
python3 - "${work}/defaults.json" "${work}/one-small.json" "${work}/two-small.json" <<'PY'
import json, sys
data = json.load(open(sys.argv[1]))
data['small']['max_size'] = 1
json.dump(data, open(sys.argv[2], 'w'))
data['small']['max_size'] = 2
json.dump(data, open(sys.argv[3], 'w'))
PY
expect_exit 1 "one small slot cannot run the outer pipeline and a JAR task" check "${work}/one-small.json"
if ! grep -q 'small pool needs at least 4' "${work}/output"; then
    echo "FAIL  the missing small worker slot is explained"
    failures=$((failures + 1))
fi
expect_exit 1 "two small slots cannot hold a pipeline and three workers" check "${work}/two-small.json"
python3 - "${work}/two-small.json" <<'PYTHON'
import json, sys
data = json.load(open(sys.argv[1]))
data['small']['small_workers_per_build'] = 1
json.dump(data, open(sys.argv[1], 'w'))
PYTHON
expect_exit 0 "two slots suffice when budgeting one worker per pipeline" check "${work}/two-small.json"

# A pool with no pod template selecting it: nothing would ever run on those nodes.
cat > "${work}/orphan.json" <<'JSON'
{
  "enormous": { "node_pool_names": ["agents-enormous-a", "agents-enormous-b"], "zones": ["us-central1-a", "us-central1-b"], "machine_types": ["n2-standard-8"], "spot": false, "disk_gb": 107, "disk_type": "pd-ssd", "max_pods_per_node": 110, "min_size": 0, "max_size": 4, "declared_max_size": 4, "node_selector_label": "cassandra.jenkins.agent.enormous" }
}
JSON

expect_exit 1 "a pool no pod template selects is reported" check "${work}/orphan.json"

# A 107-size boot disk passed the old EKS-derived model, but GKE refused to
# scale up the real 50Gi small agent with Insufficient ephemeral-storage.
expect_exit 1 "GKE reservations prevent a 50Gi agent fitting the old disk" \
    check "$(pools_file small-old-disk small e2-highcpu-8 107)"
if ! grep -q 'of ephemeral, against' "${work}/output"; then
    echo "FAIL  the old disk is rejected for ephemeral storage"
    failures=$((failures + 1))
fi
expect_exit 0 "a larger boot disk fits the same small agent" \
    check "$(pools_file small-larger-disk small e2-highcpu-8 200)"

# The running node's allocatable value must override the boot-disk estimate.
# Subtract the DaemonSet's request, but not another copy of GKE's reservation.
cat > "${work}/bin/kubectl" <<'STUB'
#!/bin/bash
case "$*" in
    *"get nodes"*)
        printf '{"items":[{"metadata":{"name":"small-node"},"status":{"conditions":[{"type":"Ready","status":"True"}],"allocatable":{"cpu":"7910m","memory":"6Gi","ephemeral-storage":"%s"}}}]}\n' "${MOCK_NODE_EPHEMERAL}"
        ;;
    *"get pods"*)
        echo '{"items":[{"metadata":{"name":"system-agent","ownerReferences":[{"kind":"DaemonSet"}]},"spec":{"containers":[{"resources":{"requests":{"cpu":"100m","memory":"100Mi","ephemeral-storage":"2Gi"}}}]}}]}'
        ;;
    *) echo "stub kubectl: unexpected call: $*" >&2; exit 64 ;;
esac
STUB
chmod +x "${work}/bin/kubectl"
check_measured() {
    MOCK_NODE_EPHEMERAL="$2" python3 "${here}/check-pool-fit.py" --pools "$1"
}
expect_exit 1 "measured capacity minus DaemonSets rejects an undersized node" \
    check_measured "$(pools_file measured-small small e2-highcpu-8 200)" 51Gi
expect_exit 1 "the reported COS allocatable value rejects the 50Gi agent" \
    check_measured "$(pools_file measured-cos small e2-highcpu-8 107)" 51571464850
expect_exit 0 "measured capacity is used even when the disk estimate would fail" \
    check_measured "$(pools_file measured-small-old small e2-highcpu-8 107)" 60Gi

# ---------------------------------------------------------------------------------------------------
# The deadlines on a pod's wait for a node
# ---------------------------------------------------------------------------------------------------

# The cases above read the real ../../jenkins-deployment.yaml, so they already assert that its own deadlines
# clear the floor.  These use a fixture instead, because the interesting values are the ones that file must
# never hold again.
#
# What they are for: build cassandra #7 created and deleted 1,110 pods in 24 minutes against a 30 s deadline,
# connected no agent, and logged 483 `went below zero` warnings.  A deadline shorter than a cold node's time
# to ready does not queue, it churns.  Nothing in Jenkins, the node pool or the autoscaler reports that, and
# on GKE there is not even an autoscaler pod whose log could be read for it.

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
    python3 "${here}/check-pool-fit.py" --estimate-only --region us-central1 \
        --pools "$1" --values "$2"
}

small_pools="$(pools_file small-e2 small e2-highcpu-8)"

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

# Every template has a pool to itself in the committed values, so this case is a fixture: a site with no pool
# to spare points agent-dind-report at a pool it already has, and that is supported.  The question is one pod
# against one node, so the pool must be judged on the most demanding of the templates on it.  The demanding
# template is written first on purpose: reading the templates in order and keeping the last would judge this
# pool on the modest one and pass a pool that fits nothing.
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

# ---------------------------------------------------------------------------------------------------
# A machine type described without a zone
# ---------------------------------------------------------------------------------------------------

# A pool that names no zones, run with no --region either: machine types are zonal, so there is nothing to
# describe the type in and the answer is "could not be established" rather than "does not fit".  Exit 2, which
# smoke-test.sh reports differently from a pool that fits nothing.
cat > "${work}/zoneless.json" <<'JSON'
{
  "large": { "node_pool_names": ["agents-large-a"], "zones": [], "machine_types": ["n2-standard-8"], "spot": false, "disk_gb": 107, "disk_type": "pd-ssd", "max_pods_per_node": 110, "min_size": 0, "max_size": 4, "declared_max_size": 4, "node_selector_label": "cassandra.jenkins.agent.large" }
}
JSON

check_zoneless() {
    # GKE_LOCATION and GOOGLE_REGION cleared, because check-pool-fit.py falls back to them and a developer's
    # own shell may hold either.  Unset in a subshell rather than through `env -u`, so that python3 is still
    # resolved by this shell: a version manager's shim resolved afresh by env is a different failure again.
    ( unset GKE_LOCATION GOOGLE_REGION
      python3 "${here}/check-pool-fit.py" --estimate-only --pools "${work}/zoneless.json" )
}

expect_exit 2 "a pool with no zone cannot be established either way" check_zoneless
if ! grep -q 'machine types are zonal' "${work}/output"; then
    echo "FAIL  the zoneless refusal says why"
    sed 's/^/        /' "${work}/output"
    failures=$((failures + 1))
else
    echo "PASS  the zoneless refusal says why"
fi

if [ "${failures}" -ne 0 ]; then
    echo "${failures} case(s) failed."
    exit 1
fi
echo "All cases behaved as recorded."
