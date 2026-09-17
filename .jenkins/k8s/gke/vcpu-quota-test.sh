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
# Offline cases for vcpu-quota.py.  Needs no cluster, no project and no credentials: it puts a stub `gcloud`
# on the PATH that answers `compute regions describe` from $STUB_QUOTA and `compute project-info describe`
# from $STUB_GLOBAL_QUOTA, and reads the numbers the script derives back off its `export` lines.
#
# What this exists to catch.  The AWS sibling's case is the one that cost a build 2 h 19 m: pools whose
# max_size sums to 480 nodes against an account quota of 384 vCPU, which is 48 nodes.  GCP adds a way to get
# that wrong that AWS does not have, and it is what most of the cases below cover: several metrics bind at
# once and the lowest of them decides.  A script that read CPUS alone would report a ceiling that
# PREEMPTIBLE_CPUS, N2_CPUS, SSD_TOTAL_GB or the global CPUS_ALL_REGIONS had already taken away, and the
# operator would raise CPUS and see nothing change.
#
#   ./vcpu-quota-test.sh
#
# Exits 0 when every case behaves as recorded below.

set -o pipefail

here="$(cd -- "$(dirname -- "$0")" && pwd)"
script="${here}/vcpu-quota.py"
# A full path with the template last, which BSD and GNU mktemp read alike, rather than a bare `mktemp -d`.
# On macOS a bare `mktemp -d` ignores TMPDIR and uses the Darwin per-user temp directory, so a suite run
# where TMPDIR is the only writable temp location cannot create its fixtures at all.  3-smoke/smoke-test.sh
# spells its own mktemp with a full path too, though for a different reason: BSD reads `-t` as a prefix.
work="$(mktemp -d "${TMPDIR:-/tmp}/cassandra-gke-test.XXXXXX")"
# Checked, because this script has no `set -e` and an unchecked failure here leaves ${work} empty: every
# fixture path then resolves to /, and the suite reports its own cases as failures.
[ -n "${work}" ] && [ -d "${work}" ] || { echo "mktemp -d failed, so there is nowhere to write the fixtures"; exit 1; }
trap 'rm -rf "${work}"' EXIT
failures=0

command -v python3 >/dev/null 2>&1 || { echo "python3 needs to be installed"; exit 1; }

# ---------------------------------------------------------------------------------------------------
# The stub `gcloud`
# ---------------------------------------------------------------------------------------------------

# Two calls are answered, which is every call vcpu-quota.py makes.  Anything else is an error, so a new call
# cannot pass this test by being silently unanswered.
#
# The quota tables come from $STUB_QUOTA (regional) and $STUB_GLOBAL_QUOTA (global), each a space separated
# list of METRIC=limit or METRIC=limit:usage.  A metric left out of the list is a metric the project does not
# report, which is how the "absent per-series quota" cases below are written.
mkdir -p "${work}/bin"
cat > "${work}/bin/gcloud" <<'STUB'
#!/bin/bash
quotas() {
    local spec="$1" first=1 entry metric figures limit usage
    printf '{"quotas":['
    for entry in ${spec}; do
        metric="${entry%%=*}"
        figures="${entry#*=}"
        limit="${figures%%:*}"
        usage="${figures#*:}"
        [ "${usage}" = "${figures}" ] && usage=0
        [ "${first}" = 1 ] || printf ','
        first=0
        printf '{"metric":"%s","limit":%s,"usage":%s}' "${metric}" "${limit}" "${usage}"
    done
    printf ']}\n'
}

case "$1 $2 $3" in
  'compute regions describe')
    quotas "${STUB_QUOTA}"
    ;;
  'compute project-info describe')
    quotas "${STUB_GLOBAL_QUOTA}"
    ;;
  *)
    echo "stub gcloud: unexpected call: $*" >&2
    exit 64
    ;;
esac
STUB
chmod +x "${work}/bin/gcloud"
export PATH="${work}/bin:${PATH}"

# ---------------------------------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------------------------------

# Quota tables written once and named, so that a case's own line says only what that case is about.  Every
# figure a case is not testing is set high enough not to bind.
spare="N2_CPUS=99999 E2_CPUS=99999 PREEMPTIBLE_CPUS=99999 PREEMPTIBLE_N2_CPUS=99999"
spare_disk="SSD_TOTAL_GB=9999999 DISKS_TOTAL_GB=9999999"
# The three tables cases reach for most, named so that each case's own line says only what it is about:
# nothing binding, PREEMPTIBLE_CPUS binding, and no per-series metric reported at all.
plenty="CPUS=4976 ${spare} ${spare_disk}"
spot_bound="CPUS=4976 E2_CPUS=99999 PREEMPTIBLE_CPUS=400 PREEMPTIBLE_N2_CPUS=99999 ${spare_disk}"
no_series="CPUS=4976 E2_CPUS=99999 PREEMPTIBLE_CPUS=99999 ${spare_disk}"
# The quota a fresh project starts near, which every node the pools may create does not fit into.
tight="CPUS=384 ${spare} ${spare_disk}"
export STUB_GLOBAL_QUOTA="CPUS_ALL_REGIONS=9999999"

# The defaults in 1-cluster/variables.tf, shaped like `tofu output -json agent_node_pools`.  Only the fields
# vcpu-quota.py reads are needed, and the rest are left out so that a change to them cannot silently change
# what this test asserts.
cat > "${work}/defaults.json" <<'JSON'
{
  "small":  { "machine_types": ["e2-standard-8"], "max_size": 20,  "spot": false, "disk_gb": 107, "disk_type": "pd-ssd", "node_pool_names": ["agents-small-a"],  "node_selector_label": "cassandra.jenkins.agent.small" },
  "medium": { "machine_types": ["e2-standard-8"], "max_size": 150, "spot": false, "disk_gb": 107, "disk_type": "pd-ssd", "node_pool_names": ["agents-medium-a"], "node_selector_label": "cassandra.jenkins.agent.medium" },
  "large":  { "machine_types": ["e2-standard-8"], "max_size": 306, "spot": false, "disk_gb": 107, "disk_type": "pd-ssd", "node_pool_names": ["agents-large-a"],  "node_selector_label": "cassandra.jenkins.agent.large" },
  "report": { "machine_types": ["e2-standard-8"], "max_size": 4,   "spot": false, "disk_gb": 107, "disk_type": "pd-ssd", "node_pool_names": ["agents-report-a"], "node_selector_label": "cassandra.jenkins.agent.report" }
}
JSON

python3 - "${work}/defaults.json" "${work}/n2.json" <<'PYTHON'
import sys
from pathlib import Path
Path(sys.argv[2]).write_text(Path(sys.argv[1]).read_text().replace("e2-standard", "n2-standard"))
PYTHON

# The same four pools on Spot, which is what var.agent_pools sets for every agent size that tolerates a
# pre-emption.  Their vCPU is charged to PREEMPTIBLE_CPUS and to nothing else.
cat > "${work}/spot.json" <<'JSON'
{
  "small":  { "machine_types": ["n2-standard-8"], "max_size": 20,  "spot": true, "disk_gb": 107, "disk_type": "pd-ssd", "node_selector_label": "cassandra.jenkins.agent.small" },
  "medium": { "machine_types": ["n2-standard-8"], "max_size": 150, "spot": true, "disk_gb": 107, "disk_type": "pd-ssd", "node_selector_label": "cassandra.jenkins.agent.medium" },
  "large":  { "machine_types": ["n2-standard-8"], "max_size": 306, "spot": true, "disk_gb": 107, "disk_type": "pd-ssd", "node_selector_label": "cassandra.jenkins.agent.large" },
  "report": { "machine_types": ["n2-standard-8"], "max_size": 4,   "spot": true, "disk_gb": 107, "disk_type": "pd-ssd", "node_selector_label": "cassandra.jenkins.agent.report" }
}
JSON

# One pool listing two machine types, to pin which of them is charged.
cat > "${work}/mixed.json" <<'JSON'
{
  "medium": { "machine_types": ["e2-standard-4", "e2-standard-16"], "max_size": 10, "spot": false, "disk_gb": 107, "disk_type": "pd-ssd", "node_selector_label": "cassandra.jenkins.agent.medium" }
}
JSON

# One pool listing two series, which may create a node of either, so both series' metrics can bind it.
cat > "${work}/two-series.json" <<'JSON'
{
  "medium": { "machine_types": ["n2-standard-8", "c2-standard-16"], "max_size": 10, "spot": false, "disk_gb": 107, "disk_type": "pd-ssd", "node_selector_label": "cassandra.jenkins.agent.medium" }
}
JSON

# Every pool at max_size 0, which is the same symptom as a quota too small and different advice.
cat > "${work}/empty.json" <<'JSON'
{
  "small": { "machine_types": ["e2-standard-8"], "max_size": 0, "spot": false, "disk_gb": 107, "disk_type": "pd-ssd", "node_selector_label": "cassandra.jenkins.agent.small" }
}
JSON

# Layer 1 scaled these to the lowest ceiling the project allows, so max_size is what the node pools were
# created with and declared_max_size is what var.agent_pools asked for.
cat > "${work}/scaled.json" <<'JSON'
{
  "medium": { "machine_types": ["e2-standard-8"], "max_size": 75,  "declared_max_size": 150, "spot": false, "disk_gb": 107, "disk_type": "pd-ssd", "node_selector_label": "cassandra.jenkins.agent.medium" },
  "large":  { "machine_types": ["e2-standard-8"], "max_size": 153, "declared_max_size": 306, "spot": false, "disk_gb": 107, "disk_type": "pd-ssd", "node_selector_label": "cassandra.jenkins.agent.large" }
}
JSON

# A shared-core type and a custom type, both refused by name rather than charged a guessed figure.  The
# custom one is the reason: its name ends in its memory, so reading the tail as a vCPU count would charge it
# 32768 vCPU a node and report a ceiling of nothing.
cat > "${work}/sharedcore.json" <<'JSON'
{
  "small": { "machine_types": ["e2-medium"], "max_size": 10, "spot": false, "disk_gb": 107, "disk_type": "pd-ssd", "node_selector_label": "cassandra.jenkins.agent.small" }
}
JSON
cat > "${work}/custom.json" <<'JSON'
{
  "small": { "machine_types": ["n2-custom-8-32768"], "max_size": 10, "spot": false, "disk_gb": 107, "disk_type": "pd-ssd", "node_selector_label": "cassandra.jenkins.agent.small" }
}
JSON

# ---------------------------------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------------------------------

# One run of the script, with the regional quota table the case names.
run() {
    local quota="$1" pools="$2"
    shift 2
    STUB_QUOTA="${quota}" python3 "${script}" \
        --pools "${pools}" --region us-central1 --project cassandra-jenkins \
        --controller-machine-types "${CONTROLLER_TYPES:-e2-standard-8}" \
        --controller-max-size "${CONTROLLER_MAX:-1}" "$@"
}

# The same run with its `export` lines sourced, so a case asserts on the variables 2-platform and 3-smoke
# will read rather than on the text around them.  Cleared first: without that, a run that printed nothing
# would be asserted against the previous case's figures.
run_exports() {
    unset GKE_VCPU_QUOTA GKE_VCPU_QUOTA_METRIC GKE_VCPU_DEMAND GKE_MAX_NODES_TOTAL GKE_MAX_AGENT_NODES
    eval "$(run "$@" 2>/dev/null)"
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

# ---------------------------------------------------------------------------------------------------
# CPUS, which is the only metric the AWS sibling has an equivalent of
# ---------------------------------------------------------------------------------------------------

# The quota a fresh project starts near, against the pools this cluster is written for: 480 agent nodes plus
# the controller, all 8 vCPU, ask 3848 against 384 allowed.  384 / 8 is 48 nodes, one of them the
# controller's.
run_exports "${tight}" "${work}/defaults.json"
expect "the quota is reported as given"      384   "${GKE_VCPU_QUOTA}"
expect "and the metric that bound is named"  CPUS  "${GKE_VCPU_QUOTA_METRIC}"
expect "the pools' full ask is summed"       3848  "${GKE_VCPU_DEMAND}"
expect "384 vCPU is 48 nodes"                48    "${GKE_MAX_NODES_TOTAL}"
expect "the controller takes one of them"    47    "${GKE_MAX_AGENT_NODES}"

# An over-ask is a warning and not a failure: the cluster runs, smaller than it is written for.
expect_exit 0 "an over-ask still exits 0" run "${tight}" "${work}/defaults.json"
expect_says "an over-ask is reported by name" 'may ask for 3848 vCPU of CPUS and the project allows 384'
expect_says "and the symptom a build sees is named" 'Pending pod'

# The quota this cluster asks for, which holds every node the pools may create.
run_exports "CPUS=3848 ${spare} ${spare_disk}" "${work}/defaults.json"
expect "3848 vCPU is 481 nodes"              481   "${GKE_MAX_NODES_TOTAL}"
expect "which is every agent the pools want" 480   "${GKE_MAX_AGENT_NODES}"
expect_exit 0 "the quota it asks for is silent" run "CPUS=3848 ${spare} ${spare_disk}" "${work}/defaults.json"
expect_silent_about "the quota it asks for warns about nothing" 'WARNING'

# A quota above the ask must not raise the ceiling: the node pools create 480 agent nodes and no more, so an
# agent.containerCap of 621 would have Jenkins ask for pods that wait on a node no pool will create.  That is
# the churn loop with the node pool in the project's place, and why this clamp exists.
run_exports "${plenty}" "${work}/defaults.json"
expect "a quota above the ask does not raise the ceiling" 480  "${GKE_MAX_AGENT_NODES}"
expect "nor the node total"                               481  "${GKE_MAX_NODES_TOTAL}"
expect "and the quota is still reported as given"         4976 "${GKE_VCPU_QUOTA}"
expect_exit 0 "a quota above the ask exits 0" run "${plenty}" "${work}/defaults.json" --check
expect_says "the binding ceiling is named" 'The node pools bind: 480 agents'
expect_says "raising a pool requires rechecking quotas" 'Recheck each quota'

# A quota too small for one agent is one of the two cases that stop a deploy: 8 vCPU holds one node, the
# controller's, and no build could ever run.
expect_exit 1 "a quota holding only the controller fails" run "CPUS=8 ${spare} ${spare_disk}" "${work}/defaults.json"
expect_says "and says no agent can run at all" 'no agent can run at all'

# The other one, and it is not the project's fault, so "raise the quota" would be wrong advice.
expect_exit 1 "every pool at max_size 0 fails" run "${plenty}" "${work}/empty.json"
expect_says "an empty pool is not blamed on the quota" 'every agent pool has max_size 0'

# A pool listing two types is charged for the larger.  A node pool may create either, so the smaller would
# under-count the ceiling and let the pools exceed the quota again.
run_exports "CPUS=1000 ${spare} ${spare_disk}" "${work}/mixed.json"
expect "a mixed pool is charged the larger type" 168 "${GKE_VCPU_DEMAND}"
expect "and one small pool binds the node total"  11 "${GKE_MAX_NODES_TOTAL}"
expect_exit 0 "the mixed pool reports" run "CPUS=1000 ${spare} ${spare_disk}" "${work}/mixed.json" --check
expect_says "the configured pool bounds its contribution" 'reported quotas allow 10 agents'

# A pool listing two series is charged to both series' metrics, because it may create a node of either.  Here
# C2_CPUS is the lower and binds at 64 / 16, which is four nodes, and charging the n2 metric alone would have
# reported the 99999 above.
run_exports "CPUS=1000 ${spare} C2_CPUS=64 ${spare_disk}" "${work}/two-series.json"
expect "a pool listing two series is charged to both" C2_CPUS "${GKE_VCPU_QUOTA_METRIC}"
expect "and the lower of the two binds"                     4 "${GKE_MAX_AGENT_NODES}"

# The largest node in the cluster sets the ceiling, whichever pool it is in.  Here the controller is the
# largest, so 1000 / 16 is 62 nodes and not 1000 / 8.
CONTROLLER_TYPES=e2-standard-16
run_exports "CPUS=1000 ${spare} ${spare_disk}" "${work}/defaults.json"
expect "a larger controller reserves its actual vCPUs" 124 "${GKE_MAX_NODES_TOTAL}"
unset CONTROLLER_TYPES

# ---------------------------------------------------------------------------------------------------
# The metrics GCP has and AWS does not
# ---------------------------------------------------------------------------------------------------

# Spot vCPU is charged to PREEMPTIBLE_CPUS and not to CPUS.  So a project with 4976 CPUS and 400
# PREEMPTIBLE_CPUS runs 50 Spot agent nodes, and raising CPUS does nothing about it.  The controller stays
# on-demand, so it is charged to CPUS and takes none of the 400: 50 agents, not 49.
run_exports "${spot_bound}" "${work}/spot.json"
expect "a Spot pool is bound by PREEMPTIBLE_CPUS" PREEMPTIBLE_CPUS "${GKE_VCPU_QUOTA_METRIC}"
expect "at that metric's limit and not CPUS'"     400              "${GKE_VCPU_QUOTA}"
expect "which is 50 Spot agent nodes"             50               "${GKE_MAX_AGENT_NODES}"
expect "and the controller's node is charged elsewhere, so it is not deducted" 51 "${GKE_MAX_NODES_TOTAL}"
expect_exit 0 "the Spot cluster reports" run "${spot_bound}" "${work}/spot.json" --check
expect_says "the Spot metric is the one named as binding" 'PREEMPTIBLE_CPUS binds: 50 agents'

# A per-series quota applies in addition to CPUS, so 240 N2_CPUS is 30 nodes whatever CPUS says.  This is the
# case a script that read CPUS alone would report as 621 agents.
run_exports "CPUS=4976 N2_CPUS=240 E2_CPUS=99999 PREEMPTIBLE_CPUS=99999 ${spare_disk}" "${work}/n2.json"
expect "a per-series quota below CPUS binds"  N2_CPUS "${GKE_VCPU_QUOTA_METRIC}"
expect "at its own limit"                     240     "${GKE_VCPU_QUOTA}"
expect "which is 30 agent nodes"              30      "${GKE_MAX_AGENT_NODES}"
expect "and the controller is charged to CPUS and not to N2_CPUS, so it is not deducted" 31 "${GKE_MAX_NODES_TOTAL}"

# A per-series metric the project does not report is unknown, not zero.  Treated as zero it would refuse
# every deploy; treated as unlimited without a word it would repeat the fault the sibling's non-standard
# family warning exists to name.  So the ceiling comes from CPUS and the gap is stated.
run_exports "${no_series}" "${work}/n2.json"
expect "an absent per-series quota is not read as zero" 480 "${GKE_MAX_AGENT_NODES}"
expect "the global metric still bounds the pool"          CPUS_ALL_REGIONS "${GKE_VCPU_QUOTA_METRIC}"
expect_exit 0 "an absent per-series quota still exits 0" run "${no_series}" "${work}/n2.json"
expect_says "the absent per-series quota is named" 'reports no N2_CPUS quota'
expect_says "and read as unknown rather than unlimited" 'not as unlimited'

# The global metric, which nothing regional reports and a project running anything in another region has
# already spent.  800 / 8 is 100 nodes, of which 99 may be agents.
STUB_GLOBAL_QUOTA="CPUS_ALL_REGIONS=800"
run_exports "${plenty}" "${work}/defaults.json"
expect "the global metric binds when it is the lowest" CPUS_ALL_REGIONS "${GKE_VCPU_QUOTA_METRIC}"
expect "at the global limit"                           800              "${GKE_VCPU_QUOTA}"
expect "which is 99 agent nodes"                       99               "${GKE_MAX_AGENT_NODES}"
STUB_GLOBAL_QUOTA="CPUS_ALL_REGIONS=9999999"

# The disk metric.  Every node carries a boot disk, and SSD_TOTAL_GB is small in a fresh project: 5000 GB at
# 107 GB a node is 46 nodes, well under any vCPU figure here.
run_exports "CPUS=4976 ${spare} SSD_TOTAL_GB=5000 DISKS_TOTAL_GB=9999999" "${work}/defaults.json"
expect "the disk quota binds when it is the lowest" SSD_TOTAL_GB "${GKE_VCPU_QUOTA_METRIC}"
expect "at its own limit, which is GB and not vCPU"  5000        "${GKE_VCPU_QUOTA}"
expect "which is 45 agent nodes"                     45          "${GKE_MAX_AGENT_NODES}"

# Usage is reported beside every limit, so headroom is known.  The sibling can only measure the ask; this can
# say what is left, and that a limit covering the ask does not mean the project can serve it.
expect_exit 0 "headroom is reported" run "CPUS=4976:2000 ${spare} ${spare_disk}" "${work}/defaults.json" --check
expect_says "the table carries usage and headroom" 'headroom +2,976'
expect_says "and what is left is checked against the ask" 'only 2976 is left against an ask of 3848'

# The pod range is a ceiling of its own, and the only one here that cannot be raised afterwards.
expect_exit 0 "a pod range below the quota still exits 0" run "${plenty}" "${work}/defaults.json" --pod-range-node-ceiling 100
expect_says "the pod range is named as binding first" 'pod range holds 100 agent node'
expect_says "and where the figure comes from"         'tofu output agent_node_ceiling'
expect_says "and that it takes a new cluster to change" 'cannot be raised after the cluster is created'
expect_exit 0 "a pod range above the quota says nothing" run "${tight}" "${work}/defaults.json" --pod-range-node-ceiling 100
expect_silent_about "because the quota binds first" 'pod range holds'

# ---------------------------------------------------------------------------------------------------
# Machine types read from their names
# ---------------------------------------------------------------------------------------------------

# A predefined type ends in its vCPU count, so no API call is needed and no permission with it.  Two shapes
# do not, and both are refused rather than guessed at.
expect_exit 2 "a shared-core type is refused" run "${plenty}" "${work}/sharedcore.json"
expect_says "and named as shared-core"  'e2-medium is a shared-core machine type'
expect_exit 2 "a custom type is refused" run "${plenty}" "${work}/custom.json"
expect_says "and named as custom, with the reason" 'ends in its memory'

# ---------------------------------------------------------------------------------------------------
# The scaled pools layer 1 writes
# ---------------------------------------------------------------------------------------------------

# When max_size and declared_max_size differ the arithmetic follows max_size, and the difference is reported
# rather than left silent.
run_exports "${plenty}" "${work}/scaled.json"
expect "a scaled pool is summed at what it was created with" 1832 "${GKE_VCPU_DEMAND}"
expect "and the agent ceiling follows the scaled size"        228 "${GKE_MAX_AGENT_NODES}"
expect_exit 0 "a scaled pool reports" run "${plenty}" "${work}/scaled.json" --check
expect_says "the scaling is reported" 'var.agent_pools asked for 456 agent nodes and the node pools were created with 228'

# The same JSON without declared_max_size, which is what layer 1 printed before it scaled anything.  It has
# to read unchanged, or a state written by an older apply stops working.
expect_exit 0 "output without declared_max_size still reports" run "${plenty}" "${work}/defaults.json" --check
expect_silent_about "unscaled pools say nothing about scaling" 'var.agent_pools asked for'

# ---------------------------------------------------------------------------------------------------
# The Makefile's own path
# ---------------------------------------------------------------------------------------------------

# --exports-to writes the same lines the reader gets, so two gcloud calls serve both.
expect_exit 0 "--exports-to writes the dot-file" run "${tight}" "${work}/defaults.json" --check --exports-to "${work}/.gke-vcpu-quota"
if [ -f "${work}/.gke-vcpu-quota" ]; then
    unset GKE_VCPU_QUOTA GKE_VCPU_QUOTA_METRIC GKE_VCPU_DEMAND GKE_MAX_NODES_TOTAL GKE_MAX_AGENT_NODES
    # shellcheck disable=SC1090
    . "${work}/.gke-vcpu-quota"
    expect "the dot-file carries the same ceiling as --check printed" 47 "${GKE_MAX_AGENT_NODES}"
    expect "and the metric with it"                                CPUS "${GKE_VCPU_QUOTA_METRIC}"
else
    echo "FAIL  --exports-to writes a file that can be sourced"
    failures=$((failures + 1))
fi

# A missing project is refused before any gcloud call, because quotas are the project's and a run against the
# wrong one reports the wrong ceiling.  In a subshell with the variable unset, so that an operator who has
# GOOGLE_PROJECT set in their own environment sees the same result as CI does.
run_without_project() {
    (
        unset GOOGLE_PROJECT
        STUB_QUOTA="CPUS=384" python3 "${script}" --pools "${work}/defaults.json" --region us-central1 \
            --controller-machine-types e2-standard-8
    )
}
expect_exit 2 "a missing project is refused" run_without_project
expect_says "and says which variable to set" 'GOOGLE_PROJECT'

if [ "${failures}" -ne 0 ]; then
    echo "${failures} case(s) failed."
    exit 1
fi
echo "All cases behaved as recorded."
