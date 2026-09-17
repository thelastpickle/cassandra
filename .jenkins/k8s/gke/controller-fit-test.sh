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

# Offline cases for controller-fit.py, against a stubbed `gcloud` CLI.
#
# What this exists to catch.  The controller is the one ceiling in this directory that costs money to raise
# while nothing runs, so the recommendation has to be right in both directions: silent when the configured
# pools fit, and specific about the size when they do not.  A model that recommends an upgrade nobody needs
# costs the operator a few hundred dollars a month for nothing.
#
# The GKE-specific part is the reservation schedule.  GKE holds back 255 MiB plus a quarter of a node's first
# 4 GiB, which the AWS sibling's flat 7% does not express: on a 8 GiB node the flat figure reads 7618 MiB
# where GKE offers 6248, and the two disagree about whether the same controller is schedulable.  One case
# below is written on exactly that node for exactly that reason.
#
# The last section covers a different failure with the same inputs: a `requests` figure above the node's
# allocatable, which is a controller that never starts rather than one that runs slowly.  It also covers
# --overrides, without which this models the shared values file rather than the one that gets deployed.
#
# Needs no credentials and no project.  The stub answers `compute machine-types describe` and `compute
# machine-types list`, deriving both from the machine type's own name, and nothing else.

set -o pipefail
here="$(cd -- "$(dirname -- "$0")" && pwd)"
script="${here}/controller-fit.py"
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
python3 -c 'import yaml' 2>/dev/null || { echo "SKIP  controller-fit-test.sh needs PyYAML"; exit 0; }

# ---------------------------------------------------------------------------------------------------
# The stub
# ---------------------------------------------------------------------------------------------------

mkdir -p "${work}/bin"
cat > "${work}/bin/gcloud" <<'STUB'
#!/bin/bash
# Answers the two calls controller-fit.py makes.  Anything else is an error, so a new call cannot pass this
# test by being silently unanswered.

# A shape derived from the machine type's own name, so a case can name a type this stub never listed: the
# tail is the vCPU count and the family word decides the memory a vCPU carries.  These are the real ratios,
# 4 GiB a vCPU for standard and 1 GiB for highcpu, so n2-standard-8 reports 32768 as Compute Engine does.
shape() {
    local name="$1" family tail per
    tail="${name##*-}"
    family="$(printf '%s' "${name}" | cut -d- -f2)"
    case "${tail}" in
        ''|*[!0-9]*) echo "stub gcloud: '${name}' does not end in a vCPU count" >&2; exit 64 ;;
    esac
    case "${family}" in
        standard) per=4096 ;;
        highcpu)  per=1024 ;;
        highmem)  per=8192 ;;
        *) echo "stub gcloud: no memory rule for the family in '${name}'" >&2; exit 64 ;;
    esac
    case "${name}" in n1-*) per=3840 ;; esac
    printf '{"name":"%s","guestCpus":%s,"memoryMb":%s}' "${name}" "${tail}" "$((tail * per))"
}

# The zone's ladder for one series and family, which is per-series and non-uniform: e2-standard stops at 32,
# n2-standard runs to 128, and c2-standard numbers its sizes 4, 8, 16, 30, 60.  A family absent from here
# lists nothing, which is the path FALLBACK_LADDERS answers.
ladder() {
    case "$1" in
        e2-standard) echo "2 4 8 16 32" ;;
        e2-highcpu)  echo "2 4 8 16 32" ;;
        n2-standard) echo "2 4 8 16 32 48 64 80 96 128" ;;
        n2-highcpu)  echo "2 4 8 16 32 48 64 80 96" ;;
        c2-standard) echo "4 8 16 30 60" ;;
        *) echo "" ;;
    esac
}

case "$1 $2 $3" in
  'compute machine-types describe')
    shape "$4"
    printf '\n'
    ;;
  'compute machine-types list')
    [ "x${STUB_NO_LIST}" = "x" ] || { echo "PERMISSION_DENIED: compute.machineTypes.list" >&2 ; exit 1 ; }
    family=""
    for arg in "$@"; do
      case "${arg}" in
        --filter=name~*) family="$(printf '%s' "${arg}" | sed 's/^--filter=name~\^//; s/-\[0-9\]+\$$//')" ;;
      esac
    done
    [ -n "${family}" ] || { echo "stub gcloud: no name filter in: $*" >&2 ; exit 64 ; }
    first=1
    printf '['
    for size in $(ladder "${family}"); do
      [ "${first}" = 1 ] || printf ','
      first=0
      shape "${family}-${size}"
    done
    printf ']\n'
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

# The four pools, with max_size the only field this script reads from them.
pools_file() {
    cat > "${work}/pools.json" <<JSON
{
  "small":  { "max_size": $1, "machine_types": ["n2-standard-8"], "node_selector_label": "cassandra.jenkins.agent.small" },
  "medium": { "max_size": $2, "machine_types": ["n2-standard-8"], "node_selector_label": "cassandra.jenkins.agent.medium" },
  "large":  { "max_size": $3, "machine_types": ["n2-standard-8"], "node_selector_label": "cassandra.jenkins.agent.large" },
  "report": { "max_size": $4, "machine_types": ["n2-standard-8"], "node_selector_label": "cassandra.jenkins.agent.report" }
}
JSON
}

# The controller pool.  Its `zone` is read as well as its machine type, because machine types are zonal on
# GCP and there is nowhere else for the zone to come from.
controller_file() {
    # ${2-default} and not ${2:-default}: a case that passes an empty second parameter means an empty zone,
    # which is the input the last case in this file is about.
    printf '{"machine_types": ["%s"], "zone": "%s", "spot": false, "max_size": 1}\n' \
        "$1" "${2-us-central1-a}" > "${work}/controller.json"
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

# A values file run-ci applies after the deployment, which is what 2-platform/jenkins-gke-overrides.yaml is.
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

# `python3 "${script}"` and not `"${script}"`: the interpreter is the one this suite already checked PyYAML
# against a few lines above, rather than whatever the shebang's `env python3` happens to resolve to.  A shell
# whose PATH python3 and whose `env python3` differ would otherwise run the SKIP check against one interpreter
# and every case against another, and report the second one's missing PyYAML as 54 failed cases.
run() {
    python3 "${script}" --pools "${work}/pools.json" --controller "${work}/controller.json" \
        --deployment "${work}/deployment.yaml" "$@" > "${work}/output" 2>&1
}

# The same run with no price to read, which is the common case: in a subshell, so that the variable is empty
# for this run alone and a later case cannot inherit it.
run_without_price() {
    (
        export GKE_SPEND_PRICE_PER_VCPU_HOUR=""
        run "$@"
    )
}

run_merged() {
    python3 "${script}" --pools "${work}/pools.json" --controller "${work}/controller.json" \
        --deployment "${work}/deployment.yaml" --overrides "${work}/overrides.yaml" \
        "$@" > "${work}/output" 2>&1
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

# The per-vCPU rate the spend guard carries, which is the only price figure available offline: there is no
# gcloud equivalent of the AWS pricing API, so a cost figure is an estimate from this or nothing at all.
export GKE_SPEND_PRICE_PER_VCPU_HOUR=0.035

# ---------------------------------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------------------------------

# The pools this cluster is written for: 480 agent nodes on the committed controller.  This fits, and the
# check has to stay quiet about upgrading rather than sell one.
pools_file 20 150 306 4
controller_file e2-standard-8
deployment_file 8 20G 8G 5
run --check
expect_says "480 agents on e2-standard-8 is reported as holding" 'controller holds this'
expect_silent_about "and recommends nothing" 'Recommended|WARNING'
expect_says "the monthly cost of what is running is stated" '\$204 a month'
expect_says "and that a list-price month over-reads a GCP bill" 'sustained'

# GKE's reservation is written down where the sibling's flat 7% would be, and the two disagree.  e2-standard-8
# allocates 7910m of 8000m, which the flat figure gets right, and 29022 MiB of 32768, which it does not: 7% of
# 32 GiB is 30474.
expect_says "the node's allocatable is reported beside its capacity" 'e2-standard-8 allocatable +7910m cpu +29022 MiB'
expect_says "and the eviction threshold is named as part of it" '100 MiB eviction threshold'

# The pools after a quota grant: 1122 agent nodes, which is what pushed the sibling's live controller to its
# node ceiling.
pools_file 46 351 716 9
run --check
expect_says "1122 agents does not fit e2-standard-8" 'WARNING'
expect_says "and e2-standard-16 is recommended" 'Recommended: e2-standard-16'
expect_says "with the monthly figure named" '\$409 a month'
expect_says "and the difference from what runs now" '\$204 a month more'
expect_says "the 24/7 cost is called out against the agents' billing" 'billed by the'
expect_says "idleMinutes is offered as the cheaper lever" 'idleMinutes'

# The ladder is per-series and non-uniform, so the search is over the sizes that exist and not over every
# even number: e2-standard has no 12.
expect_silent_about "no size off the e2-standard ladder is recommended" 'e2-standard-(12|24|48|64)'

# idleMinutes is the cpu half of the model, so raising it has to lower the requirement.  Same pools, same
# controller, idleMinutes doubled.
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
deployment_file 8 20G 8G 5
run --check
heap_8="$(grep -oE 'needs.* ([0-9]+) MiB' "${work}/output" | grep -oE '[0-9]+ MiB' | head -1)"
deployment_file 8 20G 16G 5
run --check
heap_16="$(grep -oE 'needs.* ([0-9]+) MiB' "${work}/output" | grep -oE '[0-9]+ MiB' | head -1)"
if [ "${heap_8%% *}" -lt "${heap_16%% *}" ]; then
    echo "PASS  a larger heap raises the memory requirement (${heap_8} -> ${heap_16})"
else
    echo "FAIL  a larger heap must raise the memory requirement: ${heap_8} vs ${heap_16}"
    failures=$((failures + 1))
fi

# A cluster already on a big controller is told it fits, and is not sold a bigger one.
pools_file 46 351 716 9
controller_file e2-standard-32
deployment_file 30 120G 32G 5
run --check
expect_says "1122 agents on e2-standard-32 is reported as holding" 'controller holds this'
expect_silent_about "and no upgrade is recommended" 'Recommended:'

# Pools past what the largest size in the family can hold: say so and name the two levers, rather than
# recommending a machine type that does not exist.
pools_file 2000 20000 40000 100
controller_file e2-standard-8
deployment_file 8 20G 8G 5
run --check
expect_says "pools past the family's largest size are named as such" 'No size in the e2-standard family'
expect_says "and the two levers are given" 'max_size|idleMinutes'

# Without a price the sizing still stands; only the cost figure is missing.  There is no gcloud equivalent of
# the AWS pricing API, so this is the common case rather than the degraded one.
pools_file 46 351 716 9
run_without_price --check
expect_says "the recommendation survives an unknown price" 'Recommended: e2-standard-16'
expect_says "and says why there is no cost figure" 'no price was read'

# The export form, for the Makefile.
pools_file 46 351 716 9
deployment_file 8 20G 8G 5
run
expect_says "exports the recommendation" "GKE_CONTROLLER_RECOMMENDED='e2-standard-16'"
expect_says "exports that it does not fit" "GKE_CONTROLLER_FITS='false'"
expect_says "exports the cpu the model asks for" "GKE_CONTROLLER_NEEDS_CPU='7583'"
expect_says "exports the memory the model asks for" "GKE_CONTROLLER_NEEDS_MEMORY_MIB='18463'"

# The documented trap: _RECOMMENDED is the smallest type that holds the load, so it is populated even when
# _FITS is true, and an empty string is the only "no recommendation" signal.
pools_file 20 150 306 4
run
expect_says "a controller that fits still exports a recommendation" "GKE_CONTROLLER_RECOMMENDED='e2-standard-8'"
expect_says "beside a true fit" "GKE_CONTROLLER_FITS='true'"

# An under-sized controller refuses, because a warning was not enough; see controller-fit.py.  The operator
# who has read the figures passes --warn-only, which is what CONTROLLER_FIT_ARGS carries.
pools_file 46 351 716 9
expect_exit 1 "an under-sized controller refuses" run --check
expect_exit 0 "and --warn-only deploys anyway" run --check --warn-only
pools_file 20 150 306 4
expect_exit 0 "a controller that fits exits 0" run --check

# Every pool at zero is the one case that cannot be answered.
pools_file 0 0 0 0
expect_exit 1 "every pool at max_size 0 fails" run --check

# ---------------------------------------------------------------------------------------------------
# GKE's reservation schedule, which is the difference this port turns on
# ---------------------------------------------------------------------------------------------------

# A small node, where the schedule and a flat 7% disagree most.  e2-standard-2 has 8192 MiB: GKE reserves 255
# plus a quarter of the first 4 GiB plus a fifth of the next, holds back 100 MiB more for eviction, and leaves
# 6248.  A flat 7% would read 7618.  The load below needs 7025 MiB with its headroom, which is to say it fits
# under the sibling's model and does not fit under GKE's, and the verdict has to follow GKE's.
pools_file 50 50 50 50
controller_file e2-standard-2
deployment_file 2 8G 2G 60
run --check
expect_says "a small node's allocatable follows the GKE schedule" 'e2-standard-2 allocatable +1930m cpu +6248 MiB'
expect_silent_about "and not a flat 7% of capacity" '7618 MiB'
expect_says "so 200 agents are reported as not held with margin" 'no margin|cannot hold this'
expect_silent_about "rather than as held, which a flat 7% would have said" 'The controller holds this'
expect_says "and the next size up is recommended" 'Recommended: e2-standard-4'
expect_exit 1 "a marginal small node refuses" run --check

# The cpu half of the schedule is per-core and stops at 0.25% beyond the fourth, so it is not a fraction of
# capacity either: 2000m less 70m here, 8000m less 90m above.
expect_says "the cpu reservation is per-core and not a fraction" 'e2-standard-2 allocatable +1930m'

# ---------------------------------------------------------------------------------------------------
# The ladder, which is per-series and asked of gcloud
# ---------------------------------------------------------------------------------------------------

# c2-standard numbers its sizes 4, 8, 16, 30, 60.  A load between the 16 and the 30 has to land on the 30,
# which no arithmetic on the current size would produce.
pools_file 500 1000 1500 0
controller_file c2-standard-4
deployment_file 4 16G 8G 5
run --check
expect_says "the c2-standard ladder's own rungs are searched" 'Recommended: c2-standard-30'

# A zone gcloud will not list falls back to the written-down ladder, and reaches the same answer.
pools_file 46 351 716 9
controller_file e2-standard-8
deployment_file 8 20G 8G 5
STUB_NO_LIST=1 run --check
expect_says "an unlistable zone still recommends from the written-down ladder" 'Recommended: e2-standard-16'

# A series in neither place gets no recommendation, which is reported as no size holding the load rather than
# guessed at.  That is the honest answer: this script does not know what sizes that series has.
controller_file t2d-standard-4
STUB_NO_LIST=1 run --check
expect_says "an unknown series recommends nothing" 'No size in the t2d-standard family'

# Machine types are zonal, so a controller pool JSON without a zone cannot be answered at all.
controller_file e2-standard-8 ""
run --check
expect_says "a controller pool with no zone is refused" 'machine types are zonal'
expect_exit 2 "and that is an input error, not a sizing verdict" run --check

# ---------------------------------------------------------------------------------------------------
# --overrides, and the requests
# ---------------------------------------------------------------------------------------------------

# The case this section exists for.  A requests figure generated for a 1121-node account sat in the sibling's
# 2-platform overrides against a controller whose allocatable is 7910m: the pod could never be scheduled, and
# nothing there read that file or those requests.  e2-standard-8 allocates the same 7910m.
pools_file 20 150 306 4
controller_file e2-standard-8
deployment_file 8 20G 8G 5
overrides_file 13765m 28G 16G
run_merged --check
expect_says "requests above allocatable are refused by name" 'requests do not fit e2-standard-8'
expect_says "and the symptom is named rather than the cause guessed at" 'Pending'
expect_says "and the file to edit is named" 'jenkins-gke-overrides\.yaml'
expect_exit 1 "an unschedulable controller refuses" run_merged --check
# --warn-only covers a controller that will be slow. There is nothing to accept about one that never starts.
expect_exit 1 "and --warn-only does not cover it" run_merged --check --warn-only

# Requests inside allocatable say nothing about requests, and the sizing verdict is reached as before.
overrides_file 4000m 16G 8G
run_merged --check
expect_silent_about "requests inside allocatable are not mentioned" 'requests do not fit'
expect_says "and the sizing verdict is reached" 'controller holds this'

# The overrides file wins on the heap, which is the mismatch that hid the drift on the sibling: the shared
# file says 8G and the deployed cluster ran 16G, so the memory requirement was under-read by the difference.
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

# --exports-to writes the same lines the reader gets, so one run serves both a human and the Makefile.
run --check --exports-to "${work}/.gke-controller-fit"
if [ -f "${work}/.gke-controller-fit" ]; then
    # shellcheck disable=SC1090
    . "${work}/.gke-controller-fit"
    expect "the dot-file carries the fit verdict" true "${GKE_CONTROLLER_FITS}"
else
    echo "FAIL  --exports-to writes a file that can be sourced"
    failures=$((failures + 1))
fi

if [ "${failures}" -ne 0 ]; then
    echo "${failures} case(s) failed."
    exit 1
fi
echo "All cases behaved as recorded."
