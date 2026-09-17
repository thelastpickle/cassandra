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
# What ../spend-guard.py must keep doing, checked offline against stubbed clouds.
#
# The guard decides whether to stop CI, so the cases that matter most are the ones where it must brake without
# being over a cap: every way of not being able to see is a case here.  The sibling's four measured faults are
# each a case too, because all four were arithmetic that looked right:
#
#   a written-down sampling interval, wrong by five against an account publishing every 300s
#   a sample count read as an interval, wrong by about thirty
#   a fit with no slope declaring itself calibrated on an idle fortnight
#   a sanity bound taken around the configured guess, refusing a true measurement 40x away from it
#
# The first two cannot recur here, because Cloud Monitoring is asked to align and reduce, so no interval is
# derived.  What replaces them is the apportioning of an aligned hour across midnight, which is this file's
# first case: an alignment window is anchored on its end, not on midnight, and getting it wrong moves an hour of
# every day into the next one.  The daily cap is the shortest window, so it is where that shows first.
#
# Every call is answered by a PATH shim: gcloud, bq and curl.  curl is in that list because gcloud has no
# `monitoring time-series` surface in any release track, so the usage read and the metric write have no gcloud
# form; ../spend-guard.py's docstring says so at more length.  Nothing here needs a credential, a network, a
# cluster or a google library.

set -o pipefail

here="$(cd -- "$(dirname -- "$0")" && pwd)"
script="${here}/spend-guard.py"

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

# Resolved to a path once, here, and used as a path from then on.  Every case below runs the guard with the stub
# directory prepended to PATH, and an interpreter looked up through that same modified PATH is an interpreter the
# stubs can move: whichever python3 comes first on the caller's PATH is not necessarily the one this suite just
# checked for PyYAML, and the difference shows up as every case failing with one import error.
python3_path="$(command -v python3)"

failures=0

# ---------------------------------------------------------------------------------------------------
# The stubs
# ---------------------------------------------------------------------------------------------------

mkdir -p "${work}/bin"

# Every answer is derived from an environment variable set per case, so a case reads as the state of the cloud
# it describes rather than as a table somebody has to cross-reference.  Anything unrecognised exits 64: a new
# call must not pass this suite by being silently unanswered.
cat > "${work}/bin/gcloud" <<'STUB'
#!/bin/bash
secret=""
pool=""
maximum=""
previous=""
for argument in "$@"; do
    case "${previous}" in
        --secret) secret="${argument}" ;;
        --max-nodes) maximum="${argument}" ;;
    esac
    previous="${argument}"
done

case "$1 $2 $3" in
    "auth print-access-token ")
        echo "stub-access-token"
        exit 0
        ;;
    "secrets versions access")
        case "${secret}" in
            *caps)
                [ "x${STUB_CAPS_FAIL}" = "x" ] || { echo "PERMISSION_DENIED: stubbed" >&2; exit 1; }
                if [ -n "${STUB_CAPS}" ]; then printf '%s' "${STUB_CAPS}"; else cat "${DEFAULT_CAPS}"; fi
                exit 0
                ;;
            *state)
                [ "x${STUB_STATE_FAIL}" = "x" ] || { echo "NOT_FOUND: stubbed" >&2; exit 1; }
                if [ -n "${STUB_STATE}" ]; then printf '%s' "${STUB_STATE}"; else cat "${DEFAULT_STATE}"; fi
                exit 0
                ;;
        esac
        echo "stub gcloud: no canned answer for secret '${secret}'" >&2
        exit 64
        ;;
    "secrets versions add")
        # The written figures are kept so a case can assert what the guard chose to remember.
        cat >> "${STUB_STATE_WRITES:-/dev/null}"
        exit 0
        ;;
    "container node-pools list")
        [ "x${STUB_POOLS_FAIL}" = "x" ] || { echo "PERMISSION_DENIED: stubbed" >&2; exit 1; }
        if [ -n "${STUB_POOLS}" ]; then printf '%s' "${STUB_POOLS}"; else cat "${DEFAULT_POOLS}"; fi
        exit 0
        ;;
    "container node-pools update")
        pool="$4"
        echo "${pool} ${maximum}" >> "${STUB_UPDATES:-/dev/null}"
        case " ${STUB_BUSY_POOLS} " in
            *" ${pool} "*) echo "FAILED_PRECONDITION: an operation is already operating on the node pool" >&2; exit 1 ;;
        esac
        if [ "x${STUB_REFUSE_ZERO}" != "x" ] && [ "${maximum}" = "0" ]; then
            echo "INVALID_ARGUMENT: max node count must be greater than 0" >&2
            exit 1
        fi
        exit 0
        ;;
    "compute instances list")
        echo "${STUB_INSTANCES:-[]}"
        exit 0
        ;;
    "pubsub topics publish")
        echo "published" >> "${STUB_NOTIFICATIONS:-/dev/null}"
        exit 0
        ;;
esac

echo "stub gcloud: unexpected call: $*" >&2
exit 64
STUB

# The usage read and the metric write.  A GET whose URL names timeSeries is the read; a POST to the same path is
# the write, and it is recorded so a case can assert the heartbeat was published even on an unknown.
cat > "${work}/bin/curl" <<'STUB'
#!/bin/bash
url=""
method="GET"
previous=""
for argument in "$@"; do
    case "${argument}" in
        https://*) url="${argument}" ;;
    esac
    case "${previous}" in
        -X) method="${argument}" ;;
    esac
    previous="${argument}"
done

case "${method} ${url}" in
    "POST "*timeSeries*)
        cat >> "${STUB_METRICS:-/dev/null}"
        echo '{}'
        exit 0
        ;;
    "GET "*timeSeries*)
        [ "x${STUB_SERIES_FAIL}" = "x" ] || { echo "PERMISSION_DENIED: stubbed" >&2; exit 22; }
        if [ -n "${STUB_SERIES_FILE}" ] && [ -f "${STUB_SERIES_FILE}" ]; then
            cat "${STUB_SERIES_FILE}"
        else
            echo '{}'
        fi
        exit 0
        ;;
esac

echo "stub curl: unexpected ${method} ${url}" >&2
exit 64
STUB

cat > "${work}/bin/bq" <<'STUB'
#!/bin/bash
[ "x${STUB_BILL_FAIL}" = "x" ] || { echo "Access Denied: stubbed" >&2; exit 1; }
if [ -n "${STUB_BILL_FILE}" ] && [ -f "${STUB_BILL_FILE}" ]; then
    cat "${STUB_BILL_FILE}"
else
    echo '[]'
fi
exit 0
STUB

chmod +x "${work}/bin/gcloud" "${work}/bin/curl" "${work}/bin/bq"

# The state of the cloud a case does not override.  Files rather than inline defaults inside the stub, because a
# JSON object's braces inside a `${VAR:-...}` expansion do not survive the shell reliably, and a stub that
# answers with mangled JSON fails every case with an error about the guard.
printf '%s' '{"daily":300,"weekly":1500,"monthly":5000}' > "${work}/default-caps.json"
printf '%s' '{"version":1,"cost_as_of":null,"sample_interval":null,"cost_by_day":{},"pool_maxima":{}}' \
    > "${work}/default-state.json"
printf '%s' '[{"name":"agents-large-a","autoscaling":{"enabled":true,"maxNodeCount":215}}]' \
    > "${work}/default-pools.json"

# ---------------------------------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------------------------------

# An aligned hour, as Monitoring returns one: mean cores over a known window.  Written as a function so a case
# names hours and cores rather than pasting JSON.
series_file() {
    local path="${work}/series.json"
    {
        printf '{"timeSeries":[{"points":['
        local first=1
        while [ "$#" -gt 0 ]; do
            [ "${first}" -eq 1 ] || printf ','
            first=0
            printf '{"interval":{"startTime":"%s","endTime":"%s"},"value":{"doubleValue":%s}}' "$1" "$2" "$3"
            shift 3
        done
        printf ']}]}'
    } > "${path}"
    echo "${path}"
}

# A day's bill, as `bq --format=json` returns it.
bill_file() {
    local path="${work}/bill.json"
    {
        printf '['
        local first=1
        while [ "$#" -gt 0 ]; do
            [ "${first}" -eq 1 ] || printf ','
            first=0
            printf '{"day":"%s","total":"%s"}' "$1" "$2"
            shift 2
        done
        printf ']'
    } > "${path}"
    echo "${path}"
}

# ---------------------------------------------------------------------------------------------------
# Running it
# ---------------------------------------------------------------------------------------------------

# `python3 "${script}"` and not `"${script}"`: the shebang's `env python3` can resolve to a different
# interpreter from the PATH python3 checked above, and then every case gets one error page instead of a report.
run() {
    rm -f "${work}/updates" "${work}/metrics" "${work}/notifications" "${work}/state-writes"
    env PATH="${work}/bin:${PATH}" \
        GOOGLE_PROJECT=cassandra-ci \
        GOOGLE_REGION=us-central1 \
        GKE_LOCATION=us-central1 \
        GKE_CLUSTER_NAME=cassandra-jenkins \
        GKE_SPEND_CAPS_SECRET=cassandra-jenkins-spend-caps \
        GKE_SPEND_STATE_SECRET=cassandra-jenkins-spend-state \
        GKE_SPEND_AGENT_POOL_NAMES="${POOL_NAMES:-agents-large-a}" \
        GKE_SPEND_AGENT_POOL_MAXIMA="${POOL_MAXIMA}" \
        GKE_SPEND_ALERT_TOPIC=cassandra-jenkins-spend-alerts \
        GKE_SPEND_METRIC_PREFIX=custom.googleapis.com/cassandra_jenkins/spend_guard \
        GKE_SPEND_BILLING_TABLE="${BILLING_TABLE:-}" \
        GKE_SPEND_PRICE_PER_VCPU_HOUR="${PRICE:-0.04}" \
        GKE_SPEND_FIXED_USD_PER_DAY="${FIXED:-5}" \
        GKE_SPEND_COST_REFRESH_HOURS=6 \
        GKE_SPEND_GUARD_INTERVAL_MINUTES=5 \
        GKE_SPEND_NOW="${NOW:-2026-09-10T12:00:00Z}" \
        STUB_SERIES_FILE="${SERIES_FILE:-}" \
        STUB_BILL_FILE="${BILL_FILE:-}" \
        STUB_UPDATES="${work}/updates" \
        STUB_METRICS="${work}/metrics" \
        STUB_NOTIFICATIONS="${work}/notifications" \
        STUB_STATE_WRITES="${work}/state-writes" \
        DEFAULT_CAPS="${work}/default-caps.json" \
        DEFAULT_STATE="${work}/default-state.json" \
        DEFAULT_POOLS="${work}/default-pools.json" \
        "${python3_path}" "${script}" "$@" > "${work}/output" 2>&1
}

json() {
    "${python3_path}" -c "
import json, sys
document = json.load(open('${work}/output'))
value = document$1
print(value if not isinstance(value, bool) else str(value).lower())
" 2>/dev/null
}

expect() {
    local what="$1" want="$2" got="$3"
    if [ "${want}" = "${got}" ]; then
        echo "PASS  ${what} (${got})"
    else
        echo "FAIL  ${what}"
        echo "        want: ${want}"
        echo "        got:  ${got}"
        failures=$((failures + 1))
    fi
}

expect_exit() {
    local want="$1" what="$2"
    shift 2
    local actual=0
    "$@" || actual=$?
    if [ "${actual}" = "${want}" ]; then
        echo "PASS  ${what} (exit ${actual})"
    else
        echo "FAIL  ${what}: expected exit ${want}, got ${actual}"
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
# Cases: what ran, and when
# ---------------------------------------------------------------------------------------------------

# GAUGE intervals may contain identical timestamps. The requested alignment supplies the hour's duration.
SERIES_FILE="$(series_file \
    2026-09-10T00:30:00Z 2026-09-10T00:30:00Z 40 \
    2026-09-10T10:00:00Z 2026-09-10T10:00:00Z 80)"
NOW=2026-09-10T10:30:00Z STUB_INSTANCES='[{"name":"controller"}]' expect_exit 0 "aligned gauges are usable with running instances" run --report --days
expect_says "a gauge's aligned hour is split at midnight" '2026-09-09 +20\.0'
expect_says "equal gauge timestamps retain the CPU hours" '2026-09-10 +100\.0'
STUB_CAPS='{"daily":5}' run --enforce
expect "gauge usage over the cap brakes the pool" "agents-large-a 0" "$(cat "${work}/updates" 2>/dev/null)"

# An omitted startTime has the same meaning for a GAUGE.
printf '%s' '{"timeSeries":[{"points":[{"interval":{"endTime":"2026-09-10T10:00:00Z"},"value":{"int64Value":"8"}}]}]}' > "${work}/series.json"
run --report --days
expect_says "a gauge with no start time retains its aligned hour" '2026-09-10 +8\.0'

# The case this file exists for first.  Monitoring anchors an alignment window on its end, not on midnight, so
# an hour can straddle midnight and has to be shared between the two days by overlap.  40 cores from 23:30 to
# 00:30 is 20 vCPU-hours on each side, and 80 cores for the 09:00 hour is 80 on the later day.  Read without the
# apportioning, the whole 40 lands on one day and every daily figure is an hour out.
SERIES_FILE="$(series_file \
    2026-09-09T23:30:00Z 2026-09-10T00:30:00Z 40 \
    2026-09-10T09:00:00Z 2026-09-10T10:00:00Z 80)"
run --report --days
expect_says "an hour straddling midnight is split between the two days" '2026-09-09 +20\.0'
expect_says "and the rest of it lands on the later day" '2026-09-10 +100\.0'

# The three windows, from the same series.  Nine settled days at the standing charge, plus the two days above:
# daily is 100 vCPU-hours at $0.04 and half a day of the $5, weekly reaches back to Monday the 7th, monthly to
# the 1st.  Asserted through --json, because these are the figures the caps are compared against.
run --report --json
expect "the daily window is the load plus the part-day standing charge" "6.5" "$(json '["windows"][0]["spend"]')"
expect "the weekly window reaches back to Monday" "2026-09-07" "$(json '["windows"][1]["start"]' | cut -dT -f1)"
expect "and sums the standing charge over its days" "22.3" "$(json '["windows"][1]["spend"]')"
expect "the monthly window reaches back to the first" "2026-09-01" "$(json '["windows"][2]["start"]' | cut -dT -f1)"

# ---------------------------------------------------------------------------------------------------
# Cases: the caps, and the brake
# ---------------------------------------------------------------------------------------------------

# Under every cap, so nothing is touched and nothing is said about a brake.
run --enforce
expect "under every cap the pools are left alone" "" "$(cat "${work}/updates" 2>/dev/null)"
run --enforce; status=$?
expect "and it exits 0" "0" "${status}"
expect_says "and says the brake is off" 'brake: off'

# Over the daily cap.  $6.50 against a $2 cap, so the pool's maximum goes to zero and nothing else does.
STUB_CAPS='{"daily":2,"weekly":1500,"monthly":5000}' run --enforce
expect "over a cap the pool's maximum is set to zero" "agents-large-a 0" "$(cat "${work}/updates")"
expect_says "and the window is named as over" 'OVER'
STUB_CAPS='{"daily":2,"weekly":1500,"monthly":5000}' run --enforce; status=$?
expect "and it exits 1" "1" "${status}"

# A report never changes anything, whatever it finds.  This is the whole difference between `make spend` and the
# deployed guard, and it is the one property somebody debugging a stopped cluster relies on.
STUB_CAPS='{"daily":2,"weekly":1500,"monthly":5000}' run --report
expect "a report over a cap changes no pool" "" "$(cat "${work}/updates" 2>/dev/null)"
STUB_CAPS='{"daily":2,"weekly":1500,"monthly":5000}' run --report; status=$?
expect "and still exits 1, because a cap is met" "1" "${status}"

# Releasing.  A pool found at zero, with spend under the caps, goes back to the maximum the kept figures hold.
STUB_STATE='{"version":1,"cost_as_of":null,"cost_by_day":{},"pool_maxima":{"agents-large-a":215}}' \
    STUB_POOLS='[{"name":"agents-large-a","autoscaling":{"enabled":true,"maxNodeCount":0}}]' \
    run --enforce
expect "a braked pool under the caps is restored from the kept maximum" "agents-large-a 215" "$(cat "${work}/updates")"

# The kept figures are primary, and the environment is the fallback.  A cluster whose guard has never recorded a
# maximum still releases, from what layer 1 put in the environment.
STUB_STATE='{"version":1,"cost_as_of":null,"cost_by_day":{},"pool_maxima":{}}' \
    STUB_POOLS='[{"name":"agents-large-a","autoscaling":{"enabled":true,"maxNodeCount":0}}]' \
    POOL_MAXIMA='{"agents-large-a":190}' \
    run --enforce
expect "with nothing kept, the maximum comes from the environment" "agents-large-a 190" "$(cat "${work}/updates")"

# ---------------------------------------------------------------------------------------------------
# Cases: reading the brake off the pools
# ---------------------------------------------------------------------------------------------------

# Small and report pools can have a normal maximum of one. That is not evidence of a brake.
STUB_POOLS='[{"name":"agents-large-a","autoscaling":{"enabled":true,"maxNodeCount":1}}]' \
    POOL_MAXIMA='{"agents-large-a":1}' expect_exit 0 "a configured one-node pool is running" run --report
expect_says "a configured one-node pool reports the brake off" 'brake: off'
STUB_POOLS='[{"name":"agents-large-a","autoscaling":{"enabled":true,"maxNodeCount":1}}]' \
    POOL_MAXIMA='{"agents-large-a":1}' run --enforce
expect "a healthy one-node pool needs no release" "" "$(cat "${work}/updates" 2>/dev/null)"
STUB_POOLS='[{"name":"agents-large-a","autoscaling":{"enabled":true,"maxNodeCount":1}}]' \
    POOL_MAXIMA='{"agents-large-a":1}' STUB_CAPS='{"daily":2}' run --enforce
expect "over a cap a one-node pool still needs braking" "agents-large-a 0" "$(cat "${work}/updates" 2>/dev/null)"
STUB_POOLS='[{"name":"agents-large-a","autoscaling":{"enabled":true,"maxNodeCount":1}}]' \
    POOL_MAXIMA='{"agents-large-a":215}' run --report
expect_says "a larger pool reduced to one reports the brake" 'brake: STOPPED'

# proto3 omits a zero-valued field, so a braked pool comes back with no maximum at all.  Read as "no maximum
# means no ceiling", a stopped cluster would report itself as running.
STUB_POOLS='[{"name":"agents-large-a","autoscaling":{"enabled":true}}]' run --report
expect_says "a pool whose maximum is absent reads as stopped" 'brake: STOPPED'

# The other half of the same rule.  A pool configured with total limits carries no per-zone maximum either, and
# it is running hundreds of nodes; reading only maxNodeCount would call it stopped.
STUB_POOLS='[{"name":"agents-large-a","autoscaling":{"enabled":true,"totalMaxNodeCount":215}}]' run --report
expect_says "a pool with only a total maximum reads as running" 'brake: off'

# Part braked is its own state, and it is the one that needs saying: a cluster where some pools can create nodes
# and some cannot looks like an autoscaler fault from inside.
POOL_NAMES="agents-large-a agents-large-b" \
    POOL_MAXIMA='{"agents-large-a":215,"agents-large-b":215}' \
    STUB_POOLS='[{"name":"agents-large-a","autoscaling":{"enabled":true,"maxNodeCount":0}},{"name":"agents-large-b","autoscaling":{"enabled":true,"maxNodeCount":215}}]' \
    run --report
expect_says "some pools stopped and some not is reported as part stopped" 'brake: PART STOPPED'

# ---------------------------------------------------------------------------------------------------
# Cases: the four unknowns, each of which brakes
# ---------------------------------------------------------------------------------------------------

# Nothing to compare against.
STUB_CAPS_FAIL=1 run --enforce
expect_says "unreadable caps are unknown" 'UNKNOWN: the caps could not be read'
expect_says "unreadable caps are shown as unknown in the table" 'daily .*unknown'
expect "and unknown brakes the pools" "agents-large-a 0" "$(cat "${work}/updates")"
STUB_CAPS_FAIL=1 run --enforce; status=$?
expect "and exits 2, not 1" "2" "${status}"

# Cannot see what ran.
STUB_SERIES_FAIL=1 run --enforce
expect_says "a failed usage read is unknown" 'UNKNOWN: Cloud Monitoring would not report'
expect "and brakes" "agents-large-a 0" "$(cat "${work}/updates")"

# The discriminator.  An empty metric with instances running is a guard that cannot see; an empty metric with
# nothing running is a spend of zero.  Same empty answer, opposite conclusions, which is why Compute is asked.
SERIES_FILE="" STUB_INSTANCES='[{"name":"gke-node-1"}]' run --enforce
expect_says "an empty metric with instances running is unknown" 'UNKNOWN: instances are running'
expect "and brakes" "agents-large-a 0" "$(cat "${work}/updates")"

SERIES_FILE="" STUB_INSTANCES='[]' run --enforce
expect_silent_about "an empty metric with nothing running is not unknown" 'UNKNOWN'
expect "and does not brake" "" "$(cat "${work}/updates" 2>/dev/null)"
expect_says "and says why the figure is only the standing charge" 'nothing is running'

# Cannot read the pools, so the brake's state is unknown and the safe reading is that it is off.
STUB_POOLS_FAIL=1 run --enforce
expect_says "unreadable pools are unknown" 'UNKNOWN: the agent node pools could not be read'
STUB_POOLS_FAIL=1 run --enforce; status=$?
expect "and exit 2" "2" "${status}"

# ---------------------------------------------------------------------------------------------------
# Cases: the two GKE hazards
# ---------------------------------------------------------------------------------------------------

# A pool with an operation in flight rejects a second one, and GKE serialises them per pool.  Nothing is wrong;
# the next evaluation converges.  What must not happen is the busy pool aborting the loop and leaving the rest
# of the pools able to create nodes.
POOL_NAMES="agents-large-a agents-large-b" \
    POOL_MAXIMA='{"agents-large-a":215,"agents-large-b":215}' \
    STUB_POOLS='[{"name":"agents-large-a","autoscaling":{"enabled":true,"maxNodeCount":215}},{"name":"agents-large-b","autoscaling":{"enabled":true,"maxNodeCount":215}}]' \
    STUB_CAPS='{"daily":2,"weekly":1500,"monthly":5000}' \
    STUB_BUSY_POOLS="agents-large-a" \
    run --enforce
expect "a busy pool does not stop the others being braked" "agents-large-a 0
agents-large-b 0" "$(cat "${work}/updates")"

# Whether GKE accepts a maximum of zero is unverified: the API reference states no floor of one, and gcloud
# passes a zero through without objecting.  If the server refuses it, one node per pool is the closest thing to
# stopped, and the report has to say so rather than claim the cluster is stopped.
STUB_CAPS='{"daily":2,"weekly":1500,"monthly":5000}' STUB_REFUSE_ZERO=1 run --enforce
expect "a refused zero is retried at one" "agents-large-a 0
agents-large-a 1" "$(cat "${work}/updates")"
expect_says "and the report says the pools are held at one node" 'held at one node'
expect_says "and calls the brake stopping, because a node pool update is asynchronous" 'brake: STOPPING'

# ---------------------------------------------------------------------------------------------------
# Cases: the price, fitted or configured
# ---------------------------------------------------------------------------------------------------

# No billing table is the default, and it is the largest difference from the sibling.  The caps are then compared
# against a figure somebody typed, and a report that did not say so would read as a measurement.
run --report
expect_says "with no billing table the figures are configured, not fitted" 'configured, not fitted'
expect_says "and the report says why there is no bill" 'GKE_SPEND_BILLING_TABLE is not set'
expect_says "and prints the rate to four places, so a small one does not read as zero" '\$0\.0400 a vCPU-hour'

# With a table and days that can identify a slope, the fit engages exactly as the sibling's does.  The usage has
# to span at least 500 vCPU-hours and vary by at least 15%, which is why these days differ widely: an idle
# fortnight is the case the sibling's fourth fault was found on.
SERIES_FILE="$(series_file \
    2026-09-05T00:00:00Z 2026-09-05T01:00:00Z 100 \
    2026-09-06T00:00:00Z 2026-09-06T01:00:00Z 900 \
    2026-09-07T00:00:00Z 2026-09-07T01:00:00Z 400 \
    2026-09-08T00:00:00Z 2026-09-08T01:00:00Z 1200)"
BILL_FILE="$(bill_file 2026-09-05 9.0 2026-09-06 41.0 2026-09-07 21.0 2026-09-08 53.0)"
BILLING_TABLE="cassandra-ci.billing.gcp_billing_export_resource_v1_0123AB" run --report
expect_says "with a bill and days that vary, the price is fitted" 'fitted from the bill'
expect_says "and the residual is reported as an error margin" '\+/-'

# An idle fortnight cannot price a busy day, and OLS on it answers with a rate near zero, which prices a
# full-size build at nothing.  shared/spend_model.py refuses that, and the refusal has to be visible: the
# figures it did produce are what an operator sets the standing charge from.
SERIES_FILE="$(series_file \
    2026-09-05T00:00:00Z 2026-09-05T01:00:00Z 8 \
    2026-09-06T00:00:00Z 2026-09-06T01:00:00Z 9 \
    2026-09-07T00:00:00Z 2026-09-07T01:00:00Z 8 \
    2026-09-08T00:00:00Z 2026-09-08T01:00:00Z 9)"
BILL_FILE="$(bill_file 2026-09-05 3.2 2026-09-06 3.3 2026-09-07 3.2 2026-09-08 3.3)"
BILLING_TABLE="cassandra-ci.billing.gcp_billing_export_resource_v1_0123AB" run --report
expect_says "an idle fortnight refuses the fit" 'the fit was refused'
expect_says "and still prints what those days did measure" 'what the days did give'

# A bill that contradicts the configured guess by a factor is what the sibling braked a cluster on, reading $353
# against a budget showing $10.59.  The rescue scales the configured figures to the bill rather than trusting
# them, and says it did.
SERIES_FILE="$(series_file \
    2026-09-05T00:00:00Z 2026-09-05T01:00:00Z 8 \
    2026-09-06T00:00:00Z 2026-09-06T01:00:00Z 9 \
    2026-09-07T00:00:00Z 2026-09-07T01:00:00Z 8 \
    2026-09-08T00:00:00Z 2026-09-08T01:00:00Z 9)"
BILL_FILE="$(bill_file 2026-09-05 0.4 2026-09-06 0.4 2026-09-07 0.4 2026-09-08 0.4)"
BILLING_TABLE="cassandra-ci.billing.gcp_billing_export_resource_v1_0123AB" FIXED=40 run --report
expect_says "a configured figure the bill contradicts is scaled to it" 'scaled to the bill'

# A bill that cannot be read is a warning and never an unknown, and this is the distinction that keeps a
# billing-export outage from stopping CI: every day can still be estimated, so what is lost is the fit.
BILLING_TABLE="cassandra-ci.billing.gcp_billing_export_resource_v1_0123AB" STUB_BILL_FAIL=1 run --enforce
expect_says "an unreadable bill is a warning" 'WARNING: the billing export could not be read'
expect_silent_about "and is not unknown" 'UNKNOWN'
expect "and does not brake" "" "$(cat "${work}/updates" 2>/dev/null)"

# ---------------------------------------------------------------------------------------------------
# Cases: the heartbeat, and what the state keeps
# ---------------------------------------------------------------------------------------------------

# The alert policy fires on this metric's absence, so absence has to mean the guard is not running.  A guard that
# stopped writing it when unhappy would make the alarm say the same thing whether it was broken or worried.
run --enforce
if grep -q 'evaluation_ok' "${work}/metrics" 2>/dev/null; then
    echo "PASS  an ordinary evaluation writes the heartbeat"
else
    echo "FAIL  an ordinary evaluation writes the heartbeat"
    sed 's/^/        /' "${work}/metrics" 2>/dev/null
    failures=$((failures + 1))
fi

STUB_CAPS_FAIL=1 run --enforce
if grep -q 'evaluation_ok' "${work}/metrics" 2>/dev/null; then
    echo "PASS  an evaluation that ends in unknown still writes the heartbeat"
else
    echo "FAIL  an evaluation that ends in unknown still writes the heartbeat"
    sed 's/^/        /' "${work}/metrics" 2>/dev/null
    failures=$((failures + 1))
fi

# A malformed or future-versioned state reads as empty and must not brake: braking on an evaluation with nothing
# kept would brake every new cluster on its first run.
STUB_STATE='{"version":99,"cost_by_day":{}}' run --enforce
expect_silent_about "a state from another version is not unknown" 'UNKNOWN'
expect "and does not brake" "" "$(cat "${work}/updates" 2>/dev/null)"

STUB_STATE='not json at all' run --enforce
expect_silent_about "an unparseable state is not unknown either" 'UNKNOWN'

# The maximum is recorded on every evaluation where a pool is not braked, so releasing uses a live figure rather
# than whatever was true the first time a brake went on.
STUB_POOLS='[{"name":"agents-large-a","autoscaling":{"enabled":true,"maxNodeCount":444}}]' run --enforce
if grep -q '"pool_maxima":{"agents-large-a":444}' "${work}/state-writes" 2>/dev/null; then
    echo "PASS  an unbraked pool's maximum is kept, so a release has a live figure"
else
    echo "FAIL  an unbraked pool's maximum is kept, so a release has a live figure"
    sed 's/^/        /' "${work}/state-writes" 2>/dev/null
    failures=$((failures + 1))
fi

# ---------------------------------------------------------------------------------------------------
# Cases: how it is deployed
# ---------------------------------------------------------------------------------------------------

# The sibling's live bug, as a test.  eks/1-cluster/spend-guard.tf packages only its script, so spend_model is
# not in the zip, the import fails at module load, and every scheduled evaluation dies before reading a cap.
# 1-cluster/spend-guard.tf here packages the two flat together, so the import has to find spend_model beside the
# script as well as at ../shared.
flat="${work}/flat"
mkdir -p "${flat}"
cp "${script}" "${flat}/main.py"
cp "${here}/../shared/spend_model.py" "${flat}/spend_model.py"
if (cd "${flat}" && "${python3_path}" -c 'import main' 2>"${work}/output"); then
    echo "PASS  the guard imports with spend_model flat beside it, as the function is packaged"
else
    echo "FAIL  the guard imports with spend_model flat beside it, as the function is packaged"
    sed 's/^/        /' "${work}/output"
    failures=$((failures + 1))
fi

# The Cloud Function's entry point is named in 1-cluster/spend-guard.tf, so a rename here is a deploy that
# builds and then fails on every invocation with nothing in the configuration to point at.
if (cd "${flat}" && "${python3_path}" -c 'import main; assert callable(main.guard)' 2>"${work}/output"); then
    echo "PASS  guard() exists, which is the entry point layer 1 deploys"
else
    echo "FAIL  guard() exists, which is the entry point layer 1 deploys"
    sed 's/^/        /' "${work}/output"
    failures=$((failures + 1))
fi

# An environment that does not describe a cluster with a cap is a misconfiguration, not an unknown spend, and it
# exits 2 with a sentence rather than a traceback: `make spend` is where this is seen, and a traceback there
# reads as a fault in the guard.
env PATH="${work}/bin:${PATH}" GOOGLE_PROJECT= "${python3_path}" "${script}" --report > "${work}/output" 2>&1
status=$?
expect "an environment with no cluster in it exits 2" "2" "${status}"
expect_says "and names what is missing" 'GOOGLE_PROJECT'

# ---------------------------------------------------------------------------------------------------

if [ "${failures}" -ne 0 ]; then
    echo "${failures} case(s) failed."
    exit 1
fi
echo "All cases behaved as recorded."
