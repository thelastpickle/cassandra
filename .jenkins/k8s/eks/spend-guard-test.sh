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
# Regression test for spend-guard.py.  Needs no cluster, no credentials and no money: it puts a stub `aws`
# on the PATH answering the eight operations the script makes, and sets $EKS_SPEND_NOW so that a month
# boundary and a Monday are reachable on any day of the year.
#
#   ./spend-guard-test.sh
#
# Two classes of case, and the second is the reason this file exists.  The first is arithmetic: the windows,
# the fit, and taking the larger of the billed and the estimated figure for each day.  The second is what
# happens when spend cannot be read, because a spend guard that fails quietly is worse than none: it reports
# a cluster as watched while nothing watches it.  Every unknown here has to brake and to say why.
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

# Answers from files the cases write, and records what it was asked to change.  It reads the request out of
# --cli-input-json with python3, rather than by grepping the JSON, so a change to how the script shapes a
# call fails a case instead of silently matching the wrong branch.
mkdir -p "${work}/bin"
cat > "${work}/bin/aws" <<'STUB'
#!/bin/bash
# Stub. Answers ssm, cloudwatch, ce, ec2, autoscaling and sns, and nothing else.
set -o nounset
service="$1"; shift
operation="$1"; shift

input=''
name=''
while [ $# -gt 0 ]; do
    case "$1" in
        --cli-input-json) input="$2"; shift 2 ;;
        *) shift ;;
    esac
done

field() { python3 -c 'import json,sys
print(json.loads(sys.argv[1]).get(sys.argv[2], ""))' "${input}" "$1"; }

case "${service} ${operation}" in
    'ssm get-parameter')
        name="$(field Name)"
        case "${name}" in
            */caps)
                if [ -z "${STUB_CAPS:-}" ]; then
                    echo "stub aws: ParameterNotFound: ${name}" >&2; exit 255
                fi
                printf '{"Parameter":{"Name":"%s","Value":%s}}' "${name}" \
                    "$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "${STUB_CAPS}")"
                ;;
            */state)
                if [ -z "${STUB_STATE:-}" ]; then
                    echo "stub aws: ParameterNotFound: ${name}" >&2; exit 255
                fi
                printf '{"Parameter":{"Name":"%s","Value":%s}}' "${name}" \
                    "$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "${STUB_STATE}")"
                ;;
            *) echo "stub aws: no canned parameter '${name}'" >&2; exit 255 ;;
        esac
        exit 0 ;;

    'ssm put-parameter')
        field Value >> "${STUB_RECORD}/state-writes"
        echo '{"Version":1}'
        exit 0 ;;

    'cloudwatch get-metric-data')
        if [ "${STUB_CLOUDWATCH_FAILS:-0}" = 1 ]; then
            echo "stub aws: ThrottlingException: Rate exceeded" >&2; exit 255
        fi
        # Two shapes, told apart by the period the script asks for, as CloudWatch itself would.
        #
        # Period 60 is the interval probe: datapoints spaced 3600/$STUB_SAMPLES apart, which is what the
        # script measures the publication interval from.
        #
        # Period 3600 is the daily read: one datapoint a day, at midnight of each day in $STUB_VCPU, with Sum
        # the day's vCPU-hours times $STUB_SAMPLES, so that Sum times the interval over 3600 is the figure the
        # fixture names whatever $STUB_SAMPLES is.  SampleCount is answered too, and deliberately not what the
        # arithmetic uses: an earlier version derived the interval from it and read thirty times the bill.
        #
        # $STUB_SAMPLES=0 is a metric with no datapoints at all.
        python3 - "${STUB_VCPU:-/dev/null}" "${STUB_SAMPLES:-60}" "${input}" <<'INNER'
import json, sys
from datetime import datetime, timedelta, timezone

path, samples, request = sys.argv[1], float(sys.argv[2]), json.loads(sys.argv[3])
period = request["MetricDataQueries"][0]["MetricStat"]["Period"]

stamps, sums = [], []
if samples > 0 and period == 60:
    gap = 3600.0 / samples
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for index in range(3):
        stamps.append((base + timedelta(seconds=gap * index)).isoformat())
        sums.append(480.0)
elif samples > 0:
    with open(path) as lines:
        for line in lines:
            if not line.strip() or line.startswith("#"):
                continue
            day, hours = line.split()
            stamps.append(f"{day}T00:00:00+00:00")
            sums.append(float(hours) * samples)

results = []
for query in request["MetricDataQueries"]:
    stat = query["MetricStat"]["Stat"]
    values = sums if stat == "Sum" else [samples] * len(sums)
    results.append({"Id": query["Id"], "Timestamps": stamps, "Values": values})
print(json.dumps({"MetricDataResults": results}))
INNER
        exit 0 ;;

    'cloudwatch put-metric-data')
        printf '%s\n' "${input}" >> "${STUB_RECORD}/metrics"
        exit 0 ;;

    'ce get-cost-and-usage')
        if [ "${STUB_CE_FAILS:-0}" = 1 ]; then
            echo "stub aws: AccessDeniedException: no ce:GetCostAndUsage" >&2; exit 255
        fi
        printf '%s\n' "${input}" >> "${STUB_RECORD}/ce-calls"
        python3 - "${STUB_CE:-/dev/null}" <<'PY'
import json, sys
from datetime import date, timedelta
rows = []
with open(sys.argv[1]) as lines:
    for line in lines:
        if not line.strip() or line.startswith("#"):
            continue
        day, cost = line.split()
        end = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
        rows.append({"TimePeriod": {"Start": day, "End": end},
                     "Total": {"UnblendedCost": {"Amount": cost, "Unit": "USD"}}})
print(json.dumps({"ResultsByTime": rows}))
PY
        exit 0 ;;

    'ec2 describe-instance-status')
        if [ "${STUB_RUNNING:-0}" = 1 ]; then
            echo '{"InstanceStatuses":[{"InstanceId":"i-0000000000000dead"}]}'
        else
            echo '{"InstanceStatuses":[]}'
        fi
        exit 0 ;;

    'autoscaling describe-auto-scaling-groups')
        python3 - "${STUB_SUSPENDED:-}" <<PY
import json, sys
suspended = set(sys.argv[1].split())
names = json.loads('''${input}''')["AutoScalingGroupNames"]
groups = []
for name in names:
    processes = ([{"ProcessName": p} for p in ("Launch", "AZRebalance")]
                 if name in suspended else [])
    groups.append({"AutoScalingGroupName": name, "SuspendedProcesses": processes})
print(json.dumps({"AutoScalingGroups": groups}))
PY
        exit 0 ;;

    'autoscaling suspend-processes'|'autoscaling resume-processes')
        echo "${operation} $(field AutoScalingGroupName)" >> "${STUB_RECORD}/actions"
        exit 0 ;;

    'lambda get-function-configuration')
        if [ -z "${STUB_DEPLOYED:-}" ]; then
            echo "stub aws: ResourceNotFoundException" >&2; exit 255
        fi
        printf '{"FunctionName":"guard","LastModified":"%s"}' "${STUB_DEPLOYED}"
        exit 0 ;;

    'sns publish')
        field Subject >> "${STUB_RECORD}/notifications"
        exit 0 ;;

    *) echo "stub aws: no canned answer for '${service} ${operation}'" >&2; exit 255 ;;
esac
STUB
chmod +x "${work}/bin/aws"
export PATH="${work}/bin:${PATH}"

# ---------------------------------------------------------------------------------------------------
# The environment layer 1 would set
# ---------------------------------------------------------------------------------------------------

export EKS_CLUSTER_NAME='mck--cassandra-jenkins'
export AWS_REGION='us-west-2'
export EKS_SPEND_CAPS_PARAMETER='/mck--cassandra-jenkins/spend-guard/caps'
export EKS_SPEND_STATE_PARAMETER='/mck--cassandra-jenkins/spend-guard/state'
export EKS_SPEND_AGENT_ASG_NAMES='eks-agents-large-a-abc eks-agents-large-b-def'
export EKS_SPEND_ALERT_TOPIC_ARN='arn:aws:sns:us-west-2:000000000000:spend'
export EKS_SPEND_METRIC_NAMESPACE='CassandraJenkins/SpendGuard'
export EKS_SPEND_COST_METRIC='UnblendedCost'
export EKS_SPEND_PRICE_PER_VCPU_HOUR='0.06'
export EKS_SPEND_FIXED_USD_PER_DAY='5'
export EKS_SPEND_CE_REFRESH_HOURS='6'

# 2026-09-09 is a Wednesday, so the weekly window began on Monday the 7th and the monthly on the 1st.
# Midday, so that today is half elapsed and the fixed term is pro-rated rather than whole.
export EKS_SPEND_NOW='2026-09-09T12:00:00+00:00'

export STUB_RECORD="${work}"

# ---------------------------------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------------------------------

# Six settled days of differing load, then the weekend idle, then Monday and Tuesday busy, then today.
# The costs are exactly $4 a day fixed plus $0.05 a vCPU-hour, so the fit has one right answer and any
# arithmetic error in it shows as a price that is not five cents.
cat > "${work}/vcpu" <<'EOF'
2026-08-26 1500
2026-08-27 1000
2026-08-28 2000
2026-08-29 0
2026-08-30 0
2026-08-31 3000
2026-09-01 500
2026-09-02 4000
2026-09-03 100
2026-09-04 2500
2026-09-05 0
2026-09-06 0
2026-09-07 3000
2026-09-08 2000
2026-09-09 1000
EOF

python3 - "${work}/vcpu" > "${work}/ce" <<'PY'
import sys
# The generator of the fixture above, not a second copy of it: $4 a day and 5c a vCPU-hour.  Today is
# left out, because Cost Explorer has no figure for a day that has not finished.
for line in open(sys.argv[1]):
    day, hours = line.split()
    if day == "2026-09-09":
        continue
    print(day, f"{4.0 + 0.05 * float(hours):.4f}")
PY

export STUB_VCPU="${work}/vcpu"
export STUB_CE="${work}/ce"
export STUB_SAMPLES=60
export STUB_RUNNING=1
export STUB_SUSPENDED=''
export STUB_STATE=''
export STUB_CAPS='{"daily": 500, "weekly": 1500, "monthly": 4000}'

# ---------------------------------------------------------------------------------------------------
# Running one case
# ---------------------------------------------------------------------------------------------------

failures=0

# One run.  Its output goes to $work/output whatever the exit status, and the recorded actions are cleared
# first so that a case asserts on what it caused rather than on what a previous case left.
run() {
    rm -f "${work}/actions" "${work}/notifications" "${work}/metrics" \
          "${work}/ce-calls" "${work}/state-writes"
    : > "${work}/actions"; : > "${work}/notifications"; : > "${work}/metrics"
    : > "${work}/ce-calls"; : > "${work}/state-writes"
    status=0
    python3 "${here}/spend-guard.py" "$@" > "${work}/output" 2>&1 || status=$?
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

expect_output() {
    local description="$1" pattern="$2"
    if grep -qE "${pattern}" "${work}/output"; then
        echo "PASS  ${description}"
    else
        echo "FAIL  ${description}: no line matching /${pattern}/"
        sed 's/^/        /' "${work}/output"
        failures=$((failures + 1))
    fi
}

expect_no_output() {
    local description="$1" pattern="$2"
    if grep -qE "${pattern}" "${work}/output"; then
        echo "FAIL  ${description}: a line matches /${pattern}/"
        sed 's/^/        /' "${work}/output"
        failures=$((failures + 1))
    else
        echo "PASS  ${description}"
    fi
}

json() { python3 -c 'import json,sys; print(eval("d"+sys.argv[1], {"d": json.load(open(sys.argv[2]))}))' \
             "$1" "${work}/output"; }

# How many lines of $work/<file> match, `|| true` because `grep -c` exits 1 on a count of zero and zero is
# what several cases assert.
count() { grep -c "${2:-.}" "${work}/$1" || true; }

# ---------------------------------------------------------------------------------------------------
# The windows, and the fit
# ---------------------------------------------------------------------------------------------------

# The three windows begin where UTC says, not where the local clock does.
run --report --json
expect "the daily window began at midnight" "2026-09-09T00:00:00+00:00" "$(json '["windows"][0]["since"]')"
expect "the weekly window began on Monday"  "2026-09-07T00:00:00+00:00" "$(json '["windows"][1]["since"]')"
expect "the monthly window began on the 1st" "2026-09-01T00:00:00+00:00" "$(json '["windows"][2]["since"]')"

# The fit has one right answer, and it is the fixture's own generator: $0.05 a vCPU-hour and $4 a day.
expect "the price is fitted, not configured" "calibrated" "$(json '["model"]["mode"]')"

# Today has no Cost Explorer figure, so it is estimated: 1000 vCPU-hours at the fitted price, plus half of
# the fitted fixed cost, because it is midday.  $50 + $2 = $52.
expect "today is estimated at 1000 vCPU-hours" "52.0" "$(json '["windows"][0]["spend"]')"

# The week is Monday and Tuesday billed, plus today estimated.  $154 + $104 + $52.
expect "the week sums its days" "310.0" "$(json '["windows"][1]["spend"]')"

# Nothing is over its cap, and nothing is braked, so this exits 0.
expect "under every cap exits 0" 0 "${status}"

# Every total is a sum of the day rows, and --days prints them.  The two terms are split, so a day that
# reads high is attributable to the load or to the standing-still figure without any arithmetic by hand.
run --report --days
expect_output "the day table names every day of the widest window" '2026-09-01 +500 +\$25\.00 +\$4\.00 +\$29\.00'
expect_output "and today has no billed figure to take" '2026-09-09 +1,000 +\$50\.00 +\$2\.00 +\$52\.00 +- +no +estimated'
expect_output "and says which figure counts when" 'A settled day takes the bill'

# The same run as a table, which is what `make spend` and every SNS message carry.
run --report
expect_output "the fitted rate is five cents" '\$0\.0500 a vCPU-hour'
expect_output "the fitted fixed cost is four dollars" '\$4\.00 a day standing still'
expect_output "the residual is reported as the margin" '\+/- \$0\.00 a day'
expect_output "the pools are reported as running" 'Launch is not suspended on any'
expect_output "every window is a row" 'monthly +2026-09-01'

# ---------------------------------------------------------------------------------------------------
# The larger of the billed and the estimated figure wins
# ---------------------------------------------------------------------------------------------------

# A settled day whose bill is far above what its vCPU-hours explain, which is what a storage charge or a
# data transfer looks like.  The window has to follow the bill, not the estimate.
cp "${work}/ce" "${work}/ce-spike"
python3 - "${work}/ce-spike" <<'PY'
import sys
path = sys.argv[1]
lines = [line for line in open(path) if not line.startswith("2026-09-08")]
lines.append("2026-09-08 1500.0000\n")
open(path, "w").writelines(sorted(lines))
PY
STUB_CE="${work}/ce-spike" STUB_STATE='' run --report --json
expect "a day billed above its estimate is taken as billed" "1706.0" "$(json '["windows"][1]["spend"]')"
expect "which puts the week over its cap" "True" "$(json '["windows"][1]["over"]')"
expect "and a report over a cap exits 1" 1 "${status}"

# A settled day takes the bill even when the estimate is larger, and this is the rule that keeps a model
# error out of the past.  Taking the larger everywhere is how a monthly figure came to read thirty times the
# bill: every day of the month carried the error, not just the day no bill covers yet.
python3 - "${work}/ce" > "${work}/ce-cheap" <<'PY'
import sys
for line in open(sys.argv[1]):
    day, cost = line.split()
    print(day, "1.0000" if day == "2026-09-04" else cost)
PY
STUB_CE="${work}/ce-cheap" run --report --json
expect "a settled day takes the bill" "billed" "$(json '["days"][3]["taken"]')"
expect "which is the bill and not the estimate" "1.0" "$(json '["days"][3]["billed"]')"
expect "even though the estimate is far above it" "True" \
    "$(json '["days"][3]["estimated"] > 100')"
STUB_CE="${work}/ce-cheap" run --report --days
expect_output "and the table says which days are settled" '2026-09-04 .* yes +billed'

# The model against the bill, over the days where both figures exist.  Nothing was checking that, and both
# of this file's factor-of-N faults would have shown here on the first evaluation.  A configured $15 a day
# against a bill of about $3.30 is a ratio of five, so it is reported; it is a warning and not a brake,
# because a guard that ignored its cap on the grounds that it distrusted its own figure would be worse.
python3 - > "${work}/vcpu-quiet" <<'PY'
for day in range(20, 32):
    print(f"2026-08-{day} {25 + day % 4}")
for day in range(1, 10):
    print(f"2026-09-{day:02d} {27 + day % 3}")
PY
python3 - > "${work}/ce-quiet" <<'PY'
for day in range(20, 32):
    print(f"2026-08-{day} 3.2{day % 7}")
for day in range(1, 9):
    print(f"2026-09-{day:02d} 3.3{day % 5}")
PY
EKS_SPEND_FIXED_USD_PER_DAY=15 STUB_VCPU="${work}/vcpu-quiet" STUB_CE="${work}/ce-quiet" run --report
expect_output "a configured figure the bill contradicts is scaled onto it" 'read [0-9.]+x the bill'
expect_output "and how it was scaled is stated" 'How: '
expect "and a scaled model does not brake" 0 "${status}"
expect_no_output "and nothing is left to warn about" 'WARNING: over'

# ---------------------------------------------------------------------------------------------------
# Braking, and releasing
# ---------------------------------------------------------------------------------------------------

# Over the daily cap: both groups are suspended, once each, and the reason is in the subject.
STUB_CAPS='{"daily": 10, "weekly": 1500, "monthly": 4000}' run --enforce
expect "over the daily cap suspends both groups" 2 "$(count actions '^suspend-processes ')"
expect "and suspends each group once" 2 "$(sort -u "${work}/actions" | grep -c '^suspend-processes ' || true)"
expect_output "the brake is reported by name" 'BRAKED: Launch is suspended on all 2 agent group'
if grep -q 'agent pools stopped, the daily cap is met' "${work}/notifications"; then
    echo "PASS  the notification names the cap that was met"
else
    echo "FAIL  the notification names the cap that was met"
    sed 's/^/        /' "${work}/notifications"
    failures=$((failures + 1))
fi
expect "and the run exits 1" 1 "${status}"

# Two caps met at once names both, and in one line: SNS truncates a subject at 100 characters, and an email
# client shows nothing else.
STUB_CAPS='{"daily": 10, "weekly": 20, "monthly": 4000}' run --enforce
if grep -q 'agent pools stopped, the daily and weekly caps are met' "${work}/notifications"; then
    echo "PASS  two caps met at once are both named"
else
    echo "FAIL  two caps met at once are both named"
    sed 's/^/        /' "${work}/notifications"
    failures=$((failures + 1))
fi

# Unknown spend brakes for a different reason, and saying a cap was met would send somebody to read a figure
# that was never established.
STUB_CLOUDWATCH_FAILS=1 run --enforce
if grep -q 'agent pools stopped, spend could not be established' "${work}/notifications"; then
    echo "PASS  unknown spend is not announced as a cap"
else
    echo "FAIL  unknown spend is not announced as a cap"
    sed 's/^/        /' "${work}/notifications"
    failures=$((failures + 1))
fi

# The same, with the brake already on: nothing is called and nobody is told again.  A guard that announces
# a state rather than a change sends an email every five minutes for as long as the cap holds.
STUB_CAPS='{"daily": 10, "weekly": 1500, "monthly": 4000}' \
    STUB_SUSPENDED="${EKS_SPEND_AGENT_ASG_NAMES}" run --enforce
expect "an already braked cluster is left alone" 0 "$(count actions)"
expect "and is not announced twice" 0 "$(count notifications)"

# Under every cap with the brake on: both groups are resumed and the release is announced.
STUB_SUSPENDED="${EKS_SPEND_AGENT_ASG_NAMES}" run --enforce
expect "under the caps resumes both groups" 2 "$(count actions '^resume-processes ')"
if grep -q 'agent pools released' "${work}/notifications"; then
    echo "PASS  the release is announced"
else
    echo "FAIL  the release is announced"
    failures=$((failures + 1))
fi

# One group suspended and one not is neither state, and it is what a call that failed half way leaves.
# The next evaluation has to make them uniform rather than read the pair as released.
STUB_SUSPENDED='eks-agents-large-a-abc' run --enforce
expect "a half braked pair is made uniform" 2 "$(count actions '^resume-processes ')"

# The brake on with nothing to justify it is the one state a reader cannot diagnose from the figures, because
# the figures look fine: the Lambda set it, and the Lambda runs whatever `make apply` last deployed.  The
# report has to say that rather than leave a correct-looking table to be puzzled over.
STUB_SUSPENDED="${EKS_SPEND_AGENT_ASG_NAMES}" \
    STUB_DEPLOYED='2020-01-01T00:00:00.000+0000' \
    EKS_SPEND_GUARD_FUNCTION='mck--cassandra-jenkins-spend-guard' run --report
expect_output "a brake nothing justifies is explained" 'Nothing above justifies that'
expect_output "and the Lambda is named as what set it" 'runs the copy of this file that'
expect_output "and a stale deploy is named as the cause" 'the deployed copy is the older one'
expect_output "and the command that fixes it" 'make apply'
expect "and the run still exits 1, being braked" 1 "${status}"

# The same with the deploy newer than this file: the difference is not the version, so the answer is the
# function's own log rather than another apply.
STUB_SUSPENDED="${EKS_SPEND_AGENT_ASG_NAMES}" \
    STUB_DEPLOYED='2099-01-01T00:00:00.000+0000' \
    EKS_SPEND_GUARD_FUNCTION='mck--cassandra-jenkins-spend-guard' run --report
expect_output "a current deploy sends the reader to the log" 'read its own log for what it decided'

# A cap left out is not a cap of zero.
STUB_CAPS='{"daily": null, "weekly": null, "monthly": 4000}' run --report --json
expect "a window with no cap is not over it" "False" "$(json '["windows"][0]["over"]')"
expect "and its cap is reported as absent" "None" "$(json '["windows"][0]["cap"]')"
expect "and the run exits 0" 0 "${status}"

# ---------------------------------------------------------------------------------------------------
# When spend cannot be read
# ---------------------------------------------------------------------------------------------------

# CloudWatch failing is unknown, and unknown brakes.  This is the case the whole design turns on: the
# alternative is a guard that reports a watched cluster while nothing watches it.
STUB_CLOUDWATCH_FAILS=1 run --enforce
expect "a CloudWatch failure brakes the pools" 2 "$(count actions '^suspend-processes ')"
expect_output "and says spend is unknown" 'UNKNOWN: CloudWatch would not answer'
expect "and exits 2, not 1" 2 "${status}"

# An empty metric with instances running is unknown too: the dimensions may have changed under us.
STUB_SAMPLES=0 STUB_RUNNING=1 run --enforce
expect "an empty metric with instances running brakes" 2 "$(count actions '^suspend-processes ')"
expect_output "and names the dimensions to confirm" 'AWS/Usage ResourceCount'

# An empty metric with nothing running is an idle account, which is a spend of zero and not a fault.
STUB_SAMPLES=0 STUB_RUNNING=0 run --enforce
expect "an empty metric with nothing running does not brake" 0 "$(count actions)"
expect_no_output "and is not reported as unknown" 'UNKNOWN'

# The interval the metric is published at must not change a single figure, and this is the case the account
# itself found: it publishes every 300s where an earlier version of this script divided by 60, which made
# every figure a fifth of the truth.  Twelve samples an hour and sixty both have to give the same $52 today.
STUB_SAMPLES=12 run --report --json
expect "a metric sampled every 300s gives the same figure" "52.0" "$(json '["windows"][0]["spend"]')"
expect "and is still fitted" "calibrated" "$(json '["model"]["mode"]')"
STUB_SAMPLES=12 run --report
expect_output "and the interval is reported, measured rather than assumed" 'sampled every 300s'
expect_no_output "and nothing is unknown about it" 'UNKNOWN'

# An interval AWS does not publish at is right rather than wrong, because it is measured; it is reported
# because every figure scales with it.
STUB_SAMPLES=6 run --report
expect_output "an unfamiliar interval is a warning" 'WARNING: the vCPU metric was sampled 6 times'
expect "and does not brake" 0 "${status}"

# No caps to compare against is unknown, and the parameter is named.
STUB_CAPS='' run --enforce
expect "unreadable caps brake the pools" 2 "$(count actions '^suspend-processes ')"
expect_output "and the caps parameter is named" 'the caps could not be read'

# Every cap null is the same fault as no parameter: nothing to compare against.
STUB_CAPS='{"daily": null, "weekly": null, "monthly": null}' run --enforce
expect "caps that cap nothing brake the pools" 2 "$(count actions '^suspend-processes ')"

# ---------------------------------------------------------------------------------------------------
# Cost Explorer is not required
# ---------------------------------------------------------------------------------------------------

# Cost Explorer failing is a warning, not an unknown: every day can be estimated from what ran, so the
# figure survives and what is lost is the fit.  The fallback price is 6c against the fitted 5c, so today
# reads $60 + $2.50 rather than $52.
STUB_CE_FAILS=1 run --report --json
expect "Cost Explorer failing does not brake"  "fallback" "$(json '["model"]["mode"]')"
expect "and today is estimated at the configured price" "62.5" "$(json '["windows"][0]["spend"]')"
expect_output "and it is reported rather than hidden" 'Cost Explorer would not answer'
expect "and the run still exits 0" 0 "${status}"

# Two settled days cannot be fitted, and the fallback says which of the two reasons applies.
python3 - > "${work}/ce-thin" <<'PY'
print("2026-09-07 154.0")
print("2026-09-08 104.0")
PY
STUB_CE="${work}/ce-thin" run --report --json
expect "two settled days are not enough to fit" "fallback" "$(json '["model"]["mode"]')"
expect_output "and the reason is the number of days" 'settled day\(s\) with a Cost Explorer figure'

# A fortnight of an idle cluster is the case a real account found on its first evaluation, and it is the
# dangerous one: days that differ by a few vCPU-hours are a wide relative spread over a tiny absolute one,
# so least squares answers with a price near zero and a price near zero estimates a full-size build at
# nothing at all.  The span test is what refuses it, and the figures it would have fitted are still reported.
python3 - > "${work}/vcpu-idle" <<'PY'
for day in range(20, 32):
    print(f"2026-08-{day} {25 + day % 4}")
for day in range(1, 10):
    print(f"2026-09-{day:02d} {27 + day % 3}")
PY
python3 - > "${work}/ce-idle" <<'PY'
for day in range(20, 32):
    print(f"2026-08-{day} 3.2{day % 7}")
for day in range(1, 9):
    print(f"2026-09-{day:02d} 3.3{day % 5}")
PY
STUB_VCPU="${work}/vcpu-idle" STUB_CE="${work}/ce-idle" run --report
expect_output "an idle fortnight is not fitted" 'Not fitted, and the configured figures read'
expect_output "and the reason is the span of load" 'a slope needs to mean anything'
expect_output "and the figures it would have fitted are still printed" 'would have fitted'
expect_output "and the configured figures are scaled onto the bill" 'so they are scaled onto it'

# Days spanning enough load, but too alike in it, cannot separate a fixed cost from a per-vCPU one.
python3 - > "${work}/vcpu-flat" <<'PY'
for day in range(20, 32):
    print(f"2026-08-{day} {2000 if day % 2 else 2600}")
for day in range(1, 10):
    print(f"2026-09-{day:02d} {2000 if day % 2 else 2600}")
PY
python3 - > "${work}/ce-flat" <<'PY'
for day in range(20, 32):
    print(f"2026-08-{day} {4.0 + 0.05 * (2000 if day % 2 else 2600):.4f}")
for day in range(1, 9):
    print(f"2026-09-{day:02d} {4.0 + 0.05 * (2000 if day % 2 else 2600):.4f}")
PY
STUB_VCPU="${work}/vcpu-flat" STUB_CE="${work}/ce-flat" run --report --json
expect "days too alike are not fitted" "fallback" "$(json '["model"]["mode"]')"
expect_output "and the reason is the spread" 'differ in load by'

# A fitted rate nowhere near the configured one is accepted, and this is the case a real account produced.
# Its bill was $0.0010 a vCPU-hour against a configured $0.06, over days spanning 384 to 22,599 vCPU-hours
# with a residual under a dollar a day, and an earlier version refused it for disagreeing with the guess:
# the fallback then read $353 for a day the bill put at $8.  A fit is the measurement and the configured
# figure is the guess, so the fit wins whenever the days can identify it.
python3 - "${work}/vcpu" > "${work}/ce-cheap-rate" <<'PY'
import sys
for line in open(sys.argv[1]):
    day, hours = line.split()
    if day == "2026-09-09":
        continue
    print(day, f"{3.27 + 0.001 * float(hours):.4f}")
PY
STUB_CE="${work}/ce-cheap-rate" run --report
expect_output "a rate far below the configured one is still fitted" 'Fitted over .* \$0\.0010 a vCPU-hour'
expect_output "and the standing-still figure with it" '\$3\.27 a day standing still'
expect_no_output "and the configured figures are not in force" 'configured figures are in force'

# The same in the other direction: ten times the configured rate is a fit and not a fault.
python3 - "${work}/vcpu" > "${work}/ce-dear" <<'PY'
import sys
for line in open(sys.argv[1]):
    day, hours = line.split()
    if day == "2026-09-09":
        continue
    print(day, f"{4.0 + 0.6 * float(hours):.4f}")
PY
STUB_CE="${work}/ce-dear" run --report
expect_output "a rate far above the configured one is still fitted" 'Fitted over .* \$0\.6000 a vCPU-hour'

# Only the absurd is refused.  Five dollars a vCPU-hour is a hundred times any list price, so it is
# arithmetic that has found something other than a bill.
python3 - "${work}/vcpu" > "${work}/ce-absurd" <<'PY'
import sys
for line in open(sys.argv[1]):
    day, hours = line.split()
    if day == "2026-09-09":
        continue
    print(day, f"{4.0 + 5.0 * float(hours):.4f}")
PY
STUB_CE="${work}/ce-absurd" run --report
expect_output "an absurd rate is refused" 'not a rate anything is billed at'
expect_output "and the configured figures are scaled onto the bill instead" 'so they are scaled onto it'

# ---------------------------------------------------------------------------------------------------
# A week that began in the previous month
# ---------------------------------------------------------------------------------------------------

# 2026-10-01 is a Thursday, so the weekly window began on Monday 2026-09-28 and the Cost Explorer range
# has to start there rather than at the month's first day.  Read from the range the script asked for.
python3 - > "${work}/vcpu-october" <<'PY'
for day in range(20, 31):
    print(f"2026-09-{day} 2000")
print("2026-10-01 1000")
PY
python3 - > "${work}/ce-october" <<'PY'
for day in range(20, 31):
    print(f"2026-09-{day} 104.0")
PY
EKS_SPEND_NOW='2026-10-01T12:00:00+00:00' STUB_VCPU="${work}/vcpu-october" \
    STUB_CE="${work}/ce-october" run --report --json
expect "a week can begin in the previous month" "2026-09-28T00:00:00+00:00" "$(json '["windows"][1]["since"]')"
expect "the month begins on its own first day"  "2026-10-01T00:00:00+00:00" "$(json '["windows"][2]["since"]')"
if grep -q '"Start": "2026-09-1[0-9]"' "${work}/ce-calls"; then
    echo "PASS  the Cost Explorer range reaches back for the fit"
else
    echo "FAIL  the Cost Explorer range reaches back for the fit"
    sed 's/^/        /' "${work}/ce-calls"
    failures=$((failures + 1))
fi

# The bill has to cover the same region the vCPU metric does, or the fit regresses every region's spend onto
# one region's vCPU-hours and both fitted figures are wrong with nothing to say so.  Asserted on the call
# rather than on the answer, because the stub cannot tell one region from another.
if grep -q '"Filter": {"Dimensions": {"Key": "REGION", "Values": \["us-west-2"\]}}' "${work}/ce-calls"; then
    echo "PASS  Cost Explorer is asked for this region only"
else
    echo "FAIL  Cost Explorer is asked for this region only"
    sed 's/^/        /' "${work}/ce-calls"
    failures=$((failures + 1))
fi

# ---------------------------------------------------------------------------------------------------
# The metrics the alarm reads
# ---------------------------------------------------------------------------------------------------

run --enforce
for metric in EstimatedSpendUsd Braked EvaluationOk; do
    if grep -q "\"${metric}\"" "${work}/metrics"; then
        echo "PASS  ${metric} is published"
    else
        echo "FAIL  ${metric} is published"
        failures=$((failures + 1))
    fi
done

# EvaluationOk is the heartbeat the stalled-guard alarm reads, so an evaluation that could not read spend
# must publish a zero rather than nothing: nothing is indistinguishable from a Lambda that never ran.
STUB_CLOUDWATCH_FAILS=1 run --enforce
if grep -q '"MetricName": "EvaluationOk", "Dimensions": \[{"Name": "ClusterName".*"Value": 0' \
        "${work}/metrics"; then
    echo "PASS  an unknown evaluation publishes EvaluationOk 0"
else
    echo "FAIL  an unknown evaluation publishes EvaluationOk 0"
    sed 's/^/        /' "${work}/metrics"
    failures=$((failures + 1))
fi

# ---------------------------------------------------------------------------------------------------
# A cluster with no guard
# ---------------------------------------------------------------------------------------------------

# The parameters are empty on a cluster where no cap was set, and this has to say which name is missing
# rather than fail inside an AWS call.
EKS_SPEND_CAPS_PARAMETER='' run --report
expect "no caps parameter exits 2" 2 "${status}"
expect_output "and names the variable" 'EKS_SPEND_CAPS_PARAMETER not set'

if [ "${failures}" -ne 0 ]; then
    echo "${failures} case(s) failed."
    exit 1
fi
echo "All cases behaved as recorded."
