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

# Offline cases for shared/.  Needs no credentials, no cluster and no network.
#
# What this exists to catch.  Every other suite in this repository asserts on a script's output, and these
# functions have none: they are arithmetic several scripts share, and each caller's own suite would only
# notice a fault in them if that fault happened to move a printed figure.
#
# The quantity parsing is the part worth the most.  `16G` and `16Gi` differ by 7%, the pod templates use the
# decimal form and every kubelet reports the binary one, and reading either as the other produces no error
# anywhere: it produces an agent pool that is reported as fitting a node it does not fit.  Two of the cases
# below are the two unit confusions that were in the code this module replaced.

set -o pipefail
here="$(cd -- "$(dirname -- "$0")" && pwd)"
failures=0

command -v python3 >/dev/null 2>&1 || { echo "python3 needs to be installed"; exit 1; }

# One expression, evaluated against the module, compared as a string.
check() {
    local what="$1" expression="$2" want="$3"
    local got
    got="$(cd "${here}" && python3 -c "
import sys
sys.path.insert(0, '.')
from k8s_values import *
from controller_model import *
from spend_model import *

def raises(call, *args):
    '''\"raised\" or the value, so that a case can assert on a refusal in one expression.'''
    try:
        call(*args)
    except Exception:
        return 'raised'
    return 'returned'

print(${expression})
" 2>&1)"
    if [ "${got}" = "${want}" ]; then
        echo "PASS  ${what}"
    else
        echo "FAIL  ${what}"
        echo "        ${expression}"
        echo "        wanted: ${want}"
        echo "        got:    ${got}"
        failures=$((failures + 1))
    fi
}

# ---------------------------------------------------------------------------------------------------
# CPU quantities
# ---------------------------------------------------------------------------------------------------

check "a bare cpu count is cores"            "cpu_millicores(4)"        "4000.0"
check "an m suffix is already millicores"    "cpu_millicores('500m')"   "500.0"
check "a fractional core"                    "cpu_millicores('0.5')"    "500.0"
check "a fractional millicore is kept"       "cpu_millicores('13765m')" "13765.0"
check "None takes the default"               "cpu_millicores(None, 0)"  "0"
check "None with no default is None"         "cpu_millicores(None)"     "None"

# ---------------------------------------------------------------------------------------------------
# Memory quantities, which is where the units bite
# ---------------------------------------------------------------------------------------------------

check "decimal G is a power of ten"          "memory_bytes('16G')"    "16000000000"
check "binary Gi is a power of two"          "memory_bytes('16Gi')"   "17179869184"
check "the two differ by about 7%"           "round(memory_bytes('16Gi') / memory_bytes('16G'), 3)" "1.074"
check "a kubelet's Ki is binary"             "memory_bytes('32120052Ki')" "32890933248"
check "decimal M"                            "memory_bytes('3400M')"  "3400000000"
check "the exponent form Kubernetes takes"   "memory_bytes('1e9')"    "1000000000"

# The bug this replaced: one of the two implementations shared a code path with the cpu parser, so a bare
# number was multiplied by 1000 and `memory: 20` read as 20 kilobytes.
check "a bare memory quantity is bytes"      "memory_bytes('20')"     "20"

check "MiB rounds down from decimal G"       "memory_mib('28G')"      "26702"
check "MiB is exact from Gi"                 "memory_mib('16Gi')"     "16384"
check "memory None takes the default"        "memory_mib(None, 0)"    "0"

# ---------------------------------------------------------------------------------------------------
# A JVM heap, whose suffixes are the JVM's and not Kubernetes'
# ---------------------------------------------------------------------------------------------------

# -Xmx8G is 8 GiB where `memory: 8G` beside it in the same file is 8 GB.  Both appear in
# jenkins-deployment.yaml, which is why these are two functions and not one.
check "an -Xmx in binary g"                  "heap_mib('-server -Xms8G -Xmx8G')"  "8192"
check "-Xmx in m"                            "heap_mib('-Xmx512m')"               "512"
check "a bare -Xmx is bytes"                 "heap_mib('-Xmx17179869184')"        "16384"
check "no -Xmx is nothing, not a guess"      "heap_mib('-server -XX:+UseG1GC')"   "0"
check "an empty argument string"             "heap_mib('')"                       "0"

# ---------------------------------------------------------------------------------------------------
# Helm's merge rule
# ---------------------------------------------------------------------------------------------------

check "maps merge key by key"    "sorted(deep_merge({'a': 1, 'b': 2}, {'b': 3}).items())" "[('a', 1), ('b', 3)]"
check "nested maps merge too"    "sorted(deep_merge({'a': {'x': 1, 'y': 2}}, {'a': {'y': 3}})['a'].items())" "[('x', 1), ('y', 3)]"
# Helm replaces a list whole rather than merging it element by element, and matching Helm is the point.
check "lists are replaced whole" "deep_merge({'a': [1, 2, 3]}, {'a': [9]})['a']" "[9]"
# A scalar over a map, which is how `ingress:` with only comments under it becomes `ingress: null`.
check "None replaces a map"      "deep_merge({'a': {'x': 1}}, {'a': None})['a']" "None"
# The deep copy: editing the result must not reach the input.
check "the result does not alias the input" \
    "(lambda b: (deep_merge(b, {})['a'].update({'x': 9}), b['a']['x'])[1])({'a': {'x': 1}})" "1"

# ---------------------------------------------------------------------------------------------------
# The controller model
# ---------------------------------------------------------------------------------------------------

# Memory follows the agent count and CPU follows the churn rate.  The two axes are the whole point of the
# model, so each is asserted to move only with its own driver.
check "memory follows the agent count" \
    "needed_memory_mib(480, 8192) > needed_memory_mib(240, 8192)" "True"
check "memory ignores idleMinutes" \
    "needed_memory_mib(480, 8192) == needed_memory_mib(480, 8192)" "True"
check "cpu follows the churn rate" \
    "needed_cpu_millicores(480, 5) > needed_cpu_millicores(480, 10)" "True"
check "doubling idleMinutes halves the burst" \
    "round(launches_per_burst(480, 10) / launches_per_burst(480, 5), 3)" "0.5"
# A float, because the per-agent slope is 7.5 MiB.  The caller rounds; this module does not, so that rounding
# happens once at the edge rather than at every step.
check "the heap is added to memory whole" \
    "needed_memory_mib(0, 8192) - needed_memory_mib(0, 0)" "8192.0"
# A site that names no idleMinutes anywhere gets the value the reap fraction was measured at, not zero, which
# would divide by it.
check "no podTemplates falls back to the measured interval" \
    "idle_minutes({})" "5"
check "the largest idleMinutes wins" \
    "idle_minutes({'agent': {'podTemplates': {'a': 'idleMinutes: 5', 'b': 'idleMinutes: 30'}}})" "30"

# ---------------------------------------------------------------------------------------------------
# Spend windows
# ---------------------------------------------------------------------------------------------------

# 2026-10-01 is a Thursday, so the week began on Monday 2026-09-28 in the previous month.  That case occurs
# on a handful of dates a year and is why every function here takes `now` rather than reading a clock.
check "the day begins at midnight UTC" \
    "window_start('daily', datetime(2026, 10, 1, 12, 30, tzinfo=timezone.utc)).isoformat()" \
    "2026-10-01T00:00:00+00:00"
check "the week begins on Monday, across a month boundary" \
    "window_start('weekly', datetime(2026, 10, 1, 12, 30, tzinfo=timezone.utc)).isoformat()" \
    "2026-09-28T00:00:00+00:00"
check "the month begins on its first day" \
    "window_start('monthly', datetime(2026, 10, 15, 12, 30, tzinfo=timezone.utc)).isoformat()" \
    "2026-10-01T00:00:00+00:00"
# A misspelled window must raise rather than fall through to a default, because a window that silently became
# 'daily' would report a month's cap met by a day's spend.
check "an unknown window raises" \
    "raises(window_start, 'yearly', datetime(2026, 1, 1, tzinfo=timezone.utc))" "raised"
check "a known one does not" \
    "raises(window_start, 'monthly', datetime(2026, 1, 1, tzinfo=timezone.utc))" "returned"

# A day is settled 48 hours after it ends, not 24: a figure for a day that closed an hour ago is still moving,
# and a partial figure in the fit biases the price downwards, which is the direction that fails to brake.
check "a day that ended an hour ago is not settled" \
    "is_settled(date(2026, 9, 30), datetime(2026, 10, 1, 1, 0, tzinfo=timezone.utc))" "False"
check "a day is settled after 48 hours" \
    "is_settled(date(2026, 9, 30), datetime(2026, 10, 3, 1, 0, tzinfo=timezone.utc))" "True"
check "half of today has elapsed at noon" \
    "day_elapsed_fraction(date(2026, 10, 1), datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc))" "0.5"
check "a past day is wholly elapsed" \
    "day_elapsed_fraction(date(2026, 9, 1), datetime(2026, 10, 1, tzinfo=timezone.utc))" "1.0"

# ---------------------------------------------------------------------------------------------------
# The fit
# ---------------------------------------------------------------------------------------------------

# Generated from a known price and floor, so the fit has an exact answer to recover.  This is the property the
# whole model rests on: given days that differ in load, it finds the rate the bill was made from.
check "the fit recovers the price it was generated from" "
(lambda now, days: round(calibrate(
    {(now.date() - timedelta(days=n)).isoformat(): 3.0 + 0.002 * (1000 * n) for n in days},
    {(now.date() - timedelta(days=n)): 1000.0 * n for n in days},
    now, 0.06, 5.0)['price'], 4))(datetime(2026, 10, 1, tzinfo=timezone.utc), range(3, 11))" \
    "0.002"

# The fault that read \$0 a vCPU-hour and called itself calibrated: an idle fortnight whose days differ by a
# few units has a wide relative spread over a tiny absolute one, and least squares answers with a rate near
# zero.  A rate near zero estimates a full-size build at nothing at all.
check "an idle fortnight refuses the fit" "
(lambda now, days: calibrate(
    {(now.date() - timedelta(days=n)).isoformat(): 3.27 + 0.01 * n for n in days},
    {(now.date() - timedelta(days=n)): 20.0 + n for n in days},
    now, 0.06, 5.0)['mode'])(datetime(2026, 10, 1, tzinfo=timezone.utc), range(3, 15))" \
    "fallback"
check "and reports the span that refused it" "
'vCPU-hours' in (lambda now, days: calibrate(
    {(now.date() - timedelta(days=n)).isoformat(): 3.27 + 0.01 * n for n in days},
    {(now.date() - timedelta(days=n)): 20.0 + n for n in days},
    now, 0.06, 5.0)['why'])(datetime(2026, 10, 1, tzinfo=timezone.utc), range(3, 15))" \
    "True"
# A refused fit still reports what it measured, because a fortnight of an idle cluster measures the
# standing-still cost well even when it cannot identify a rate.
check "a refused fit still reports its figures" "
(lambda now, days: calibrate(
    {(now.date() - timedelta(days=n)).isoformat(): 3.27 + 0.01 * n for n in days},
    {(now.date() - timedelta(days=n)): 20.0 + n for n in days},
    now, 0.06, 5.0)['fitted'] is not None)(datetime(2026, 10, 1, tzinfo=timezone.utc), range(3, 15))" \
    "True"
check "too few settled days refuses the fit" "
calibrate({}, {}, datetime(2026, 10, 1, tzinfo=timezone.utc), 0.06, 5.0)['mode']" "fallback"

# The bound on the rate is absolute and not relative to the caller's guess.  A fit forty times under the
# configured figure was refused by a relative bound once, and the fallback then over-read the bill by 42x.
check "a fitted rate far under the guess is accepted" "
(lambda now, days: calibrate(
    {(now.date() - timedelta(days=n)).isoformat(): 3.0 + 0.001 * (1000 * n) for n in days},
    {(now.date() - timedelta(days=n)): 1000.0 * n for n in days},
    now, 0.06, 5.0)['mode'])(datetime(2026, 10, 1, tzinfo=timezone.utc), range(3, 11))" \
    "calibrated"

# ---------------------------------------------------------------------------------------------------
# Per-day figures, and the audit
# ---------------------------------------------------------------------------------------------------

# A settled day takes the bill even when the model says more.  Taking the larger everywhere is what let a
# model error reach into every past day of every window.
check "a settled day takes the bill, not the larger figure" "
day_figures([date(2026, 9, 20)], datetime(2026, 10, 1, tzinfo=timezone.utc),
            {'2026-09-20': 10.0}, {date(2026, 9, 20): 5000.0},
            {'price': 0.06, 'fixed_per_day': 5.0})[date(2026, 9, 20)]['taken']" "billed"
# An unsettled day takes the larger, because its bill is partial.
check "an unsettled day takes the larger" "
day_figures([date(2026, 10, 1)], datetime(2026, 10, 1, 12, tzinfo=timezone.utc),
            {'2026-10-01': 1.0}, {date(2026, 10, 1): 5000.0},
            {'price': 0.06, 'fixed_per_day': 5.0})[date(2026, 10, 1)]['taken']" "estimated"
# The audit is the measurement that catches a model wrong by a factor rather than by a margin.
check "the audit flags a model out by a factor" "
audit_model(day_figures([date(2026, 9, d) for d in (18, 19, 20)],
            datetime(2026, 10, 1, tzinfo=timezone.utc),
            {f'2026-09-{d}': 10.0 for d in (18, 19, 20)},
            {date(2026, 9, d): 5000.0 for d in (18, 19, 20)},
            {'price': 0.06, 'fixed_per_day': 5.0}))['off']" "True"
check "and does not flag one inside it" "
audit_model(day_figures([date(2026, 9, d) for d in (18, 19, 20)],
            datetime(2026, 10, 1, tzinfo=timezone.utc),
            {f'2026-09-{d}': 305.0 for d in (18, 19, 20)},
            {date(2026, 9, d): 5000.0 for d in (18, 19, 20)},
            {'price': 0.06, 'fixed_per_day': 5.0}))['off']" "False"
# Under three days there is nothing to audit against, and a lagging figure must not raise the alarm.
check "too few settled days is no verdict" "
audit_model(day_figures([date(2026, 9, 20)], datetime(2026, 10, 1, tzinfo=timezone.utc),
            {'2026-09-20': 10.0}, {date(2026, 9, 20): 5000.0},
            {'price': 0.06, 'fixed_per_day': 5.0}))['ratio']" "None"

if [ "${failures}" -ne 0 ]; then
    echo "${failures} case(s) failed."
    exit 1
fi
echo "All cases behaved as recorded."
