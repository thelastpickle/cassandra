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
# Regression test for spend-caps.py.  Needs no AWS at all: the script only asks questions and writes a
# tfvars file, so every case here is that file's content and the exit status.
#
#   ./spend-caps-test.sh
#
# The prompt is driven by piping the answers, which is what makes it testable: it reads stdin and leaves the
# decision about whether there is a terminal to ../Makefile.
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

out="${work}/spend-caps.auto.tfvars"

# The account's own size, so the arithmetic the prompt prints has both halves.  1122 nodes of 8 vCPU at 6c is
# $539 an hour, and the floor is $16.52 a day.
export EKS_MAX_AGENT_NODES=1122
export EKS_SPEND_PRICE_PER_VCPU_HOUR=0.06
export EKS_SPEND_FIXED_USD_PER_DAY=5

failures=0

caps() { rm -f "${out}"; status=0
    python3 "${here}/spend-caps.py" --out "${out}" "$@" > "${work}/output" 2>&1 || status=$?; }

# The same, keeping whatever the last case wrote, which is what a second `make caps` does.
caps_again() { status=0
    python3 "${here}/spend-caps.py" --out "${out}" "$@" > "${work}/output" 2>&1 || status=$?; }

expect() {
    local description="$1" expected="$2" actual="$3"
    if [ "${actual}" = "${expected}" ]; then
        echo "PASS  ${description} (${actual})"
    else
        echo "FAIL  ${description}: expected ${expected}, got ${actual}"
        failures=$((failures + 1))
    fi
}

expect_file() {
    local description="$1" pattern="$2"
    if grep -qE "${pattern}" "${out}" 2>/dev/null; then
        echo "PASS  ${description}"
    else
        echo "FAIL  ${description}: no line of ${out} matches /${pattern}/"
        sed 's/^/        /' "${out}" 2>/dev/null || echo "        (no file)"
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

value() { grep -E "^$1" "${out}" | sed 's/.*= *//'; }

# ---------------------------------------------------------------------------------------------------
# Set without asking
# ---------------------------------------------------------------------------------------------------

caps --daily 1200 --weekly 4000 --monthly 12000 --email me@example.org
expect "three caps and an address are written" 0 "${status}"
expect "the daily cap"   "1200"  "$(value spend_cap_daily_usd)"
expect "the weekly cap"  "4000"  "$(value spend_cap_weekly_usd)"
expect "the monthly cap" "12000" "$(value spend_cap_monthly_usd)"
expect "the address"     '"me@example.org"' "$(value spend_alert_email)"
expect_file "the file names the script that wrote it" 'spend-caps\.py'

# The arithmetic is printed, because a cap with nothing to compare it against is a guess.
expect_output "the floor is stated"    'costs about \$17 a day'
expect_output "the burn is stated"     '1122 agent nodes add about \$539 an hour'
expect_output "each cap is measured against both" 'leaving 2\.2 hour\(s\) of the pools at full size'

# `none` is an answer, and it is written as null rather than left out: OpenTofu reads an absent variable as
# its default, and this file has to say what was decided.
caps --daily none --weekly none --monthly 9000 --email none
expect "no cap is written as null" "null" "$(value spend_cap_daily_usd)"
expect "and the address as empty"  '""'   "$(value spend_alert_email)"
expect_output "a window with no cap says so" 'daily    no cap'

# A cap under what the cluster costs standing still is refused, not warned about: it is met with no build
# running, so the pools would be braked for the whole window.
caps --monthly 400
expect "a monthly cap under the floor is refused" 2 "${status}"
expect_output "and the floor is named" 'under the \$496 this cluster costs over a month'
expect_output "and a usable figure is offered" 'Set at least \$743'
if [ -e "${out}" ]; then
    echo "FAIL  a refused cap writes nothing"
    failures=$((failures + 1))
else
    echo "PASS  a refused cap writes nothing"
fi

# A daily cap of $20 is over the $16.52 daily floor, so it is accepted, and the report says how little is
# left: this is a cap that runs the controller and no builds, which is a choice somebody may want.
caps --daily 20
expect "a cap just over the floor is accepted" 0 "${status}"
expect_output "and the floor's share of it is stated" '83% of it is the floor'

# ---------------------------------------------------------------------------------------------------
# What can be typed
# ---------------------------------------------------------------------------------------------------

caps --daily '$1,500'
expect "dollars and commas are accepted" "1500" "$(value spend_cap_daily_usd)"

caps --daily 2.5k
expect "a k suffix is accepted" "2500" "$(value spend_cap_daily_usd)"

caps --daily lots
expect "a word that is not an amount is refused" 2 "${status}"
expect_output "and says what to type" 'is not an amount'

caps --daily 900 --email 'not-an-address'
expect "an address that is not one is refused" 2 "${status}"
expect_output "and says what to type instead" 'is not an email address'

# ---------------------------------------------------------------------------------------------------
# The prompt
# ---------------------------------------------------------------------------------------------------

# Three amounts and an address, typed.  The prompt offers a suggestion for each, so a blank answer is a
# separate case below.
rm -f "${out}"
status=0
printf '1200\n4000\n12000\nme@example.org\n' \
    | python3 "${here}/spend-caps.py" --prompt --out "${out}" > "${work}/output" 2>&1 || status=$?
expect "the prompt writes what was typed" 0 "${status}"
expect "the typed daily cap"   "1200"  "$(value spend_cap_daily_usd)"
expect "the typed monthly cap" "12000" "$(value spend_cap_monthly_usd)"
expect_output "the prompt states the windows are UTC" 'the week on Monday, the month on the 1st'

# A blank answer takes the suggestion, which is the floor plus four hours of the pools at full size.  With
# 1122 nodes that is $16.52 + 4 * $538.56, rounded to the nearest ten.
rm -f "${out}"
status=0
printf '\n\n\n\n' \
    | python3 "${here}/spend-caps.py" --prompt --out "${out}" > "${work}/output" 2>&1 || status=$?
expect "a blank answer takes the suggestion" "2170" "$(value spend_cap_daily_usd)"
expect "and the weekly suggestion is three of those days" "6580" "$(value spend_cap_weekly_usd)"
expect "and a blank address is nobody" '""' "$(value spend_alert_email)"

# A second run offers what is already set rather than the suggestion, so that blank answers keep the caps.
status=0
printf '\n\n\n\n' \
    | python3 "${here}/spend-caps.py" --prompt --out "${out}" > "${work}/output" 2>&1 || status=$?
expect "a re-run keeps the caps already written" "2170" "$(value spend_cap_daily_usd)"
expect_output "and offers them in the question" 'daily    cap\? \[\$2,170\]'

# A refused answer is asked again rather than taken, and the reason is printed between the two.
rm -f "${out}"
status=0
printf '10\n900\n4000\n30000\nnone\n' \
    | python3 "${here}/spend-caps.py" --prompt --out "${out}" > "${work}/output" 2>&1 || status=$?
expect "a refused answer is asked again" 0 "${status}"
expect "and the second answer is taken" "900" "$(value spend_cap_daily_usd)"
expect_output "and the reason is printed" 'under the \$17 this cluster costs over a day'

# No caps at all needs confirming, because it is indistinguishable from having skipped the question.
rm -f "${out}"
status=0
printf 'none\nnone\nnone\nyes\nnone\n' \
    | python3 "${here}/spend-caps.py" --prompt --out "${out}" > "${work}/output" 2>&1 || status=$?
expect "no caps at all is accepted when confirmed" 0 "${status}"
expect "and every window is null" "null" "$(value spend_cap_monthly_usd)"
expect_output "and it is spelled out first" 'nothing will watch what this cluster spends'

rm -f "${out}"
status=0
printf 'none\nnone\nnone\n\n' \
    | python3 "${here}/spend-caps.py" --prompt --out "${out}" > "${work}/output" 2>&1 || status=$?
expect "no caps at all unconfirmed writes nothing" 2 "${status}"
if [ -e "${out}" ]; then
    echo "FAIL  and leaves no file behind"
    failures=$((failures + 1))
else
    echo "PASS  and leaves no file behind"
fi

# An answer that never comes is a run with no terminal, and it has to say which command to use rather than
# read an end of file as a blank.
rm -f "${out}"
status=0
python3 "${here}/spend-caps.py" --prompt --out "${out}" < /dev/null > "${work}/output" 2>&1 || status=$?
expect "no answer at all exits 2" 2 "${status}"
expect_output "and names the command to run from a terminal" 'make caps'

# ---------------------------------------------------------------------------------------------------
# Reporting what is set
# ---------------------------------------------------------------------------------------------------

caps --daily 1200 --weekly 4000 --monthly 12000 --email me@example.org
caps_again --check
expect "--check reads the file back" 0 "${status}"
expect_output "and reports each cap" 'monthly     \$12,000'
expect_output "and who is told" 'emails me@example.org'

rm -f "${out}"
caps_again --check
expect "--check with no file exits 1" 1 "${status}"
expect_output "and says nothing is watching" 'Nothing watches what this cluster spends'

# Without the account's size, the arithmetic has one half and says which half is missing rather than
# printing a figure that leaves the agent pools out without saying so.
rm -f "${out}"
EKS_MAX_AGENT_NODES='' caps --daily 1200
expect_output "the missing half is named" 'not known here'
expect_output "and the command that fills it in" 'make quota'

if [ "${failures}" -ne 0 ]; then
    echo "${failures} case(s) failed."
    exit 1
fi
echo "All cases behaved as recorded."
