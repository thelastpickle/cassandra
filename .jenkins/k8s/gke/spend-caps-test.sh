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
# Offline cases for spend-caps.py.  There is no stub CLI here and no $STUB_ variables, because the script
# calls no cloud API at all: it asks questions, does arithmetic on three numbers, and writes a tfvars file.
# So every case is that file's content, the prose the operator reads, or the exit status.
#
#   ./spend-caps-test.sh
#
# What this exists to catch.  Two things, and the second is the reason the file is this long.
#
# The first is the refusal.  A cap at or below what the cluster costs standing still is met with no build
# running at all, so the agent pools sit braked for the whole window and the cluster is a bill that does
# nothing.  That has to be refused rather than warned about, and the boundary is exact: the daily floor here
# is $12.68, so $12.68 is refused and $20 is not.
#
# The second is what can be typed.  Every amount format has a case because the parsing refuses a typo rather
# than rounding it, and a spend cap silently read as ten times what somebody meant is the one fault in this
# script nobody would notice until the bill.
#
# The prompt is driven by piping the answers, which is what makes it testable: it reads stdin and leaves the
# decision about whether there is a terminal to ../Makefile.
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
failures=0

command -v python3 >/dev/null 2>&1 || { echo "python3 needs to be installed"; exit 1; }

out="${work}/spend-caps.auto.tfvars"

# The project's own size, so the arithmetic the prompt prints has both halves, and both cost figures, so that
# a change to the defaults in the script does not silently move every expected figure below.  1122 nodes of
# 8 vCPU at 4c is $359.04 an hour; the floor is $5 fixed plus 24 node-hours of controller, so $12.68 a day,
# $88.76 a week and $380.40 a month.
export GKE_MAX_AGENT_NODES=1122
export GKE_SPEND_PRICE_PER_VCPU_HOUR=0.04
export GKE_SPEND_FIXED_USD_PER_DAY=5

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

expect_no_file() {
    local description="$1"
    if [ -e "${out}" ]; then
        echo "FAIL  ${description}"
        sed 's/^/        /' "${out}"
        failures=$((failures + 1))
    else
        echo "PASS  ${description}"
    fi
}

value() { grep -E "^$1" "${out}" | sed 's/.*= *//'; }

# ---------------------------------------------------------------------------------------------------
# Set without asking
# ---------------------------------------------------------------------------------------------------

caps --daily 1200 --weekly 4000 --monthly 12000 --email you@example.org
expect "three caps and an address are written" 0 "${status}"
expect "the daily cap"   "1200"  "$(value spend_cap_daily_usd)"
expect "the weekly cap"  "4000"  "$(value spend_cap_weekly_usd)"
expect "the monthly cap" "12000" "$(value spend_cap_monthly_usd)"
expect "the address"     '"you@example.org"' "$(value spend_alert_email)"
expect_file "the file names the script that wrote it" 'spend-caps\.py'
expect_file "and says how often the guard reads them" 'every 5 minutes'

# The arithmetic is printed, because a cap with nothing to compare it against is a guess.
expect_output "the floor is stated"    'costs about \$13 a day'
expect_output "the burn is stated"     '1122 agent nodes add about \$359 an hour'
expect_output "each cap is measured against both" 'leaving 3\.3 hour\(s\) of the pools at full size'

# A cap set now starts from the file rather than from nothing, so naming one window keeps the other two and
# the address.  This is also the only case that reads the script's own output back through read_existing.
caps_again --weekly 5000
expect "a second run keeps the caps it did not name" "1200" "$(value spend_cap_daily_usd)"
expect "and takes the one it did"                    "5000" "$(value spend_cap_weekly_usd)"
expect "and keeps the address"  '"you@example.org"' "$(value spend_alert_email)"

# `none` is an answer, and it is written as null rather than left out: OpenTofu reads an absent variable as
# its default, and this file has to say what was decided.
caps --daily none --weekly no --monthly 9000 --email none
expect "no cap is written as null" "null" "$(value spend_cap_daily_usd)"
expect 'the word `no` is no cap as well' "null" "$(value spend_cap_weekly_usd)"
expect "and the address as empty"  '""'   "$(value spend_alert_email)"
expect_output "a window with no cap says so" 'daily    no cap'

# The other three spellings of the same answer, so that a change to the list is a failure here rather than a
# refusal in front of an operator who typed a word this script used to take.
caps --daily off --weekly unlimited --monthly 0
expect '`off` is no cap'       "null" "$(value spend_cap_daily_usd)"
expect '`unlimited` is no cap' "null" "$(value spend_cap_weekly_usd)"
expect '`0` is no cap'         "null" "$(value spend_cap_monthly_usd)"

# A cap under what the cluster costs standing still is refused, not warned about: it is met with no build
# running, so the pools would be braked for the whole window.
caps --monthly 300
expect "a monthly cap under the floor is refused" 2 "${status}"
expect_output "and the floor is named" 'under the \$380 this cluster costs over a month'
expect_output "and a usable figure is offered" 'Set at least \$571'
expect_no_file "a refused cap writes nothing"

# The boundary is at the floor and not below it: $12.68 a day pays for the controller and the fixed charges
# exactly, which leaves nothing for a build, so it is refused too.
caps --daily 12.68
expect "a cap at the floor exactly is refused" 2 "${status}"
expect_output "and the reason is the same one" 'is under the \$13 this cluster costs over a day'

# A daily cap of $20 is over the $12.68 daily floor, so it is accepted, and the report says how little is
# left: this is a cap that runs the controller and no builds, which is a choice somebody may want.
caps --daily 20
expect "a cap just over the floor is accepted" 0 "${status}"
expect_output "and the floor's share of it is stated" '63% of it is the floor'

# ---------------------------------------------------------------------------------------------------
# What can be typed
# ---------------------------------------------------------------------------------------------------

caps --daily 1500
expect "a bare number is accepted" "1500" "$(value spend_cap_daily_usd)"

caps --daily '$1,500'
expect "dollars and commas are accepted" "1500" "$(value spend_cap_daily_usd)"

caps --daily 2.5k
expect "a k suffix is accepted" "2500" "$(value spend_cap_daily_usd)"

# %g writes a million in exponent form, which HCL reads as a number: `1.5e+06` is 1500000 to OpenTofu and to
# read_existing both.
caps --monthly 1.5m
expect "an m suffix is accepted" "1.5e+06" "$(value spend_cap_monthly_usd)"
caps_again --check
expect "and reads back as the figure it means" 0 "${status}"
expect_output "printed as dollars" 'monthly  \$1,500,000'

caps --daily lots
expect "a word that is not an amount is refused" 2 "${status}"
expect_output "and says what to type" 'is not an amount'

# A typo is refused rather than rounded to what it looks like.  `12OO` is two letter Os, and reading it as
# 1200 would be this script setting a cap nobody typed.
caps --daily '12OO'
expect "a typo is refused rather than rounded" 2 "${status}"
expect_output "and the typed text is quoted back" "'12OO' is not an amount"
expect_no_file "and nothing is written"

# Under a dollar is not a cap anything runs under, and it is caught before the floor is consulted so that the
# message is about the number rather than about this cluster.
caps --daily 0.5
expect "an amount under a dollar is refused" 2 "${status}"
expect_output "and says so in those terms" 'is not a cap anything could run under'

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
printf '1200\n4000\n12000\nyou@example.org\n' \
    | python3 "${here}/spend-caps.py" --prompt --out "${out}" > "${work}/output" 2>&1 || status=$?
expect "the prompt writes what was typed" 0 "${status}"
expect "the typed daily cap"   "1200"  "$(value spend_cap_daily_usd)"
expect "the typed monthly cap" "12000" "$(value spend_cap_monthly_usd)"
expect_output "the prompt states the windows are UTC" 'the week on Monday, the month on the 1st'
expect_output "and that a quota is not a limit on money" 'nothing here'

# A blank answer takes the suggestion, which is the floor plus four hours of the pools at full size.  With
# 1122 nodes that is $12.68 + 4 * $359.04, rounded to the nearest ten.  Every suggestion has to be over its
# own floor or the prompt would offer a figure it then refuses, and an exit of 0 here is that assertion: a
# suggestion under the floor is refused three times and the run ends 2.
rm -f "${out}"
status=0
printf '\n\n\n\n' \
    | python3 "${here}/spend-caps.py" --prompt --out "${out}" > "${work}/output" 2>&1 || status=$?
expect "every suggestion is over its own floor" 0 "${status}"
expect "a blank answer takes the suggestion" "1450" "$(value spend_cap_daily_usd)"
expect "and the weekly suggestion is three of those days" "4400" "$(value spend_cap_weekly_usd)"
expect "and the monthly one ten of them" "14740" "$(value spend_cap_monthly_usd)"
expect "and a blank address is nobody" '""' "$(value spend_alert_email)"

# A second run offers what is already set rather than the suggestion, so that blank answers keep the caps.
status=0
printf '\n\n\n\n' \
    | python3 "${here}/spend-caps.py" --prompt --out "${out}" > "${work}/output" 2>&1 || status=$?
expect "a re-run keeps the caps already written" "1450" "$(value spend_cap_daily_usd)"
expect_output "and offers them in the question" 'daily    cap\? \[\$1,450\]'

# A refused answer is asked again rather than taken, and the reason is printed between the two.
rm -f "${out}"
status=0
printf '10\n900\n4000\n30000\nnone\n' \
    | python3 "${here}/spend-caps.py" --prompt --out "${out}" > "${work}/output" 2>&1 || status=$?
expect "a refused answer is asked again" 0 "${status}"
expect "and the second answer is taken" "900" "$(value spend_cap_daily_usd)"
expect_output "and the reason is printed" 'under the \$13 this cluster costs over a day'

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
expect_no_file "and leaves no file behind"

# An answer that never comes is a run with no terminal, and it has to say which command to use rather than
# read an end of file as a blank.
rm -f "${out}"
status=0
python3 "${here}/spend-caps.py" --prompt --out "${out}" < /dev/null > "${work}/output" 2>&1 || status=$?
expect "no answer at all exits 2" 2 "${status}"
expect_output "and names the command to run from a terminal" 'make caps'

# No mode at all is the help and an exit of 2, so that a Makefile line that loses its arguments fails rather
# than writing a file nobody asked for.
caps
expect "no mode at all exits 2" 2 "${status}"
expect_output "and prints the usage" 'usage: spend-caps\.py'
expect_no_file "and writes nothing"

# ---------------------------------------------------------------------------------------------------
# Reporting what is set
# ---------------------------------------------------------------------------------------------------

caps --daily 1200 --weekly 4000 --monthly 12000 --email you@example.org
caps_again --check
expect "--check reads the file back" 0 "${status}"
expect_output "and reports each cap" 'monthly     \$12,000'
expect_output "and who is told" 'emails you@example.org'

caps --daily 1200
caps_again --check
expect_output "with no address it names what is told instead" 'published to Pub/Sub and to Cloud Monitoring'

rm -f "${out}"
caps_again --check
expect "--check with no file exits 1" 1 "${status}"
expect_output "and says nothing is watching" 'Nothing watches what this cluster spends'
expect_no_file "and --check writes nothing itself"

# Without the project's size, the arithmetic has one half and says which half is missing rather than
# printing a figure that leaves the agent pools out without saying so.
rm -f "${out}"
GKE_MAX_AGENT_NODES='' caps --daily 1200
expect_output "the missing half is named" 'not known here'
expect_output "and the command that fills it in" 'make quota'

if [ "${failures}" -ne 0 ]; then
    echo "${failures} case(s) failed."
    exit 1
fi
echo "All cases behaved as recorded."
