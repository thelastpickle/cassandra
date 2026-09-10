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

# Offline cases for build-jenkins-values.py's --pools handling, which rewrites each agent podTemplate's
# instanceCap to the size layer 1 created that pool at.
#
# What this exists to catch.  A cap above its pool's nodes is not idle: `.build/run-ci` treats it as an error
# and prints "Refusing to deploy agent podTemplates that could never be scheduled".  So the day layer 1
# started scaling the pools, every account whose quota is smaller than this one's lost its deploy.  A cap
# below its pool's nodes is the quieter half: the nodes exist and Jenkins never asks for them.
#
# Needs no credentials.  It runs against the committed jenkins-deployment.yaml and a hand-written pools JSON
# of the shape `tofu output -json agent_node_groups` prints.

set -o pipefail
here="$(cd -- "$(dirname -- "$0")" && pwd)"
script="${here}/build-jenkins-values.py"
deployment="${here}/../../jenkins-deployment.yaml"
work="$(mktemp -d)"
# Checked, because this script has no `set -e` and an unchecked failure here leaves ${work} empty: every
# fixture path then resolves to /, and the suite reports its own cases as failures.
[ -n "${work}" ] && [ -d "${work}" ] || { echo "mktemp -d failed, so there is nowhere to write the fixtures"; exit 1; }
trap 'rm -rf "${work}"' EXIT
failures=0

command -v python3 >/dev/null 2>&1 || { echo "python3 needs to be installed"; exit 1; }
python3 -c 'import yaml' 2>/dev/null || { echo "SKIP  build-jenkins-values-test.sh needs PyYAML"; exit 0; }
[ -f "${deployment}" ] || { echo "FAIL  ${deployment} must exist"; exit 1; }

printf 'controller:\n  jenkinsUrlProtocol: http\n' > "${work}/base.yaml"

# One row per pool, keyed by the size word, as layer 1 prints it.  `max_size` is what the node groups were
# created with and is deliberately not the committed cap, so every case below has something to rewrite.
pools_file() {
    cat > "${work}/pools.json" <<JSON
{
  "small":  { "max_size": $1, "instance_types": ["c7a.2xlarge"], "node_selector_label": "cassandra.jenkins.agent.small" },
  "medium": { "max_size": $2, "instance_types": ["c7a.2xlarge"], "node_selector_label": "cassandra.jenkins.agent.medium" },
  "large":  { "max_size": $3, "instance_types": ["m7a.2xlarge"], "node_selector_label": "cassandra.jenkins.agent.large" },
  "report": { "max_size": $4, "instance_types": ["m7a.2xlarge"], "node_selector_label": "cassandra.jenkins.agent.report" }
}
JSON
}

# The caps every template ends up with, after the overlay is merged over the deployment the way helm merges
# them, printed as "<size>=<instanceCap>,<instanceCapStr>" and sorted.  Reading them back through a YAML
# parser is the point: a substitution that broke the string would fail here rather than at a deploy.
caps() {
    python3 - "$1" "${deployment}" <<'PY'
import re, sys, yaml
out = yaml.safe_load(open(sys.argv[1])) or {}
dep = yaml.safe_load(open(sys.argv[2])) or {}
merged = dict(dep.get("agent", {}).get("podTemplates", {}))
merged.update(out.get("agent", {}).get("podTemplates", {}))
rows = []
for name, body in merged.items():
    yaml.safe_load(body)
    size = re.search(r"cassandra\.jenkins\.agent\.([a-z0-9]+)\s*=\s*true", body).group(1)
    cap = re.search(r"^\s*instanceCap:\s*(\d+)\s*$", body, re.M).group(1)
    cap_str = re.search(r"^\s*instanceCapStr:\s*\"(\d+)\"\s*$", body, re.M).group(1)
    rows.append("%s=%s,%s" % (size, cap, cap_str))
print(" ".join(sorted(rows)))
PY
}

run() {
    "${script}" --base "${work}/base.yaml" --out "${work}/out.yaml" --deployment "${deployment}" "$@" \
        > "${work}/output" 2>&1
}

expect() {
    local what="$1" want="$2" got="$3"
    if [ "${want}" = "${got}" ]; then
        echo "PASS  ${what}"
    else
        echo "FAIL  ${what}"
        echo "        want: ${want}"
        echo "        got:  ${got}"
        failures=$((failures + 1))
    fi
}

# ---------------------------------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------------------------------

# Scaled down, which is the case that used to break the deploy.  Every cap follows its own pool.
pools_file 10 75 153 2
run --pools "${work}/pools.json" --container-cap 240
expect "a shrunken account gets each cap lowered to its pool" \
    "large=153,153 medium=75,75 report=2,2 small=10,10" "$(caps "${work}/out.yaml")"

# Scaled up.  The caps have to rise too, or the nodes layer 1 created are never asked for.
pools_file 21 159 325 4
run --pools "${work}/pools.json" --container-cap 509
expect "a raised quota gets each cap raised to its pool" \
    "large=325,325 medium=159,159 report=4,4 small=21,21" "$(caps "${work}/out.yaml")"

# The invariant jenkins-deployment.yaml states above agent.containerCap: the cap across every template
# equals the sum of the four.  It is what keeps one pool from taking the whole cluster's allowance.
sum="$(python3 - "${work}/out.yaml" "${deployment}" <<'PY'
import re, sys, yaml
out = yaml.safe_load(open(sys.argv[1])) or {}
dep = yaml.safe_load(open(sys.argv[2])) or {}
merged = dict(dep.get("agent", {}).get("podTemplates", {}))
merged.update(out.get("agent", {}).get("podTemplates", {}))
print(sum(int(re.search(r"^\s*instanceCap:\s*(\d+)\s*$", b, re.M).group(1)) for b in merged.values()))
PY
)"
container="$(python3 -c "import sys,yaml; print(yaml.safe_load(open('${work}/out.yaml'))['agent']['containerCap'])")"
expect "the four caps sum to containerCap" "${sum}" "${container}"

# Without --pools nothing is rewritten, which is what every deploy did before the pools were scaled.
run --container-cap 480
expect "no --pools leaves every committed cap alone" \
    "large=306,306 medium=150,150 report=4,4 small=20,20" "$(caps "${work}/out.yaml")"

# A pools file that cannot be read is the same as no pools file.  `make jenkins` builds it from tofu output
# on the same run, so an unreadable one means the apply is stale and that is not this script's to report.
run --pools "${work}/absent.json" --container-cap 480
expect "an unreadable pools file leaves every cap alone" \
    "large=306,306 medium=150,150 report=4,4 small=20,20" "$(caps "${work}/out.yaml")"
if ! grep -q 'leaving each instanceCap as committed' "${work}/output"; then
    echo "FAIL  an unreadable pools file says so"
    sed 's/^/        /' "${work}/output"
    failures=$((failures + 1))
else
    echo "PASS  an unreadable pools file says so"
fi

# A pool missing from the output is left as committed rather than dropped to a default.  This is the shape of
# a `tofu output` written before an agent pool was added.
cat > "${work}/partial.json" <<'JSON'
{
  "large": { "max_size": 200, "instance_types": ["m7a.2xlarge"], "node_selector_label": "cassandra.jenkins.agent.large" }
}
JSON
run --pools "${work}/partial.json" --container-cap 400
expect "a pool absent from the output keeps its committed cap" \
    "large=200,200 medium=150,150 report=4,4 small=20,20" "$(caps "${work}/out.yaml")"

# A pool already at its committed cap is not rewritten at all, so the overlay stays as small as it can be.
pools_file 20 150 306 4
run --pools "${work}/pools.json" --container-cap 480
templates="$(python3 -c "import yaml; v=yaml.safe_load(open('${work}/out.yaml')); print(len(v.get('agent',{}).get('podTemplates',{})))")"
expect "caps that already match are not emitted" "0" "${templates}"

if [ "${failures}" -ne 0 ]; then
    echo "${failures} case(s) failed."
    exit 1
fi
echo "All cases behaved as recorded."
