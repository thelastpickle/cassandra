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
# What this exists to catch.  A cap above its pool's nodes is not idle: each surplus pod stays Pending, drives
# the pool to its maximum, expires after waitForPodSec and is asked for again, which is the churn that preceded
# the 2026-08-11 controller stall.  On EKS `.build/run-ci` refuses that deploy outright, printing "Refusing to
# deploy agent podTemplates that could never be scheduled", because it can read each pool's ceiling from the
# cluster-autoscaler status configmap.  GKE's autoscaler is part of the control plane and publishes no such
# configmap, so run-ci establishes nothing here and reports "could not establish": on this cloud these cases
# are the only thing standing between a shrunken project and that churn.  A cap below its pool's nodes is the
# quieter half: the nodes exist and Jenkins never asks for them.
#
# Needs no credentials.  It runs against the committed jenkins-deployment.yaml and a hand-written pools JSON of
# the shape `tofu output -json agent_node_pools` prints.

set -o pipefail
here="$(cd -- "$(dirname -- "$0")" && pwd)"
script="${here}/build-jenkins-values.py"
deployment="${here}/../../jenkins-deployment.yaml"
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
python3 -c 'import yaml' 2>/dev/null || { echo "SKIP  build-jenkins-values-test.sh needs PyYAML"; exit 0; }
[ -f "${deployment}" ] || { echo "FAIL  ${deployment} must exist"; exit 1; }

printf 'controller:\n  jenkinsUrlProtocol: http\n' > "${work}/base.yaml"

# One object per pool size, keyed by the size word, as layer 1 prints it.  `max_size` is the pool's total
# across every zone, which is what the pools were created with, and is deliberately not the committed cap, so
# every case below has something to rewrite.
pools_file() {
    cat > "${work}/pools.json" <<JSON
{
  "small":  { "max_size": $1, "machine_types": ["e2-highcpu-8"], "spot": true, "node_selector_label": "cassandra.jenkins.agent.small" },
  "medium": { "max_size": $2, "machine_types": ["n2-highcpu-8"], "spot": true, "node_selector_label": "cassandra.jenkins.agent.medium" },
  "large":  { "max_size": $3, "machine_types": ["n2-standard-8"], "spot": true, "node_selector_label": "cassandra.jenkins.agent.large" },
  "report": { "max_size": $4, "machine_types": ["n2-standard-8"], "spot": true, "node_selector_label": "cassandra.jenkins.agent.report" }
}
JSON
}

# The caps every template ends up with, after the overlay is merged over the deployment the way helm merges
# them, printed as "<size>=<instanceCap>,<instanceCapStr>" and sorted.  Reading them back through a YAML
# parser is the point: a substitution that broke the string would fail here rather than at a deploy.  Both keys
# are read, because the plugin takes whichever applies last and run-ci errors when the two disagree, which is
# the one part of its capacity check that still runs on GKE.
caps() {
    python3 - "$1" "${deployment}" <<'PY'
import re, sys, yaml
out = yaml.safe_load(open(sys.argv[1])) or {}
dep = yaml.safe_load(open(sys.argv[2])) or {}
merged = dict(dep.get("agent", {}).get("podTemplates", {}))
merged.update(out.get("agent", {}).get("podTemplates", {}))
totals = {}
for body in merged.values():
    for template in yaml.safe_load(body):
        size = re.search(r"cassandra\.jenkins\.agent\.([a-z0-9]+)\s*=\s*true", template['nodeSelector']).group(1)
        cap, cap_str = totals.get(size, (0, 0))
        totals[size] = (cap + template['instanceCap'], cap_str + int(template['instanceCapStr']))
print(" ".join("%s=%s,%s" % (size, *caps) for size, caps in sorted(totals.items())))
PY
}

run() {
    # `python3 "${script}"`, not `"${script}"`: the shebang's `env python3` can resolve to a different
    # interpreter from the PATH `python3` the PyYAML gate above checked, and then every case in this file gets
    # one import error instead of a report.  The other GKE suites invoke their script the same way.
    python3 "${script}" --base "${work}/base.yaml" --out "${work}/out.yaml" --deployment "${deployment}" "$@" \
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

for hostname in '' jenkins.example.org; do
    run --hostname "${hostname}"
    ingress="$(python3 - "${work}/out.yaml" "${deployment}" <<'PY'
import sys, yaml
out, deployment = (yaml.safe_load(open(path)) for path in sys.argv[1:])
controller = out.get("controller", {})
print(controller.get("ingress", deployment["controller"]["ingress"])["enabled"])
PY
)"
    expect "the Service load balancer needs no Ingress (hostname=${hostname})" "False" "${ingress}"
done

# Scaled down, which is the case that churns pods no node can hold.  Every cap follows its own pool.
pools_file 10 75 153 2
run --pools "${work}/pools.json" --container-cap 240
expect "a shrunken project gets each cap lowered to its pool" \
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
print(sum(t['instanceCap'] for b in merged.values() for t in yaml.safe_load(b)))
PY
)"
container="$(python3 -c "import sys,yaml; print(yaml.safe_load(open('${work}/out.yaml'))['agent']['containerCap'])")"
expect "the template caps sum to containerCap" "${sum}" "${container}"

# Without --pools nothing is rewritten, which is what every deploy did before the pools were scaled.
run --container-cap 480
expect "no --pools leaves every committed cap alone" \
    "large=306,306 medium=150,150 report=4,4 small=20,20" "$(caps "${work}/out.yaml")"

# Refuse stale pool data instead of installing shared caps that can consume every worker slot.
run --pools "${work}/absent.json" --container-cap 480
expect "an unreadable pools file stops generation" "2" "$?"
cat > "${work}/partial.json" <<'JSON'
{"large": {"max_size": 200}}
JSON
run --pools "${work}/partial.json" --container-cap 400
expect "a missing small pool stops generation" "2" "$?"
pools_file 4 34 49 1
run --pools "${work}/pools.json" --container-cap 3
expect "the cloud cap must leave room for a worker" "2" "$?"
printf 'agent:\n  containerCap: 3\n' > "${work}/base.yaml"
run --pools "${work}/pools.json"
expect "a site cloud cap must also leave room for a worker" "2" "$?"
printf 'controller:\n  jenkinsUrlProtocol: http\n' > "${work}/base.yaml"
python3 "${script}" --base "${work}/base.yaml" --out "${work}/out.yaml" --pools "${work}/pools.json" \
    > "${work}/output" 2>&1
expect "pool sizing needs the deployment templates" "2" "$?"

# Matching pool sizes still need separate pipeline and worker templates.
pools_file 20 150 306 4
run --pools "${work}/pools.json" --container-cap 480
templates="$(python3 -c "import yaml; v=yaml.safe_load(open('${work}/out.yaml')); print(len(v.get('agent',{}).get('podTemplates',{})))")"
expect "only the two small-pool templates change when caps match" "2" "${templates}"

if [ "${failures}" -ne 0 ]; then
    echo "${failures} case(s) failed."
    exit 1
fi
echo "All cases behaved as recorded."
