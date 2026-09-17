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

# Run the smoke command with cloud replies and the separately tested fit/spend checks stubbed.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
work="$(mktemp -d "${TMPDIR:-/tmp}/gke-smoke-test.XXXXXX")"
trap 'rm -rf "${work}"' EXIT
mkdir -p "${work}/repo/.build" "${work}/bin" "${work}/repo/.jenkins/k8s/gke/3-smoke"
suite="${work}/repo/.jenkins/k8s/gke"
cp "${here}/smoke-test.sh" "${suite}/3-smoke/"
[ ! -f "${here}/nat-coverage.py" ] || cp "${here}/nat-coverage.py" "${suite}/3-smoke/"
cat > "${work}/repo/.build/run-ci" <<'STUB'
#!/bin/bash
if [ "${STUB_BUILD_SUCCESS:-0}" = 1 ]; then
    [ "${PYTHONUNBUFFERED:-0}" = 1 ] || exit 24
    echo 'Build number: 7'
    touch "${STUB_NEW_NODE}"
    exit 0
fi
exit 23
STUB
chmod +x "${work}/repo/.build/run-ci"
printf '#!/bin/bash\nexit 0\n' > "${suite}/3-smoke/check-pool-fit.py"
printf '#!/bin/bash\nexit "${STUB_SPEND_STATUS:-0}"\n' > "${suite}/spend-guard.py"
chmod +x "${suite}/3-smoke/check-pool-fit.py" "${suite}/spend-guard.py"
cat > "${work}/bin/gcloud" <<'STUB'
#!/bin/bash
case "$1 $2 $3" in
    'container clusters describe') printf 'RUNNING\t%s\tTrue\t%s\tdefault\tdefault\n' "${STUB_HTTP_DISABLED:-True}" "${STUB_PRIVATE:-False}" ;;
    'container node-pools list') printf 'agents-small-a\tTrue\t1\n' ;;
    'compute routers list') echo router ;;
    'compute routers nats') printf '%s' "${STUB_NATS}" ;;
    'functions describe guard') echo ACTIVE ;;
    'scheduler jobs describe') echo ENABLED ;;
    'alpha monitoring policies')
        [ "${STUB_POLICY_FAIL:-0}" = 0 ] || exit 1
        if [[ "$*" == *'value(name,enabled)'* ]]; then
            # gcloud's value formatter can expose BoolValue as a map.
            printf 'projects/test/alertPolicies/123\tvalue=True\n'
        else
            cat "${STUB_POLICY_FILE}"
        fi ;;
    *) echo "unexpected gcloud: $*" >> "${STUB_UNEXPECTED}"; exit 64 ;;
esac
STUB
cat > "${work}/bin/kubectl" <<'STUB'
#!/bin/bash
if [ "${1:-}" = --context ]; then
    [ "$2" = gke_test_europe-west9_ci ] || exit 64
    shift 2
fi
case "$*" in
    'config current-context') echo "${STUB_CONTEXT:-gke_test_europe-west9_ci}" ;;
    'get nodes -l cassandra.jenkins.agent=true -o name')
        echo node/existing
        [ ! -f "${STUB_NEW_NODE}" ] || echo node/new ;;
    'get nodes '*) echo 'controller True' ;;
    'top node '*) exit 0 ;;
    *'get ingress '*)
        [ "${STUB_INGRESS_FAIL:-0}" = 0 ] || exit 1
        cat "${STUB_INGRESS_FILE}" ;;
    *) echo "unexpected kubectl: $*" >> "${STUB_UNEXPECTED}"; exit 64 ;;
esac
STUB
chmod +x "${work}/bin/gcloud" "${work}/bin/kubectl"
export PATH="${work}/bin:${PATH}" STUB_UNEXPECTED="${work}/unexpected"
export STUB_NEW_NODE="${work}/new-node"
export STUB_POLICY_FILE="${work}/policy.json" STUB_INGRESS_FILE="${work}/ingress.json"
export GKE_CLUSTER_NAME=ci GOOGLE_PROJECT=test GOOGLE_REGION=europe-west9 GKE_LOCATION=europe-west9
export GKE_CONTROLLER_NODE_LABEL=cassandra.jenkins.controller=true GKE_AGENT_NODE_POOL_NAMES=agents-small-a
export GKE_AGENT_POOLS_FILE="${work}/pools.json" GKE_VCPU_QUOTA=500 GKE_VCPU_QUOTA_METRIC=CPUS
export GKE_VCPU_DEMAND=16 GKE_MAX_NODES_TOTAL=2 GKE_MAX_AGENT_NODES=1
export GKE_SPEND_CAPS_SECRET=caps GKE_SPEND_GUARD_FUNCTION=guard
export GKE_SPEND_GUARD_SCHEDULER_JOB=guard GKE_SPEND_STALLED_ALERT_POLICY='guard stalled'
export GKE_JENKINS_HOSTNAME=''
printf '%s' '{"items":[]}' > "${STUB_INGRESS_FILE}"
printf '%s' '[{"name":"projects/test/alertPolicies/123","enabled":{"value":true}}]' > "${STUB_POLICY_FILE}"
failures=0
run() {
    local want="$1" pattern="$2" status=0
    shift 2
    bash "${suite}/3-smoke/smoke-test.sh" "$@" > "${work}/output" 2>&1 || status=$?
    if [ "${status}" = "${want}" ] && rg -q "${pattern}" "${work}/output" && [ ! -s "${STUB_UNEXPECTED}" ]; then
        echo "PASS  ${pattern}"
    else
        echo "FAIL  ${pattern}: wanted exit ${want}, got ${status}"
        cat "${work}/output"
        if [ -s "${STUB_UNEXPECTED}" ]; then cat "${STUB_UNEXPECTED}"; fi
        failures=$((failures + 1))
    fi
}

run 0 'the alert.*exists and is enabled'
run 0 'no Jenkins Ingress requires the HTTP load balancing add-on'
printf '%s' '[{"name":"projects/test/alertPolicies/123","enabled":true}]' > "${STUB_POLICY_FILE}"
run 0 'the alert.*exists and is enabled'
printf '%s' '[{"name":"projects/test/alertPolicies/123","enabled":false}]' > "${STUB_POLICY_FILE}"
run 1 'the alert.*exists but is disabled'
printf '%s' '[{"name":"projects/test/alertPolicies/123","enabled":{"value":false}}]' > "${STUB_POLICY_FILE}"
run 1 'the alert.*exists but is disabled'
printf '%s' '[{"name":"projects/test/alertPolicies/123"}]' > "${STUB_POLICY_FILE}"
run 1 "the alert.*enabled state could not be read"
printf '%s' '[]' > "${STUB_POLICY_FILE}"
run 1 'the alert.*was not found'
STUB_POLICY_FAIL=1 run 1 'the alert.*could not be read'
printf '%s' '[{"name":"projects/test/alertPolicies/123","enabled":true}]' > "${STUB_POLICY_FILE}"
printf '%s' '{"items":[{"metadata":{"name":"cassius-jenkins"}}]}' > "${STUB_INGRESS_FILE}"
run 1 'the HTTP load balancing add-on is disabled.*Ingress'
STUB_HTTP_DISABLED=False run 0 'the HTTP load balancing add-on is enabled'
STUB_INGRESS_FAIL=1 run 1 'Jenkins Ingress resources could not be read'
printf '%s' '{"items":[{"metadata":{"annotations":{"kubernetes.io/ingress.class":"nginx"}}}]}' > "${STUB_INGRESS_FILE}"
run 0 'no Jenkins Ingress requires the HTTP load balancing add-on'
printf '%s' '{"items":[]}' > "${STUB_INGRESS_FILE}"
STUB_SPEND_STATUS=2 run 1 'This local report uses your credentials'

STUB_CONTEXT=gke_other_europe-west9_ci run 1 "does not name"
STUB_PRIVATE=True STUB_NATS='[{"name":"nat","sourceSubnetworkIpRangesToNat":"LIST_OF_SUBNETWORKS","subnetworks":[{"name":"projects/test/regions/europe-west9/subnetworks/default-other","sourceIpRangesToNat":["ALL_IP_RANGES"]}]}]' run 1 'no NAT covering default'
STUB_PRIVATE=True STUB_NATS='[{"name":"nat","sourceSubnetworkIpRangesToNat":"LIST_OF_SUBNETWORKS","subnetworks":[{"name":"projects/test/regions/europe-west9/subnetworks/default","sourceIpRangesToNat":["LIST_OF_SECONDARY_IP_RANGES"]}]}]' run 1 'no NAT covering default'
STUB_PRIVATE=True STUB_NATS='[{"name":"nat","sourceSubnetworkIpRangesToNat":"LIST_OF_SUBNETWORKS","subnetworks":[{"name":"projects/test/regions/europe-west9/subnetworks/default","sourceIpRangesToNat":["PRIMARY_IP_RANGE"]}]}]' run 0 'a Cloud NAT covers default'
run 1 'run-ci exited|no new agent node' --build --build-timeout 2
STUB_BUILD_SUCCESS=1 run 0 'a build was submitted and an agent node appeared' --build --build-timeout 3
[ "${failures}" -eq 0 ]
