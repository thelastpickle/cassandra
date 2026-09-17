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
# Read regional and global quotas for data.external.quotas. Diagnostics go to stderr.
# Usage is reported separately; plan-time sizing reserves headroom from limits.

set -o nounset
set -o pipefail

fail() {
    echo "read-quotas.sh: $1" >&2
    exit 1
}

command -v gcloud >/dev/null 2>&1 || fail "gcloud is not on the PATH.
    Reading quotas needs it, and this is the only part of 1-cluster that does.  Either install the Google
    Cloud CLI and run \`gcloud auth application-default login\`, or set size_pools_to_quotas = false in
    terraform.tfvars, which uses the declared pool sizes as written."

command -v python3 >/dev/null 2>&1 || fail "python3 is not on the PATH."

query="$(cat)"

project="$(printf '%s' "${query}" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("project",""))')"
region="$(printf '%s' "${query}" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("region",""))')"

[ -n "${project}" ] || fail "no project in the query"
[ -n "${region}" ] || fail "no region in the query"

regional="$(gcloud compute regions describe "${region}" --project "${project}" --format=json 2>&1)" \
    || fail "gcloud could not describe region ${region} in project ${project}.  Check the credential first:
    gcloud auth list
    gcloud services enable compute.googleapis.com --project ${project}

gcloud said:
${regional}"

# A failed global read must not silently discard CPUS_ALL_REGIONS.
global="$(gcloud compute project-info describe --project "${project}" --format=json 2>&1)" \
    || fail "gcloud could not describe project ${project}.

gcloud said:
${global}"

REGIONAL_JSON="${regional}" GLOBAL_JSON="${global}" python3 <<'PY'
import json
import os
import sys


def as_string(value):
    """The external protocol requires string values; missing figures stay empty."""
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


answer = {}

# Regional values take precedence if a metric appears in both scopes.
for source in ("REGIONAL_JSON", "GLOBAL_JSON"):
    try:
        document = json.loads(os.environ[source])
    except ValueError as error:
        sys.exit("read-quotas.sh: %s was not JSON: %s" % (source, error))

    for quota in document.get("quotas") or []:
        metric = quota.get("metric")
        if not metric or metric in answer:
            continue
        answer[metric] = as_string(quota.get("limit"))
        answer["usage_" + metric] = as_string(quota.get("usage"))

if not answer:
    sys.exit("read-quotas.sh: neither the region nor the project reported any quota, which means the answer"
             " was read but held nothing.  Sizing the pools against that would read as no ceiling at all.")

json.dump(answer, sys.stdout)
PY
