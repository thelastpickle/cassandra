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

set -euo pipefail

here="$(cd -- "$(dirname -- "$0")" && pwd)"
work="$(mktemp -d "${TMPDIR:-/tmp}/cassandra-eks-pool-sizing-test.XXXXXX")"
trap 'rm -rf "${work}"' EXIT

# Evaluate the production allocation expressions against fixed quota and address inputs.
cp "${here}/1-cluster/variables.tf" "${here}/1-cluster/pool-sizing.tf" \
    "${here}/tests/pool-sizing-fixture.tf" "${here}/tests/pool-sizing.tftest.hcl" "${work}/"
"${TOFU:-tofu}" -chdir="${work}" init -backend=false -input=false >/dev/null
"${TOFU:-tofu}" -chdir="${work}" test -no-color
