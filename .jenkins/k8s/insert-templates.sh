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
#
# Script to insert includes files into jenkins-deployment-template.yaml as jenkins-deployment.yaml 

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUT="$SCRIPT_DIR/jenkins-deployment-template.yaml"
OUTPUT="$SCRIPT_DIR/jenkins-deployment.yaml"

clean() { 
    sed "/^#/d;" 
}

# Read template files
POD_TEMPLATE=$(cat "$SCRIPT_DIR/includes/pod-template-base.yaml"     | clean )
JNLP_CONTAINER=$(cat "$SCRIPT_DIR/includes/jnlp-container-base.yaml" | clean )
DIND_CONTAINER=$(cat "$SCRIPT_DIR/includes/dind-container-base.yaml" | clean )
VOLUMES=$(cat "$SCRIPT_DIR/includes/volumes-base.yaml"               | clean )

# Replace anchor references
awk -v pod="$POD_TEMPLATE" -v jnlp="$JNLP_CONTAINER" -v dind="$DIND_CONTAINER" -v vols="$VOLUMES" '
{
    line = $0
    gsub(/        <<: \*pod-template-base/, pod, line)
    gsub(/          <<: \*jnlp-container-base/, jnlp, line)
    gsub(/          <<: \*dind-container-base/, dind, line)
    gsub(/volumes: \*volumes-base/, "volumes: " vols, line)
    print line
}
' "$INPUT" > "$OUTPUT"
