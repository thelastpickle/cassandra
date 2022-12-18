#!/bin/bash -e
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
# Expected to be run from inside cassandra-builds/docker/bullseye-image.docker
#

################################
#
# Prep
#
################################

# variables, with defaults
[ "x${CASSANDRA_DIR}" != "x" ] || { CASSANDRA_DIR="$(pwd)"; }
[ -d "${CASSANDRA_DIR}" ] || { echo >&2 "Directory ${CASSANDRA_DIR} must exist"; exit 1; }
[ -d "${DEB_DIST_DIR}" ] || { echo >&2 "Directory ${DEB_DIST_DIR} must exist"; exit 1; }

# pre-conditions
command -v ant >/dev/null 2>&1 || { echo >&2 "ant needs to be installed"; exit 1; }
command -v pip >/dev/null 2>&1 || { echo >&2 "pip needs to be installed"; exit 1; }
[ -f "${CASSANDRA_DIR}/build.xml" ] || { echo >&2 "${CASSANDRA_DIR}/build.xml must exist"; exit 1; }

java_version_default=`grep 'property\s*name="java.default"' ${CASSANDRA_DIR}/build.xml |sed -ne 's/.*value="\([^"]*\)".*/\1/p'`
java_version_supported=`grep 'property\s*name="java.supported"' ${CASSANDRA_DIR}/build.xml |sed -ne 's/.*value="\([^"]*\)".*/\1/p'`

if [ "$1" == "-h" ]; then
   echo "$0 [-h] [<java version>]"
   echo "if Java version is not set, it is set to ${java_version_default} by default, valid ${java_version_supported}"
   exit 1
fi

JAVA_VERSION=$1

[ "x${JAVA_VERSION}" != "x" ] || JAVA_VERSION="${java_version_default}"
regx_java_version="(${java_version_supported/,/|})"
if [[ ! "$JAVA_VERSION" =~ $regx_java_version ]]; then
   echo "Error: Java version is not in ${java_version_supported}, it is set to $JAVA_VERSION"
   exit 1
fi

sudo update-java-alternatives --set java-1.${JAVA_VERSION}.0-openjdk-$(dpkg --print-architecture)
echo "Cassandra will be built with Java ${JAVA_VERSION}"

# print debug information on versions
ant -version
pip --version
java -version
javac -version


ARTIFACTS_BUILD_RUN=0
ECLIPSE_WARNINGS_RUN=0

#HAS_DEPENDENCY_CHECK_TARGET=$(ant -p build.xml | grep "dependency-check " | wc -l)
HAS_DEPENDENCY_CHECK_TARGET=0
# versions starting from 6.4.1 contain "rate limiter" functionality to make builds more stable
# https://github.com/jeremylong/DependencyCheck/pull/3725
DEPENDENCY_CHECK_VERSION=6.4.1

################################
#
# Main
#
################################

pushd "${CASSANDRA_DIR}/" > /dev/null

# Pre-download dependencies, loop to prevent failures
set +e
for x in $(seq 1 3); do
    ant realclean clean resolver-dist-lib
    RETURN="$?"
    if [ "${RETURN}" -eq "0" ]; then break ; fi
    sleep 3
done

for x in $(seq 1 3); do
    if [ "${ARTIFACTS_BUILD_RUN}" -eq "0" ]; then
      ant clean artifacts
      RETURN="$?"
    fi
    if [ "${RETURN}" -eq "0" ]; then
        ARTIFACTS_BUILD_RUN=1
        if [ "${ECLIPSE_WARNINGS_RUN}" -eq "0" ]; then
          # Run eclipse-warnings if build was successful
          ant eclipse-warnings
          RETURN="$?"
        fi
        if [ "${RETURN}" -eq "0" ]; then
            ECLIPSE_WARNINGS_RUN=1
            if [ "${HAS_DEPENDENCY_CHECK_TARGET}" -eq "1" ]; then
                ant -Ddependency-check.version=${DEPENDENCY_CHECK_VERSION} -Ddependency-check.home=/tmp/dependency-check-${DEPENDENCY_CHECK_VERSION} dependency-check
                RETURN="$?"
            else
                RETURN="0"
            fi
            if [ ! "${RETURN}" -eq "0" ]; then
                if [ -f /tmp/dependency-check-${DEPENDENCY_CHECK_VERSION}/dependency-check-ant/dependency-check-ant.jar ]; then
                    # Break the build here only in case dep zip was downloaded (hence JAR was extracted) just fine
                    # but the check itself has failed. If JAR does not exist, it is probably
                    # because the network was down so the ant target did not download the zip in the first place.
                    echo "Failing the build on OWASP dependency check. Run 'ant dependency-check' locally and consult build/dependency-check-report.html to see the details."
                    break
                else
                    # sleep here to give the net the chance to resume after probable partition
                    sleep 10
                    continue
                fi
            fi
            set -e
        fi
        break
    fi
    # sleep here to give the net the chance to resume after probable partition
    sleep 10
done
popd > /dev/null

set -x
exit "${RETURN}"
