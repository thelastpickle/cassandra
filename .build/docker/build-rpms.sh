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

# Expected to be run from inside cassandra-builds/docker/almalinux-image.docker
set -e

[ "x${CASSANDRA_DIR}" != "x" ] || { CASSANDRA_DIR="$(pwd)"; }
[ -d "${CASSANDRA_DIR}" ] || { echo >&2 "Directory ${CASSANDRA_DIR} must exist"; exit 1; }
[ "x${RPM_BUILD_DIR}" != "x" ] || { echo >&2 "RPM_BUILD_DIR needs to be defined"; exit 1; }
[ -d "${RPM_BUILD_DIR}/SOURCES" ] || { echo >&2 "Directory ${RPM_BUILD_DIR}/SOURCES must exist"; exit 1; }
[ "x${RPM_DIST_DIR}" != "x" ] || { echo >&2 "RPM_DIST_DIR needs to be defined"; exit 1; }
[ -d "${RPM_DIST_DIR}" ] || { echo >&2 "Directory ${RPM_DIST_DIR} must exist"; exit 1; }

java_version_default=`grep 'property\s*name="java.default"' ${CASSANDRA_DIR}/build.xml |sed -ne 's/.*value="\([^"]*\)".*/\1/p'`
java_version_supported=`grep 'property\s*name="java.supported"' ${CASSANDRA_DIR}/build.xml |sed -ne 's/.*value="\([^"]*\)".*/\1/p'`

if [ "$1" == "-h" ]; then
   echo "$0 [-h] [<java version>] [dist_type]"
   echo "if Java version is not set, it is set to 11 by default, choose from 11 or 17"
   echo "dist types are [rpm, noboolean] and rpm is default"
   exit 1
fi

JAVA_VERSION=$1
RPM_DIST=$2

[ "x${JAVA_VERSION}" != "x" ] || JAVA_VERSION="${java_version_default}"
regx_java_version="(${java_version_supported/,/|})"
if [[ ! "$JAVA_VERSION" =~ $regx_java_version ]]; then
   echo "Error: Java version is not in ${java_version_supported}, it is set to $JAVA_VERSION"
   exit 1
fi

[ "x${RPM_DIST}" != "x" ] || RPM_DIST="rpm"
if [ "${RPM_DIST}" == "rpm" ]; then
    RPM_SPEC="redhat/cassandra.spec"
else # noboolean
    RPM_SPEC="redhat/noboolean/cassandra.spec"
fi

# note, this edits files in your working cassandra directory
cd $CASSANDRA_DIR

# Used version for build will always depend on the git referenced used for checkout above
# Branches will always be created as snapshots, while tags are releases
tag=`git describe --tags --exact-match` 2> /dev/null || true
branch=`git symbolic-ref -q --short HEAD` 2> /dev/null || true

is_tag=false
git_version=''

# Parse version from build.xml so we can verify version against release tags and use the build.xml version
# for any branches. Truncate from snapshot suffix if needed.
buildxml_version=`grep 'property\s*name="base.version"' build.xml |sed -ne 's/.*value="\([^"]*\)".*/\1/p'`
regx_snapshot="([0-9.]+)-SNAPSHOT$"
if [[ $buildxml_version =~ $regx_snapshot ]]; then
   buildxml_version=${BASH_REMATCH[1]}
fi

if [ "$tag" ]; then
   is_tag=true
   # Official release
   regx_tag="cassandra-(([0-9.]+)(-(alpha|beta|rc)[0-9]+)?)$"
   # Tentative release
   regx_tag_tentative="(([0-9.]+)(-(alpha|beta|rc)[0-9]+)?)-tentative$"
   if [[ $tag =~ $regx_tag ]] || [[ $tag =~ $regx_tag_tentative ]]; then
      git_version=${BASH_REMATCH[1]}
   else
      echo "Error: could not recognize version from tag $tag">&2
      exit 2
   fi
   if [ $buildxml_version != $git_version ]; then
      echo "Error: build.xml version ($buildxml_version) not matching git tag derived version ($git_version)">&2
      exit 4
   fi
   CASSANDRA_VERSION=$git_version
   CASSANDRA_REVISION='1'
else
   # This could be either trunk or any dev branch or SHA, so we won't be able to get the version
   # from the branch name. In this case, fall back to version specified in build.xml.
   CASSANDRA_VERSION="${buildxml_version}"
   dt=`date +"%Y%m%d"`
   ref=`git rev-parse --short HEAD`
   CASSANDRA_REVISION="${dt}git${ref}"
fi

sudo alternatives --set java $(alternatives --display java | grep "family java-${JAVA_VERSION}-openjdk" | cut -d' ' -f1)
sudo alternatives --set javac $(alternatives --display javac | grep "family java-${JAVA_VERSION}-openjdk" | cut -d' ' -f1)
echo "Cassandra will be built with Java ${JAVA_VERSION}"
export JAVA_HOME=$(readlink -f /usr/bin/javac | sed "s:/bin/javac::")

java -version
javac -version

# Pre-download dependencies, loop to prevent failures
set +e
for x in $(seq 1 3); do
    ant realclean clean resolver-dist-lib
    RETURN="$?"
    if [ "${RETURN}" -eq "0" ]; then break ; fi
    sleep 3
done
set -e

# javadoc target is broken in docker without this mkdir
mkdir -p ./build/javadoc
# Artifact will only be used internally for build process and won't be found with snapshot suffix
ant artifacts -Drelease=true
cp ./build/apache-cassandra-*-src.tar.gz ${RPM_BUILD_DIR}/SOURCES/

# if CASSANDRA_VERSION is -alphaN, -betaN, -rcN, then rpmbuild fails on the '-' char; replace with '~'
CASSANDRA_VERSION=${CASSANDRA_VERSION/-/\~}

command -v python >/dev/null 2>&1 || sudo ln -s /usr/bin/python3 /usr/bin/python
rpmbuild --define="version ${CASSANDRA_VERSION}" --define="revision ${CASSANDRA_REVISION}" -ba ${RPM_SPEC}
cp $RPM_BUILD_DIR/SRPMS/*.rpm $RPM_BUILD_DIR/RPMS/noarch/*.rpm $RPM_DIST_DIR
