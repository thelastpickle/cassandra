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
regx_java_version="(${java_version_supported//,/|})"
if [[ ! "$JAVA_VERSION" =~ $regx_java_version ]]; then
   echo "Error: Java version is not in ${java_version_supported}, it is set to $JAVA_VERSION"
   exit 1
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
   CASSANDRA_VERSION=$git_version
   CASSANDRA_REVISION='1'
else
   regx_branch="cassandra-([0-9.]+)$"
   if [[ $branch =~ $regx_branch ]]; then
      git_version=${BASH_REMATCH[1]}
   else
      # This could be either trunk or any dev branch or SHA, so we won't be able to get the version
      # from the branch name. In this case, fall back to debian change log version.
      git_version=$(dpkg-parsechangelog | sed -ne 's/^Version: \(.*\).*/\1/p' | sed 's/~/-/')
      if [ -z $git_version ]; then
         echo "Error: could not recognize version from branch $branch">&2
         exit 2
      else
         echo "Warning: could not recognize version from branch. dpkg version is $git_version"
      fi
   fi
    # if CASSANDRA_VERSION is -alphaN, -betaN, -rcN, then rpmbuild fails on the '-' char; replace with '~'
    CASSANDRA_VERSION=${buildxml_version/-/\~}
    dt=`date +"%Y%m%d"`
    ref=`git rev-parse --short HEAD`
    CASSANDRA_REVISION="${dt}git${ref}"
    dch -D unstable -v "${CASSANDRA_VERSION}-${CASSANDRA_REVISION}" --package "cassandra" "TODO msg"
fi

# The version used for the deb build process will the current version in the debian/changelog file.
# See debian/rules for how the value is read. The only thing left for us to do here is to check if
# the changes file contains the correct version for the checked out git revision. The version value
# has to be updated manually by a committer and we only warn and abort on mismatches here.
changelog_version=$(dpkg-parsechangelog | sed -ne 's/^Version: \(.*\).*/\1/p' | sed 's/~/-/')
chl_expected="${buildxml_version}"
if [[ ! $changelog_version =~ $chl_expected ]]; then
   echo "Error: changelog version (${changelog_version}) doesn't match expected (${chl_expected})">&2
   exit 3
fi

# Version (base.version) in build.xml must be set manually as well. Let's validate the set value.
if [ $buildxml_version != $git_version ]; then
   echo "Warning: build.xml version ($buildxml_version) not matching git/dpkg derived version ($git_version)">&2
fi

sudo update-java-alternatives --set java-1.${JAVA_VERSION}.0-openjdk-$(dpkg --print-architecture)
export JAVA_HOME=$(readlink -f /usr/bin/javac | sed "s:/bin/javac::")
echo "Cassandra will be built with Java ${JAVA_VERSION}"

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

# Install build dependencies (retry if failed)
until ( echo "y" | sudo mk-build-deps --install ) ; do echo "mk-build-deps failed… trying again after 10s… " ; sleep 10 ; done

# build package
dpkg-buildpackage -rfakeroot -uc -us -tc

# Copy created artifacts to dist dir mapped to docker host directory (must have proper permissions)
mv ../cassandra[-_]* $DEB_DIST_DIR
