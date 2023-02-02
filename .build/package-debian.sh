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
# Builds (inside docker) the deb packages
#
# Called from build-scripts/cassandra-artifacts.sh or cassandra-release/prepare_release.sh

################################
#
# Prep
#
################################

# variables, with defaults
[ "x${cassandra_dir}" != "x" ] || cassandra_dir="$(pwd)"
[ "x${deb_dir}" != "x" ] || deb_dir="$(mktemp -d /tmp/cassandra-package-deb.XXXXXXXXXX)"

java_version_default=`grep 'property\s*name="java.default"' ${cassandra_dir}/build.xml |sed -ne 's/.*value="\([^"]*\)".*/\1/p'`
java_version_supported=`grep 'property\s*name="java.supported"' ${cassandra_dir}/build.xml |sed -ne 's/.*value="\([^"]*\)".*/\1/p'`

if [ "$1" == "-h" ]; then
   echo "$0 <java version>"
   echo "if Java version is not set, it is set to ${java_version_default} by default, choose from ${java_version_supported}"
   exit 1
fi

java_version=$1

# pre-conditions
command -v docker >/dev/null 2>&1 || { echo >&2 "docker needs to be installed"; exit 1; }
(docker info >/dev/null 2>&1) || { echo >&2 "docker needs to running"; exit 1; }
[ -f "${cassandra_dir}/build.xml" ] || { echo >&2 "${cassandra_dir}/build.xml must exist"; exit 1; }
[ -f "${cassandra_dir}/.build/docker/bullseye-image.docker" ] || { echo >&2 "${cassandra_dir}/.build/docker/bullseye-image.docker must exist"; exit 1; }
[ -f "${cassandra_dir}/.build/docker/build-debs.sh" ] || { echo >&2 "${cassandra_dir}/.build/docker/build-debs.sh must exist"; exit 1; }

[ "x${java_version}" != "x" ] || java_version="${java_version_default}"
regx_java_version="(${java_version_supported//,/|})"
if [[ ! "${java_version}" =~ $regx_java_version ]]; then
   echo "Error: Java version is not in ${java_version_supported}, it is set to ${java_version}"
   exit 1
fi

# print debug information on versions
docker --version

# remove any older built images from previous days
docker image prune --all --force --filter label=org.cassandra.buildenv=bullseye --filter "until=24h" || true

################################
#
# Main
#
################################

echo
echo "==="
echo "WARNING: this script modifies local versioned files"
echo "==="
echo

mkdir -p "${deb_dir}"
pushd ${cassandra_dir}/.build

image_tag="$(md5sum docker/bullseye-image.docker | cut -d' ' -f1)"

# Create build images containing the build tool-chain, Java and an Apache Cassandra git working directory, with retry
until docker build --build-arg UID_ARG=`id -u` --build-arg GID_ARG=`id -g` -t cassandra-packaging-bullseye:${image_tag} -f docker/bullseye-image.docker docker/  ; do echo "docker build failed… trying again in 10s… " ; sleep 10 ; done

# Run build script through docker
docker run --rm -v "${cassandra_dir}:/home/build/cassandra" -v "${deb_dir}":/dist -v ~/.m2/repository/:/home/build/.m2/repository/ cassandra-packaging-bullseye:${image_tag} /home/build/build-debs.sh ${java_version}

mkdir -p ${cassandra_dir}/build/packages/deb
cp -r ${deb_dir}/* ${cassandra_dir}/build/packages/deb
popd
