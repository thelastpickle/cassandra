#!/usr/bin/env bash
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
# Wrapper script for running a split chunk of an ant test target
#
# Usage: test.sh target [split_chunk]
#  split_chunk formatted as "K/N" for the Kth chunk of N chunks

set -o xtrace
set -o errexit
set -o pipefail
set -o nounset

# pre-conditions
command -v ant >/dev/null 2>&1 || { echo >&2 "ant needs to be installed"; exit 1; }
command -v git >/dev/null 2>&1 || { echo >&2 "git needs to be installed"; exit 1; }
[ -f "build.xml" ] || { echo >&2 "build.xml must exist in $(pwd)"; exit 1; }

# print debug information on versions
ant -version
git --version
pip --version
java -version
javac -version

# lists all tests for the specific test type
_list_tests() {
  local -r classlistprefix="$1"
  find "test/$classlistprefix" -name '*Test.java' | sed "s;^test/$classlistprefix/;;g" | sort
}

_split_tests() {
  local -r split_chunk="$1"
  split_cmd=split
  ( split --help 2>&1 ) | grep -q "r/K/N" || split_cmd=gsplit
  command -v ${split_cmd} >/dev/null 2>&1 || { echo >&2 "${split_cmd} needs to be installed"; exit 1; }
  if [[ "x" == "x${split_chunk}" ]] ; then
    ${split_cmd} -n r/1/1
  else
    ${split_cmd} -n r/${split_chunk}
  fi
}

_timeout_for() {
  grep "name=\"$1\"" build.xml | awk -F'"' '{print $4}'
}

_build_all_dtest_jars() {
    mkdir -p build
    cd $TMP_DIR
    until git clone --quiet --depth 1 --no-single-branch https://github.com/apache/cassandra.git cassandra-dtest-jars ; do echo "git clone failed… trying again… " ; done
    cd cassandra-dtest-jars
    for branch in cassandra-2.2 cassandra-3.0 cassandra-3.11 cassandra-4.0 cassandra-4.1 trunk; do
        git checkout $branch
        ant realclean
        ant jar dtest-jar ${ANT_BUILD_OPTS}
        cp build/dtest*.jar ../../build/
    done
    cd ../..
    rm -fR ${TMP_DIR}/cassandra-dtest-jars
    ant clean dtest-jar ${ANT_BUILD_OPTS}
    ls -l build/dtest*.jar
}

_run_testlist() {
    local _target_prefix=$1
    local _testlist_target=$2
    local _split_chunk=$3
    local _test_timeout=$4
    testlist="$( _list_tests "${_target_prefix}" | _split_tests "${_split_chunk}")"
    if [[ -z "$testlist" ]]; then
      # something has to run in the split to generate a junit xml result
      echo Hacking ${_target_prefix} ${_testlist_target} to run only first test found as no tests in split ${_split_chunk} were found
      testlist="$( _list_tests "${_target_prefix}" | head -n1)"
    fi
    ant $_testlist_target -Dtest.classlistprefix="${_target_prefix}" -Dtest.classlistfile=<(echo "${testlist}") -Dtest.timeout="${_test_timeout}" ${ANT_TEST_OPTS} || echo "failed ${_target_prefix} ${_testlist_target}"
}

_main() {
  # parameters
  local -r target="${1:-}"
  local -r split_chunk="${2:-}" # Optional: pass in chunk to test, formatted as "K/N" for the Kth chunk of N chunks

  # check split_chunk is compatible with target
  if [[ "x" != "x${split_chunk}" ]] && [[ "1/1" != "${split_chunk}" ]] ; then
    case ${target} in
      "stress-test" | "fqltool-test" | "microbench" | "cqlsh-test")
          echo "Target ${target} does not suport splits."
          exit 1
          ;;
        *)
          ;;
    esac
  fi

  # jdk check
  local -r java_version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | awk -F. '{print $1}')
  local -r version=$(grep 'property\s*name=\"base.version\"' build.xml |sed -ne 's/.*value=\"\([^"]*\)\".*/\1/p')
  local -r java_version_default=`grep 'property\s*name="java.default"' build.xml |sed -ne 's/.*value="\([^"]*\)".*/\1/p'`

  if [ "${java_version}" -ne "${java_version_default}" ] && [[ "${target}" == "jvm-dtest-upgrade" ]] ; then
    echo "Invalid JDK${java_version} execution. Only the default and overlapping supported JDK (JDK${java_version_default}) can be used when upgrading."
    exit 1
  fi

  # check project is already built
  [[ -f "build/apache-cassandra-${version}.jar" ]] || { echo "Project must be built first. Use \`ant jar\`"; exit 1; }

  # check test target exists in code
  case ${target} in
    "stress-test" | "fqltool-test")
      ant -projecthelp | grep -q " $target " || { echo "Skipping ${target}. It does not exist in ${version}"; exit 0; }
      ;;
    "test-cdc")
      regx_version="(2.2|3.0).([0-9]+)$"
      ! [[ $version =~ $regx_version ]] || { echo "Skipping ${target}. It does not exist in ${version}"; exit 0; }
      ;;
    "cqlsh-test")
      [[ -f "./pylib/cassandra-cqlsh-tests.sh" ]] || { echo "Skipping ${target}. It does not exist in ${version}"; exit 0; }
      ;;
    *)
      ;;
  esac

  # ant test setup
  export TMP_DIR="$(pwd)/tmp"
  mkdir -p ${TMP_DIR}
  export ANT_TEST_OPTS="-Dno-build-test=true -Dtmp.dir=\"${TMP_DIR}\""
  export ANT_BUILD_OPTS="${ANT_TEST_OPTS} -Drat.skip=true -Dno-checkstyle=true -Dno-javadoc=true -Dant.gen-doc.skip=true"

  case ${target} in
    "stress-test")
      # hard fail on test compilation, but dont fail the test run as unstable test reports are processed
      ant stress-build-test ${ANT_BUILD_OPTS}
      ant $target ${ANT_TEST_OPTS} || echo "failed $target"
      ;;
    "fqltool-test")
      # hard fail on test compilation, but dont fail the test run so unstable test reports are processed
      ant fqltool-build-test ${ANT_BUILD_OPTS}
      ant $target ${ANT_TEST_OPTS} || echo "failed $target"
      ;;
    "microbench")
      ant $target ${ANT_TEST_OPTS} -Dmaven.test.failure.ignore=true
      ;;
    "test")
      _run_testlist "unit" "testclasslist" "${split_chunk}" "$(_timeout_for 'test.timeout')"
      ;;
    "test-cdc")
      _run_testlist "unit" "testclasslist-cdc" "${split_chunk}" "$(_timeout_for 'test.timeout')"
      ;;
    "test-compression")
      _run_testlist "unit" "testclasslist-compression" "${split_chunk}" "$(_timeout_for 'test.timeout')"
      ;;
    "test-burn")
      _run_testlist "burn" "testclasslist" "${split_chunk}" "$(_timeout_for 'test.burn.timeout')"
      ;;
    "long-test")
      _run_testlist "long" "testclasslist" "${split_chunk}" "$(_timeout_for 'test.long.timeout')"
      ;;
    "jvm-dtest")
      testlist=$( _list_tests "distributed" | grep -v "upgrade" | _split_tests "${split_chunk}")
      if [[ -z "$testlist" ]]; then
          # something has to run in the split to generate a junit xml result
          echo Hacking jvm-dtest to run only first test found as no tests in split ${split_chunk} were found
          testlist="$( _list_tests "distributed"  | grep -v "upgrade" | head -n1)"
      fi
      ant testclasslist -Dtest.classlistprefix=distributed -Dtest.timeout=$(_timeout_for "test.distributed.timeout") -Dtest.classlistfile=<(echo "${testlist}") ${ANT_TEST_OPTS} || echo "failed ${target}"
      ;;
    "jvm-dtest-upgrade")
      _build_all_dtest_jars
      testlist=$( _list_tests "distributed"  | grep "upgrade" | _split_tests "${split_chunk}")
      if [[ -z "$testlist" ]]; then
          # something has to run in the split to generate a junit xml result
          echo Hacking jvm-dtest-upgrade to run only first test found as no tests in split ${split_chunk} were found
          testlist="$( _list_tests "distributed"  | grep "upgrade" | head -n1)"
      fi
      ant testclasslist -Dtest.classlistprefix=distributed -Dtest.timeout=$(_timeout_for "test.distributed.timeout") -Dtest.classlistfile=<(echo "${testlist}") ${ANT_TEST_OPTS} || echo "failed ${target}"
      ;;
    "cqlsh-test")
      ./pylib/cassandra-cqlsh-tests.sh $(pwd)
      ;;
    *)
      echo "unregconised \"$target\""
      exit 1
      ;;
  esac
}

_main "$@"
