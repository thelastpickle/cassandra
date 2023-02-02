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
#
# A wrapper script to test.sh (or dtest-python.sh)
#  that splits the test list into multiple docker runs, collecting results.
#
# Each split chunk may be further parallelised over docker containers based on the host's available cpu and memory.
#
# The docker image used is normally based from those found in .build/docker/
# If in doubt use 'apache/cassandra-testing-ubuntu2004-java11-w-dependencies'
#
# Usage: test-docker.sh docker_image target split_chunk
#

if [ "$#" -lt 3 ]; then
    # inside the docker container, setup env before calling cassandra-test.sh
    export WORKSPACE=/home/cassandra/cassandra
    export LANG=en_US.UTF-8
    export LC_CTYPE=en_US.UTF-8
    export PYTHONIOENCODING=utf-8
    export PYTHONUNBUFFERED=true

    sudo update-java-alternatives --set java-1.${JAVA_VERSION}.0-openjdk-$(dpkg --print-architecture)
    export JAVA_HOME=$(readlink -f /usr/bin/javac | sed "s:/bin/javac::")
    cd ${WORKSPACE}
    .build/${TEST_SCRIPT} "$@"
    status=$?
    if [ -d build/test/logs ]; then find build/test/logs -type f -name "*.log" | xargs xz -qq ; fi
    xz -f test_stdout.txt
    exit ${status}
else

    # variables, with defaults
    [ "x${cassandra_dir}" != "x" ] || cassandra_dir="$(pwd)"
    [ "x${cassandra_dtest_dir}" != "x" ] || cassandra_dtest_dir="${cassandra_dir}/../cassandra-dtest"

    # pre-conditions
    command -v docker >/dev/null 2>&1 || { echo >&2 "docker needs to be installed"; exit 1; }
    (docker info >/dev/null 2>&1) || { echo >&2 "docker needs to running"; exit 1; }
    [ -f "${cassandra_dir}/build.xml" ] || { echo >&2 "${cassandra_dir}/build.xml must exist"; exit 1; }
    [ -f "${cassandra_dir}/.build/test.sh" ] || { echo >&2 "${cassandra_dir}/.build/test.sh must exist"; exit 1; }

    # print debug information on versions
    docker --version
    # help
    if [ "$#" -lt 3 ] || [ "$1" == "-h" ]; then
       echo "Usage: test-docker.sh docker_image target split_chunk"
       exit 1
    fi
    # defaults
    docker_image=$1
    target=$2
    split_chunk=$3
    test_script="test.sh"
    docker_mounts="-v ${cassandra_dir}:/home/cassandra/cassandra -v ${HOME}/.m2/repository:/home/cassandra/.m2/repository"


    # Setup JDK
    java_version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | awk -F. '{print $1}')
    java_version_supported=`grep 'property\s*name="java.supported"' ${cassandra_dir}/build.xml |sed -ne 's/.*value="\([^"]*\)".*/\1/p'`
    regx_java_version="(${java_version_supported//,/|})"
    if [[ ! "${java_version}" =~ $regx_java_version ]]; then
        echo "Error: Java version is not in ${java_version_supported}, it is set to ${java_version}"
        exit 1
    fi

    # Jenkins agents run multiple executors per machine. `jenkins_executors=1` is used for anything non-jenkins.
    jenkins_executors=1
    if [[ ! -z ${JENKINS_URL+x} ]] && [[ ! -z ${NODE_NAME+x} ]] ; then
        fetched_jenkins_executors=$(curl -s --retry 9 --retry-connrefused --retry-delay 1 "${JENKINS_URL}/computer/${NODE_NAME}/api/json?pretty=true" | grep 'numExecutors' | awk -F' : ' '{print $2}' | cut -d',' -f1)
        # use it if we got a valid number (despite retry settings the curl above can still fail
        [[ ${fetched_jenkins_executors} =~ '^[0-9]+$' ]] && jenkins_executors=${fetched_jenkins_executors}
    fi

    # find host's available cores and mem
    cores=1
    command -v nproc >/dev/null 2>&1 && cores=$(nproc --all)
    mem=1
    # linux
    command -v free >/dev/null 2>&1 && mem=$(free -b | grep Mem: | awk '{print $2}')
    # macos
    sysctl -n hw.memsize >/dev/null 2>&1 && mem=$(sysctl -n hw.memsize)

    # for relevant test targets calculate how many docker containers we should split the test list over
    case ${target} in
      # test-burn doesn't have enough tests in it to split beyond 8, and burn and long we want a bit more resources anyway
      "stress-test" | "fqltool-test" | "microbench" | "test-burn" | "long-test" | "cqlsh-test" )
          [[ ${mem} -gt $((5 * 1024 * 1024 * 1024 * ${jenkins_executors})) ]] || { echo >&2 "tests require minimum docker memory 6g (per jenkins executor (${jenkins_executors})), found ${mem}"; exit 1; }
          docker_runs=1
        ;;
      "dtest" | "dtest-novnode" | "dtest-offheap" | "dtest-large" | "dtest-large-novnode" | "dtest-upgrade" )
          [ -f "${cassandra_dtest_dir}/dtest.py" ] || { echo >&2 "${cassandra_dtest_dir}/dtest.py must exist"; exit 1; }
          [[ ${mem} -gt $((15 * 1024 * 1024 * 1024 * ${jenkins_executors})) ]] || { echo >&2 "dtests require minimum docker memory 16g (per jenkins executor (${jenkins_executors})), found ${mem}"; exit 1; }
          docker_runs=1
          test_script="dtest-python.sh"
          docker_mounts="${docker_mounts} -v ${cassandra_dtest_dir}:/home/cassandra/cassandra-dtest"
        ;;
      "test"| "test-cdc" | "test-compression" | "jvm-dtest" | "jvm-dtest-upgrade")
          [[ ${mem} -gt $((5 * 1024 * 1024 * 1024 * ${jenkins_executors})) ]] || { echo >&2 "tests require minimum docker memory 6g (per jenkins executor (${jenkins_executors})), found ${mem}"; exit 1; }
          max_docker_runs_by_cores=$( echo "sqrt( ${cores} / ${jenkins_executors} )" | bc )
          max_docker_runs_by_mem=$(( ${mem} / ( 5 * 1024 * 1024 * 1024 * ${jenkins_executors} ) ))
          docker_runs=$(( ${max_docker_runs_by_cores} < ${max_docker_runs_by_mem} ? ${max_docker_runs_by_cores} : ${max_docker_runs_by_mem} ))
          docker_runs=$(( ${docker_runs} < 1 ? 1 : ${docker_runs} ))
        ;;
      *)
        echo "unrecognized \"${target}\""
        exit 1
        ;;
    esac

docker_runs=1 # tmp FIXME

    # Break up the requested split chunk into a number of concurrent docker runs, as calculated above
    # This will typically be between one to four splits. Five splits would require >25 cores and >25GB ram
    inner_splits=$(( $(echo $split_chunk | cut -d"/" -f2 ) * ${docker_runs} ))
    inner_split_first=$(( ( $(echo $split_chunk | cut -d"/" -f1 ) * ${docker_runs} ) - ( ${docker_runs} - 1 ) ))
    docker_cpus=$(echo "scale=2; ${cores} / ( ${jenkins_executors} * ${docker_runs} )" | bc)
    docker_flags="--cpus=${docker_cpus} -m 5g --memory-swap 5g --env-file env.list -dt"

    # the docker container's env
    cat > env.list <<EOF
TEST_SCRIPT=${test_script}
JAVA_VERSION=${java_version}
# FIXME python
PYTHON_VERSION=${python_version}
CYTHON_ENABLED=${cython}
EOF

    # hack: long-test does not handle limited CPUs
    if [ "${target}" == "long-test" ] ; then
        docker_flags="-m 5g --memory-swap 5g --env-file env.list -dt"
    elif [[ "${target}" =~ dtest* ]] ; then
        docker_flags="--cpus=${docker_cpus} -m 15g --memory-swap 15g --env-file env.list -dt"
    fi

    # docker login to avoid rate-limiting apache images. credentials are expected to already be in place
    docker login || true
    [[ "$(docker images -q ${docker_image} 2>/dev/null)" != "" ]] || docker pull -q ${docker_image}

    mkdir -p build/test/logs
    declare -a docker_ids
    declare -a process_ids
    declare -a statuses

    for i in `seq 1 ${docker_runs}` ; do
        inner_split=$(( ${inner_split_first} + ( $i - 1 ) ))
        # start the container
        echo "test-docker.sh: running:  bash /home/cassandra/cassandra/.build/test-docker.sh ${target} ${inner_split}/${inner_splits}"
        docker_id=$(docker run ${docker_flags} ${docker_mounts} ${docker_image} dumb-init bash -ilc "/home/cassandra/cassandra/.build/test-docker.sh ${target} ${inner_split}/${inner_splits}")

        # capture logs and pid for container
        docker attach --no-stdin ${docker_id} > build/test/logs/docker_attach_${i}.log &
        process_ids+=( $! )
        docker_ids+=( ${docker_id} )
    done

    exit_result=0
    i=0
    for process_id in "${process_ids[@]}" ; do
        # wait for each container to complete
        docker_id=${docker_ids[$i]}
        inner_split=$(( $inner_split_first + $i ))
        cat build/test/logs/docker_attach_$(( $i + 1 )).log
        tail -F build/test/logs/docker_attach_$(( $i + 1 )).log &
        tail_process=$!
        wait ${process_id}
        status=$?
        process_ids+=( ${status} )
        kill ${tail_process}

        if [ "$status" -ne 0 ] ; then
            echo "${docker_id} failed (${status}), debug…"
            docker inspect ${docker_id}
            echo "–––"
            docker logs ${docker_id}
            echo "–––"
            docker ps -a
            echo "–––"
            docker info
            echo "–––"
            exit_result=1
        else
            echo "${docker_id} done (${status}), copying files"
            # jvm tests
            docker cp ${docker_id}:/home/cassandra/cassandra/build/test/output/. build/test/output
            docker cp ${docker_id}:/home/cassandra/cassandra/build/test/logs/. build/test/logs
            # cqlshlib tests
            docker cp ${docker_id}:/home/cassandra/cassandra/cqlshlib.xml cqlshlib.xml
            # pytest results
            docker cp ${docker_id}:/home/cassandra/cassandra/cassandra-dtest/nosetests.xml .
            # pytest logs
            docker cp ${docker_id}:/home/cassandra/cassandra/test_stdout.txt.xz .
            docker cp ${docker_id}:/home/cassandra/cassandra/cassandra-dtest/ccm_logs.tar.xz .
        fi
        docker rm ${docker_id}
        ((i++))
    done

    xz -f build/test/logs/docker_attach_*.log
    exit $exit_result
fi
