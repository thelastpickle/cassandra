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
# Exercises the cpus() shell from .jenkins/Jenkinsfile against fabricated cgroup layouts.
#
# cpus() exists because nproc reports the node's processors and not the container's quota.  On a
# 8-processor node an agent limited to 2 cpu is told 8, and `xargs -P8` then runs four times the work the
# kubelet will let it run, which the cgroup throttles rather than refuses.  Every case below is a layout
# this cluster produces; the answer is the number of processors the pod may actually use.
#
# The function is extracted from the Jenkinsfile on every run, so this test cannot drift from what the
# pipeline executes.  The two absolute paths it reads are substituted for a temporary root.
#
# Requires: nothing beyond a POSIX shell and awk.  Run from anywhere:
#
#   .jenkins/k8s/cpus-test.sh [path to .jenkins/Jenkinsfile]

set -o nounset

CASSANDRA_DIR="$(cd "$(dirname "$0")/../.." > /dev/null && pwd)"
jenkinsfile="${1:-${CASSANDRA_DIR}/.jenkins/Jenkinsfile}"
work="$(mktemp -d)"
# Checked, because this script has no `set -e` and an unchecked failure here leaves ${work} empty: every
# fixture path then resolves to /, and the suite reports its own cases as failures.
[ -n "${work}" ] && [ -d "${work}" ] || { echo "mktemp -d failed, so there is nowhere to write the fixtures"; exit 1; }
trap 'rm -rf "${work}"' EXIT

# The function, between `cpusShell        = '''` and the closing `'''`.
awk "/^@Field String       cpusShell/{f=1;next} f&&/^'''\$/{exit} f" "${jenkinsfile}" > "${work}/cpus.raw"
[ -s "${work}/cpus.raw" ] || { echo "FAIL  could not extract cpus() from ${jenkinsfile}"; exit 1; }

nproc_here="$(nproc 2>/dev/null || echo 1)"
failures=0
n=0

# One case.  $1 names it, $2 is the expected answer, $3 is the /proc/self/cgroup content or "-" for absent,
# and the rest are "<relative cpu.max path>=<content>" pairs.
case_is() {
    local name="$1" expected="$2" proc="$3"
    shift 3
    n=$((n + 1))
    local root="${work}/root.${n}" procfile="${work}/proc.${n}"
    mkdir -p "${root}"
    if [ "${proc}" = "-" ]; then procfile="${work}/absent"; else printf '%s\n' "${proc}" > "${procfile}"; fi
    local pair path content
    for pair in "$@"; do
        path="${pair%%=*}"; content="${pair#*=}"
        mkdir -p "${root}/$(dirname "${path}")"
        printf '%s\n' "${content}" > "${root}/${path}"
    done

    sed -e "s#/sys/fs/cgroup#${root}#g" -e "s#/proc/self/cgroup#${procfile}#g" "${work}/cpus.raw" \
        > "${work}/cpus.${n}.sh"
    local actual
    actual="$(sh -c ". ${work}/cpus.${n}.sh; cpus" 2>/dev/null)"
    if [ "${actual}" = "${expected}" ]; then
        echo "PASS  ${name} (${actual})"
    else
        echo "FAIL  ${name}: expected ${expected}, got '${actual}'"
        failures=$((failures + 1))
    fi
}

# The case build 4 failed on: a host cgroup namespace, so /sys/fs/cgroup is the host's root and has no
# cpu.max.  The container's own path comes from /proc/self/cgroup.
case_is "host namespace reads the container's own cgroup" 2 \
    '0::/kubepods.slice/kubepods-burstable.slice/cri-containerd-abc.scope' \
    'kubepods.slice/kubepods-burstable.slice/cri-containerd-abc.scope/cpu.max=200000 100000'

# A private namespace, where /sys/fs/cgroup is the container's own cgroup.
case_is "private namespace reads /sys/fs/cgroup/cpu.max" 3 '0::/' 'cpu.max=300000 100000'

# A cgroup with no cpu limit.  nproc is the right answer, and this is the bare-metal agent case.
case_is "no limit falls back to nproc" "${nproc_here}" '0::/' 'cpu.max=max 100000'

# cgroup v1: no cpu.max anywhere, and /proc/self/cgroup has no 0:: line.
case_is "cgroup v1 falls back to nproc" "${nproc_here}" \
    '4:cpu,cpuacct:/kubepods/burstable/podabc'

# No /proc/self/cgroup at all, and a readable /sys/fs/cgroup/cpu.max.
case_is "an absent /proc/self/cgroup still reads the root" 4 '-' 'cpu.max=400000 100000'

# A quota under one whole cpu divides to zero, which would mean unbounded to xargs -P.
case_is "a sub-cpu quota clamps to 1" 1 '0::/' 'cpu.max=50000 100000'

# The container's own cgroup has no limit but the root does.  The root's value, because a limit on an
# ancestor bounds this container as well, and because under a private cgroup namespace the two files are the
# same file.  A host root with a cpu.max is hypothetical: the case that occurs is a root with none, which is
# the first case above.
case_is "an ancestor's limit still counts" 8 \
    '0::/kubepods.slice/pod.scope' \
    'kubepods.slice/pod.scope/cpu.max=max 100000' 'cpu.max=800000 100000'

# Nothing readable anywhere.
case_is "nothing readable falls back to nproc" "${nproc_here}" '-'

if [ "${failures}" -ne 0 ]; then
    echo "${failures} case(s) failed."
    exit 1
fi
echo "All cases behaved as recorded."
