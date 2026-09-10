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

"""How much CPU and memory a Jenkins controller needs to hold N agents.

The model is about Jenkins and its Kubernetes plugin, so it is the same on every cloud.  What it is compared
against is a node, and a node's allocatable is the cloud's: that stays in each cloud's own directory, along
with the instance-size ladder the recommendation walks.

## Why this is not a guess

Measured on one m7a.2xlarge with a fixed 8 GiB heap; `.jenkins/k8s/eks/NOTES.md` records the runs.

  - 1087 agents connected at once.  Memory sat at 18105 MiB and did not move across four minutes of pod
    churn, so memory tracks the number of agents held.
  - CPU over the same four minutes ranged from 1301m to 7633m at that same 1087 agents, in bursts.  The
    bursts are `CloudRetentionStrategy#check` reaping agents that reached `idleMinutes` and the provisioner
    replacing them: the controller log shows 49 provisioned and 42 terminated inside one minute, then eight
    of each in the next.  So CPU tracks the pod churn rate and not the number of agents.

That distinction is the useful part.  A controller sized on the agent count alone is sized for the wrong axis
on CPU, and `idleMinutes` in jenkins-deployment.yaml is therefore a controller-sizing knob: doubling it
halves how many agents are reaped per interval and halves the burst.

## The model

    memory_MiB  =  heap + MEMORY_BASE_MIB + MEMORY_MIB_PER_AGENT * N
    cpu_m       =  CPU_BASE_MILLICORES + CPU_MILLICORES_PER_LAUNCH * L
    L           =  REAP_FRACTION * N * (REAP_FRACTION_MEASURED_AT_IDLE_MINUTES / idleMinutes)

N is the agents the caps allow, which is the sum of the pools' max_size, and L is how many pods launch at
once.  Every constant is one measurement at one load, so read this as an order of magnitude with the right
slope and not as four significant figures.  It is enough for the only question asked of it: does the
configured cluster need a bigger controller than the one configured.
"""

import re

# Non-heap memory an idle controller holds, beyond the JVM heap: metaspace, code cache, the JVM's own
# threads, and the plugins' fixed structures.  Estimated, and the one constant here not read off a
# measurement: 18105 MiB at 1087 agents less an 8 GiB heap leaves 10009 MiB, and the per-agent slope below
# accounts for 8153 of that.
MEMORY_BASE_MIB = 1856

# A connected agent costs one remoting channel: a thread stack, its input and output buffers, and the
# Computer object behind it.  From the same measurement, and the figure that makes memory the axis the agent
# count drives.
MEMORY_MIB_PER_AGENT = 7.5

# CPU with no pod churn.  The trough of the bursts, not an idle controller: at 1087 connected agents the
# controller still runs its queue maintenance, its ping thread per channel and its web requests.
CPU_BASE_MILLICORES = 1300

# One pod being created, launched and connected, or reaped.  6300m above the base for a burst of about 45.
CPU_MILLICORES_PER_LAUNCH = 140

# What share of the connected agents turn over in one reap interval.  45 of 1087, measured at idleMinutes 5.
REAP_FRACTION = 0.04
REAP_FRACTION_MEASURED_AT_IDLE_MINUTES = 5

# How much of the instance to leave unclaimed.  A controller at 100% of its node has no room for a plugin
# update, a heavier profile, or the kubelet and DaemonSets that share the node with it.
HEADROOM = 1.3


def idle_minutes(deployment: dict) -> int:
    """The largest idleMinutes across the agent podTemplates.

    The largest, not the smallest: a pool that keeps its pods longer contributes less churn, and the reap
    burst is dominated by whichever pool holds the most agents.  Every template in this repository carries
    the same value, so this only matters for a site that has changed one.
    """
    values = []
    for body in (deployment.get("agent", {}).get("podTemplates", {}) or {}).values():
        if isinstance(body, str):
            values += [int(m) for m in re.findall(r"^\s*idleMinutes:\s*(\d+)\s*$", body, re.M)]
    return max(values) if values else REAP_FRACTION_MEASURED_AT_IDLE_MINUTES


def launches_per_burst(agent_nodes: int, idle: int) -> float:
    """How many pods are created or reaped at once, which is what the CPU figure follows."""
    return REAP_FRACTION * agent_nodes * (REAP_FRACTION_MEASURED_AT_IDLE_MINUTES / max(idle, 1))


def needed_cpu_millicores(agent_nodes: int, idle: int) -> float:
    return CPU_BASE_MILLICORES + CPU_MILLICORES_PER_LAUNCH * launches_per_burst(agent_nodes, idle)


def needed_memory_mib(agent_nodes: int, heap_mib: int) -> float:
    return heap_mib + MEMORY_BASE_MIB + MEMORY_MIB_PER_AGENT * agent_nodes
