#!/usr/bin/env python3
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

"""Whether the controller can hold the agents the pools are configured for, and what a bigger one costs.

Every other ceiling in this directory is the account's.  This one is the cluster's own, and it is the only
one that gets more expensive to raise while nothing is running: agent nodes are billed by the minute and
scale to zero between builds, the controller is billed for all 730 hours of a month.

## What is here and what is shared

The sizing model is ../shared/controller_model.py, because it is about Jenkins rather than about AWS: memory
follows the agent count, CPU follows the pod churn rate, and `idleMinutes` is therefore a controller-sizing
knob.  Read that module's docstring for the model and the measurement behind each constant.

What is here is everything AWS decides: what a node of a given instance type allocates, which instance sizes
exist to be recommended, and what one costs an hour.

## Usage

    controller-fit.py --pools <agent_node_groups.json> --controller <controller_node_group.json> \\
        --deployment ../jenkins-deployment.yaml [--overrides 2-platform/jenkins-eks-overrides.yaml] \\
        --region <region> [--check]

`--check` prints the sizing and the recommendation.  Without it only the export lines are printed, for the
`Makefile` to read.

`--overrides` is every values file `.build/run-ci` is given after the deployment, merged the way Helm merges
them.  Pass it, or this models a controller nobody deploys: the heap, the requests and the limits all come
from whichever file sets them last, and `2-platform/jenkins-eks-overrides.yaml` is after the shared one.

## It refuses the deploy, and why

A controller that cannot hold the agents the pools are configured for does not degrade gracefully.  It
queues, and the queue looks exactly like the agent ceiling binding, so the operator raises the pools and
makes it worse.  Worse still, the pods it cannot service are created anyway, wait out their
`slaveConnectTimeout`, and are reaped, which is the churn loop this directory has already been through once.

So this exits 1 when the controller lacks the headroom for the configured pools, and `make quota` therefore
fails, and `make jenkins` and `make platform` with it.  There are three ways past it and all three are
stated in the output: raise `controller_pool.instance_types`, lower the pools' `max_size`, or raise
`idleMinutes`, which is the only one that is free.

`--warn-only` downgrades the refusal to a warning, for an operator who has read the figures and accepts a
marginal controller.  `make quota CONTROLLER_FIT_ARGS=--warn-only` is the form of that.

## The requests are checked separately, and that refusal is not marginal

The model above is about a controller that will be slow.  A `requests` figure above the node's allocatable is
a different failure with the same inputs: the pod is never scheduled at all, the StatefulSet stays Pending,
and nothing in the cluster says why.  `--warn-only` does not cover it, because there is nothing to accept.

This is the check the committed values needed and did not have.  A `requests.cpu` of 13765m, generated for a
1121-node account, sat in `2-platform/jenkins-eks-overrides.yaml` against a default controller of
m7a.2xlarge, whose allocatable is 7910m.
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

# The sizing model itself, and the quantity parsing, are cloud-neutral and shared with the other clouds'
# directories; see ../shared/README.md.  What stays here is what AWS decides: node allocatable, the instance
# ladder, and the price.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "shared"))
from controller_model import (HEADROOM,  # noqa: E402 - after the sys.path line above
                             REAP_FRACTION_MEASURED_AT_IDLE_MINUTES, idle_minutes, launches_per_burst,
                             needed_cpu_millicores, needed_memory_mib)
from k8s_values import cpu_millicores, deep_merge, heap_mib, memory_mib  # noqa: E402 - same

# What EKS holds back from a node before a pod may have any of it: the kubelet's own reservation and its
# eviction threshold.  Measured on this cluster's m7a.2xlarge, which reports 7910m of 8000m allocatable and
# 29.7 GiB of 32 GiB.
NODE_CPU_OVERHEAD_MILLICORES = 90
NODE_MEMORY_OVERHEAD_FRACTION = 0.07

# Hours in a month, for the cost figures.  730 is 8760/12, which is what AWS uses in its own calculator.
HOURS_PER_MONTH = 730

# The ladder searched for a big enough instance, in the family the controller already uses.  Sizes only, so
# that a cluster on m7i or c7a searches its own family rather than being moved to another.
SIZE_LADDER = ("large", "xlarge", "2xlarge", "4xlarge", "8xlarge", "12xlarge", "16xlarge", "24xlarge")


def aws_json(args: list) -> dict:
    """One AWS CLI call returning JSON.  Raises CalledProcessError, which main() turns into a sentence."""
    output = subprocess.run(["aws"] + args + ["--output", "json"],
                            capture_output=True, text=True, check=True).stdout
    return json.loads(output)


def instance_shape(instance_type: str, region: str) -> dict:
    """One instance type's vCPU count and memory in MiB."""
    types = aws_json(["ec2", "describe-instance-types", "--region", region,
                      "--instance-types", instance_type])["InstanceTypes"]
    if not types:
        raise ValueError(f"EC2 reports no such instance type in {region}: {instance_type}")
    return {"instance_type": instance_type,
            "vcpus": int(types[0]["VCpuInfo"]["DefaultVCpus"]),
            "memory_mib": int(types[0]["MemoryInfo"]["SizeInMiB"])}


def hourly_price(instance_type: str, region: str) -> float:
    """On-demand Linux price an hour, or 0.0 when the pricing API cannot be read.

    The pricing API lives in us-east-1 whatever region is being priced, and needs `pricing:GetProducts`,
    which a credential that can read EC2 and EKS may not carry.  A missing price is reported as unknown
    rather than fatal: the sizing is the point and the cost is the annotation on it.
    """
    try:
        products = aws_json([
            "pricing", "get-products", "--region", "us-east-1", "--service-code", "AmazonEC2",
            "--max-results", "1",
            "--filters",
            f"Type=TERM_MATCH,Field=instanceType,Value={instance_type}",
            f"Type=TERM_MATCH,Field=regionCode,Value={region}",
            "Type=TERM_MATCH,Field=tenancy,Value=Shared",
            "Type=TERM_MATCH,Field=operatingSystem,Value=Linux",
            "Type=TERM_MATCH,Field=preInstalledSw,Value=NA",
            "Type=TERM_MATCH,Field=capacitystatus,Value=Used",
        ])["PriceList"]
        if not products:
            return 0.0
        terms = json.loads(products[0])["terms"]["OnDemand"]
        dimensions = next(iter(terms.values()))["priceDimensions"]
        return float(next(iter(dimensions.values()))["pricePerUnit"]["USD"])
    except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError, StopIteration, ValueError):
        return 0.0


# A CPU quantity as whole millicores, which is what this script reports and exports.  cpu_millicores()
# returns a float, because the kubelet reports fractions of one and check-pool-fit.py compares them.
def millicores(value, default=None):
    got = cpu_millicores(value, default)
    return got if got is None else int(got)


def derive(agent_nodes: int, controller: dict, deployment: dict, region: str) -> dict:
    """What the controller needs, what it has, and the smallest instance that would hold it."""
    controller_section = deployment.get("controller", {})
    resources = controller_section.get("resources", {}) or {}
    limits = resources.get("limits", {}) or {}
    requests = resources.get("requests", {}) or {}
    heap = heap_mib(controller_section.get("javaOpts", ""))
    idle = idle_minutes(deployment)

    launches = launches_per_burst(agent_nodes, idle)
    needed_cpu = needed_cpu_millicores(agent_nodes, idle)
    needed_memory = needed_memory_mib(agent_nodes, heap)

    # The declared limits are the pod's ceiling; the node's allocatable is the real one, and on this cluster
    # the pod's cpu limit already exceeds it.  Both are reported, because raising the limit alone buys
    # nothing.
    current_type = max(controller["instance_types"], key=lambda t: SIZE_LADDER.index(t.split(".")[-1])
                       if t.split(".")[-1] in SIZE_LADDER else -1)
    shape = instance_shape(current_type, region)
    allocatable_cpu = shape["vcpus"] * 1000 - NODE_CPU_OVERHEAD_MILLICORES
    allocatable_memory = int(shape["memory_mib"] * (1 - NODE_MEMORY_OVERHEAD_FRACTION))

    family = current_type.split(".")[0]
    start = SIZE_LADDER.index(current_type.split(".")[-1]) if current_type.split(".")[-1] in SIZE_LADDER else 0
    recommended = None
    for size in SIZE_LADDER[start:]:
        candidate = instance_shape(f"{family}.{size}", region)
        if (candidate["vcpus"] * 1000 - NODE_CPU_OVERHEAD_MILLICORES >= needed_cpu * HEADROOM
                and candidate["memory_mib"] * (1 - NODE_MEMORY_OVERHEAD_FRACTION) >= needed_memory * HEADROOM):
            recommended = candidate
            break

    # Reported, not compared: what the container may actually have is the lower of its own limit and the
    # node's allocatable, so both have to be raised together.  A bigger instance under an unchanged pod limit
    # changes nothing, and an 8 CPU limit already exceeds an m7a.2xlarge's 7910m, so the limit is not always
    # the binding one either.  None is unlimited, which is what Kubernetes does with an absent limit.
    limit_cpu = millicores(limits.get("cpu"))
    limit_memory = memory_mib(limits.get("memory"))

    # The requests, against the same allocatable.  A separate question from everything above: a limit above
    # allocatable makes a slow controller, a request above it makes a controller that is never scheduled.  0
    # for an absent request, which is what Kubernetes assumes and is always schedulable.
    request_cpu = millicores(requests.get("cpu"), 0)
    request_memory = memory_mib(requests.get("memory"), 0)

    current_price = hourly_price(current_type, region)
    recommended_price = current_price if recommended and recommended["instance_type"] == current_type \
        else (hourly_price(recommended["instance_type"], region) if recommended else 0.0)

    return {
        "region": region,
        "agent_nodes": agent_nodes,
        "idle_minutes": idle,
        "launches_per_burst": launches,
        "heap_mib": heap,
        "needed_cpu_millicores": int(needed_cpu),
        "needed_memory_mib": int(needed_memory),
        "limit_cpu_millicores": limit_cpu,
        "limit_memory_mib": limit_memory,
        "request_cpu_millicores": request_cpu,
        "request_memory_mib": request_memory,
        "requests_schedulable": (allocatable_cpu >= request_cpu
                                 and allocatable_memory >= request_memory),
        "current": shape,
        "current_allocatable_cpu_millicores": allocatable_cpu,
        "current_allocatable_memory_mib": allocatable_memory,
        "current_hourly_usd": current_price,
        "recommended": recommended,
        "recommended_hourly_usd": recommended_price,
        "fits": (allocatable_cpu >= needed_cpu * HEADROOM
                 and allocatable_memory >= needed_memory * HEADROOM),
        "fits_without_headroom": allocatable_cpu >= needed_cpu and allocatable_memory >= needed_memory,
    }


def exports(derived: dict) -> str:
    """The form ./Makefile puts these in the environment with, the same as vcpu-quota.py's."""
    recommended = derived["recommended"]
    return "\n".join([
        f"export EKS_CONTROLLER_NEEDS_CPU='{derived['needed_cpu_millicores']}'",
        f"export EKS_CONTROLLER_NEEDS_MEMORY_MIB='{derived['needed_memory_mib']}'",
        f"export EKS_CONTROLLER_FITS='{'true' if derived['fits'] else 'false'}'",
        f"export EKS_CONTROLLER_RECOMMENDED='{recommended['instance_type'] if recommended else ''}'",
    ]) + "\n"


def print_check(derived: dict) -> None:
    current = derived["current"]
    print(f"Controller sizing for {derived['agent_nodes']} agent nodes, which is what the pools' max_size"
          f" figures allow.")
    print()
    print(f"  {'needs':<26} {derived['needed_cpu_millicores']:>7}m cpu"
          f"   {derived['needed_memory_mib']:>7} MiB memory"
          f"   (heap {derived['heap_mib']} MiB, {derived['launches_per_burst']:.0f} pods a burst"
          f" at idleMinutes {derived['idle_minutes']})")
    print(f"  {'with ' + str(HEADROOM) + 'x headroom':<26} {int(derived['needed_cpu_millicores'] * HEADROOM):>7}m cpu"
          f"   {int(derived['needed_memory_mib'] * HEADROOM):>7} MiB memory")
    print(f"  {'pod requests ask':<26} {str(derived['request_cpu_millicores']) + 'm':>8} cpu"
          f"   {derived['request_memory_mib']:>7} MiB memory")
    print(f"  {'pod limits allow':<26} {str(derived['limit_cpu_millicores']) + 'm':>8} cpu"
          f"   {derived['limit_memory_mib']:>7} MiB memory")
    print(f"  {current['instance_type'] + ' allocatable':<26}"
          f" {derived['current_allocatable_cpu_millicores']:>7}m cpu"
          f"   {derived['current_allocatable_memory_mib']:>7} MiB memory")
    print()

    # Before the sizing verdict, because it is a harder failure than anything the model reports: a pod asking
    # for more than the node has is never placed, so the sizing question never arises.
    if not derived["requests_schedulable"]:
        print(f"REFUSING: the controller's requests do not fit {current['instance_type']}, so its pod is never"
              f" scheduled and the StatefulSet stays Pending.")
        print(f"  Lower controller.resources.requests, or raise controller_pool.instance_types to hold"
              f" {derived['request_cpu_millicores']}m cpu and {derived['request_memory_mib']} MiB.")
        print("  Whichever values file sets `controller.resources` last is the one to edit; on this cluster")
        print("  that is 2-platform/jenkins-eks-overrides.yaml, after ../jenkins-deployment.yaml.")
        return

    if derived["fits"]:
        print(f"The controller holds this. {current['instance_type']} at"
              f" ${derived['current_hourly_usd']:.4f} an hour is"
              f" ${derived['current_hourly_usd'] * HOURS_PER_MONTH:,.0f} a month, 24/7.")
        return

    if derived["fits_without_headroom"]:
        print(f"WARNING: the controller holds this with no margin. {current['instance_type']} covers the"
              f" figures above but not the {HEADROOM}x headroom, so a plugin update or a heavier profile"
              f" takes it over.")
    else:
        print(f"WARNING: the controller cannot hold this. {current['instance_type']} is short of the"
              f" figures above, so builds will queue behind a controller that is the bottleneck rather"
              f" than behind the agent ceiling.")

    recommended = derived["recommended"]
    if not recommended:
        print(f"  No size in the {current['instance_type'].split('.')[0]} family holds it. Lower the pools'"
              f" max_size figures, or raise idleMinutes in ../jenkins-deployment.yaml, which cuts the pod"
              f" churn that drives the cpu figure.")
        return

    delta = (derived["recommended_hourly_usd"] - derived["current_hourly_usd"]) * HOURS_PER_MONTH
    print(f"  Recommended: {recommended['instance_type']}, {recommended['vcpus']} vCPU and"
          f" {recommended['memory_mib']} MiB.")
    if derived["recommended_hourly_usd"] and derived["current_hourly_usd"]:
        print(f"  Cost: ${derived['recommended_hourly_usd'] * HOURS_PER_MONTH:,.0f} a month against"
              f" ${derived['current_hourly_usd'] * HOURS_PER_MONTH:,.0f}, so"
              f" ${delta:,.0f} a month more.")
    else:
        print("  Cost: the pricing API could not be read, so no figure. It needs pricing:GetProducts.")
    print()
    print("Three ways past this, cheapest first.")
    print()
    print(f"  1. Raise idleMinutes in ../jenkins-deployment.yaml. It is {derived['idle_minutes']} now, and it")
    print("     drives the cpu figure and nothing else here: doubling it halves the pods reaped per interval.")
    print("     It costs nothing but that agent pods live longer between builds.")
    print()
    print("  2. Lower the pools' max_size in 1-cluster/variables.tf. The memory figure follows the agent")
    print("     count directly, so this is the only lever that moves it.")
    print()
    print(f"  3. Raise controller_pool.instance_types to {recommended['instance_type']}, and raise")
    print("     controller.resources.limits and -Xmx in ../jenkins-deployment.yaml to match, or the bigger")
    print("     node changes nothing. This one costs money, every hour of every month:")
    print()
    print("     The agent nodes are billed by the minute and scale to zero between builds. The controller is")
    print("     billed for all 730 hours whether a build runs or not, so this is the one ceiling in this")
    print("     directory that is not free to raise.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--pools", required=True,
                       help="JSON from `tofu output -json agent_node_groups` in 1-cluster.")
    parser.add_argument("--controller", required=True,
                       help="JSON from `tofu output -json controller_node_group` in 1-cluster.")
    parser.add_argument("--deployment", required=True,
                       help="jenkins-deployment.yaml, read for the controller's limits, its -Xmx and each"
                            " podTemplate's idleMinutes")
    parser.add_argument("--overrides", action="append", default=[],
                       help="a values file run-ci applies after --deployment, merged over it the way Helm"
                            " merges two -f files. Repeatable, in the order run-ci passes them. Without it"
                            " this models the shared file alone, which is not what gets deployed")
    parser.add_argument("--region", default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"),
                       help="AWS region. Defaults to AWS_REGION or AWS_DEFAULT_REGION")
    parser.add_argument("--check", action="store_true", help="print the sizing and the recommendation")
    parser.add_argument("--exports-to", help="write the export lines to this file as well as stdout")
    parser.add_argument("--warn-only", action="store_true",
                       help="warn instead of refusing when the controller cannot hold the configured pools")
    args = parser.parse_args()

    if not args.region:
        print("A region is required: pass --region or set AWS_REGION.", file=sys.stderr)
        return 2

    try:
        import yaml
    except ImportError:
        print("PyYAML is needed to read jenkins-deployment.yaml: pip install pyyaml", file=sys.stderr)
        return 2

    try:
        pools = json.loads(Path(args.pools).read_text())
        controller = json.loads(Path(args.controller).read_text())
        deployment = yaml.safe_load(Path(args.deployment).read_text()) or {}
        for path in args.overrides:
            deployment = deep_merge(deployment, yaml.safe_load(Path(path).read_text()) or {})
    except (OSError, ValueError) as error:
        print(f"Could not read the inputs: {error}", file=sys.stderr)
        return 2

    agent_nodes = sum(int(pool["max_size"]) for pool in pools.values())
    if agent_nodes < 1:
        print("Every agent pool has max_size 0, so there is no controller sizing to do.", file=sys.stderr)
        return 1

    try:
        derived = derive(agent_nodes, controller, deployment, args.region)
    except (subprocess.CalledProcessError, ValueError, KeyError) as error:
        print(f"Could not size the controller: {error}", file=sys.stderr)
        return 2

    if args.check:
        print_check(derived)
    else:
        sys.stdout.write(exports(derived))
    if args.exports_to:
        Path(args.exports_to).write_text(exports(derived))

    # Before anything is written to stderr.  Both streams are captured to one log by `make quota`, and a
    # buffered stdout puts the refusal above the figures it refuses, which reads as a fault in the harness.
    sys.stdout.flush()

    # Not covered by --warn-only: an unschedulable pod is not a trade the operator can accept, it is a
    # controller that never starts.
    if not derived["requests_schedulable"]:
        print(f"\nRefusing: controller.resources.requests asks for {derived['request_cpu_millicores']}m cpu"
              f" and {derived['request_memory_mib']} MiB, and {derived['current']['instance_type']} allocates"
              f" {derived['current_allocatable_cpu_millicores']}m and"
              f" {derived['current_allocatable_memory_mib']} MiB.", file=sys.stderr)
        print("The controller's pod would stay Pending. Lower the requests or raise the instance type.",
              file=sys.stderr)
        return 1

    if derived["fits"]:
        return 0

    # The exports are written before this returns, so a caller that wants the figures after a refusal has
    # them.  Only the exit status changes.
    if args.warn_only:
        print("Continuing because --warn-only was given.", file=sys.stderr)
        return 0

    print("\nRefusing: the controller cannot hold the agents these pools are configured for.",
          file=sys.stderr)
    print("Take one of the three above, or pass --warn-only to deploy anyway"
          " (`make quota CONTROLLER_FIT_ARGS=--warn-only`).", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
