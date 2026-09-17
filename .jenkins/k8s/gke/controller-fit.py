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

"""Check controller requests and modeled demand against node capacity and pod limits."""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "shared"))
from controller_model import (HEADROOM,  # noqa: E402 - after the sys.path line above
                             REAP_FRACTION_MEASURED_AT_IDLE_MINUTES, idle_minutes, launches_per_burst,
                             needed_cpu_millicores, needed_memory_mib)
from k8s_values import cpu_millicores, deep_merge, heap_mib, memory_mib  # noqa: E402 - same

MEMORY_RESERVED_BASE_MIB = 255
MEMORY_RESERVED_TIERS = ((4096, 0.25), (4096, 0.20), (8192, 0.10), (114688, 0.06), (None, 0.02))

# The kubelet's default hard eviction threshold on memory, held back on top of the reservation above.  A pod
# sized to the last byte of what is left is a pod the node evicts.
EVICTION_MEMORY_MIB = 100

CPU_RESERVED_SHARES = (6.0, 1.0, 0.5, 0.5)
CPU_RESERVED_SHARE_BEYOND = 0.25

# Hours in a month, for the cost figures.  730 is 8760/12.
HOURS_PER_MONTH = 730

FALLBACK_LADDERS = {
    "e2-standard": (2, 4, 8, 16, 32),
    "e2-highcpu": (2, 4, 8, 16, 32),
    "n1-standard": (1, 2, 4, 8, 16, 32, 64, 96),
    "n1-highcpu": (2, 4, 8, 16, 32, 64, 96),
    "n2-standard": (2, 4, 8, 16, 32, 48, 64, 80, 96, 128),
    "n2-highcpu": (2, 4, 8, 16, 32, 48, 64, 80, 96),
    "n2d-standard": (2, 4, 8, 16, 32, 48, 64, 80, 96, 128, 224),
    "n2d-highcpu": (2, 4, 8, 16, 32, 48, 64, 80, 96, 128, 224),
    "c2-standard": (4, 8, 16, 30, 60),
    "c2d-standard": (2, 4, 8, 16, 32, 56, 112),
    "c2d-highcpu": (2, 4, 8, 16, 32, 56, 112),
}


def gcloud_json(args: list, project: str):
    """One gcloud call returning its parsed JSON, an object for a describe and a list for a list."""
    command = ["gcloud"] + args + ["--format=json"]
    if project:
        command += ["--project", project]
    output = subprocess.run(command, capture_output=True, text=True, check=True).stdout
    return json.loads(output)


def gke_reserved_memory_mib(capacity_mib: int) -> float:
    """MiB GKE reserves from a node's memory, by the tier schedule above."""
    if capacity_mib < 1024:
        return float(MEMORY_RESERVED_BASE_MIB)
    reserved = 0.0
    remaining = float(capacity_mib)
    for band, share in MEMORY_RESERVED_TIERS:
        if remaining <= 0:
            break
        inside = remaining if band is None else min(remaining, band)
        reserved += inside * share
        remaining -= inside
    return reserved


def gke_reserved_cpu_millicores(vcpus: int) -> float:
    """Millicores GKE reserves from a node's CPU, by the per-core schedule above."""
    reserved = 0.0
    for core in range(vcpus):
        share = CPU_RESERVED_SHARES[core] if core < len(CPU_RESERVED_SHARES) else CPU_RESERVED_SHARE_BEYOND
        reserved += 10.0 * share
    return reserved


def allocatable(shape: dict) -> dict:
    """What a node of this shape offers a pod: capacity less GKE's reservation, less the eviction threshold."""
    return {
        "cpu_millicores": int(shape["vcpus"] * 1000 - gke_reserved_cpu_millicores(shape["vcpus"])),
        "memory_mib": int(shape["memory_mib"] - gke_reserved_memory_mib(shape["memory_mib"])
                          - EVICTION_MEMORY_MIB),
    }


def machine_shape(machine_type: str, zone: str, project: str) -> dict:
    """One machine type's vCPU count and memory in MiB."""
    described = gcloud_json(["compute", "machine-types", "describe", machine_type, "--zone", zone], project)
    if not described:
        raise ValueError(f"Compute Engine reports no such machine type in {zone}: {machine_type}")
    return {"machine_type": described.get("name", machine_type),
            "vcpus": int(described["guestCpus"]),
            "memory_mib": int(described["memoryMb"])}


def type_vcpus(machine_type: str) -> int:
    """The vCPU count in a predefined machine type's name, or -1 when the name does not carry one."""
    tail = machine_type.strip().rsplit("-", 1)[-1]
    return int(tail) if tail.isdigit() and "custom" not in machine_type else -1


def family_of(machine_type: str) -> str:
    """The first two components of the name, `n2-standard`, which is what the ladder is a tail of."""
    return "-".join(machine_type.split("-")[:2])


def candidate_shapes(machine_type: str, zone: str, project: str) -> list:
    """Every size in this type's series and family, smallest first, with its shape."""
    family = family_of(machine_type)
    try:
        listed = gcloud_json(["compute", "machine-types", "list", "--zones", zone,
                              f"--filter=name~^{family}-[0-9]+$"], project)
    except (subprocess.CalledProcessError, json.JSONDecodeError):
        listed = []
    shapes = [{"machine_type": entry["name"],
               "vcpus": int(entry["guestCpus"]),
               "memory_mib": int(entry["memoryMb"])}
              for entry in listed or [] if entry.get("name", "").startswith(f"{family}-")]
    if not shapes:
        for size in FALLBACK_LADDERS.get(family, ()):
            try:
                shapes.append(machine_shape(f"{family}-{size}", zone, project))
            except (subprocess.CalledProcessError, KeyError, ValueError):
                continue
    return sorted(shapes, key=lambda shape: shape["vcpus"])


def hourly_usd(shape: dict, price_per_vcpu_hour: float) -> float:
    """List price an hour for a node of this shape, or 0.0 for unknown, which the caller reports as unknown."""
    if not price_per_vcpu_hour:
        return 0.0
    return shape["vcpus"] * price_per_vcpu_hour

# A CPU quantity as whole millicores, which is what this script reports and exports.  cpu_millicores()
# returns a float, because the kubelet reports fractions of one and 3-smoke/check-pool-fit.py compares them.
def millicores(value, default=None):
    got = cpu_millicores(value, default)
    return got if got is None else int(got)


def derive(agent_nodes: int, controller: dict, deployment: dict, zone: str, project: str,
           price_per_vcpu_hour: float) -> dict:
    """What the controller needs, what it has, and the smallest machine type that would hold it."""
    controller_section = deployment.get("controller", {})
    resources = controller_section.get("resources", {}) or {}
    limits = resources.get("limits", {}) or {}
    requests = resources.get("requests", {}) or {}
    heap = heap_mib(controller_section.get("javaOpts", ""))
    idle = idle_minutes(deployment)

    launches = launches_per_burst(agent_nodes, idle)
    needed_cpu = needed_cpu_millicores(agent_nodes, idle)
    needed_memory = needed_memory_mib(agent_nodes, heap)

    current_type = max(controller["machine_types"], key=type_vcpus)
    shape = machine_shape(current_type, zone, project)
    node = allocatable(shape)

    recommended = None
    for candidate in candidate_shapes(current_type, zone, project):
        if candidate["vcpus"] < shape["vcpus"]:
            continue
        room = allocatable(candidate)
        if (room["cpu_millicores"] >= needed_cpu * HEADROOM
                and room["memory_mib"] >= needed_memory * HEADROOM):
            recommended = candidate
            break

    limit_cpu = millicores(limits.get("cpu"))
    limit_memory = memory_mib(limits.get("memory"))

    request_cpu = millicores(requests.get("cpu") if requests.get("cpu") is not None else limits.get("cpu"), 0)
    request_memory = memory_mib(requests.get("memory") if requests.get("memory") is not None else limits.get("memory"), 0)

    available_cpu = min(node["cpu_millicores"], limit_cpu if limit_cpu is not None else float("inf"))
    available_memory = min(node["memory_mib"], limit_memory if limit_memory is not None else float("inf"))
    current_price = hourly_usd(shape, price_per_vcpu_hour)
    recommended_price = hourly_usd(recommended, price_per_vcpu_hour) if recommended else 0.0

    return {
        "zone": zone,
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
        "requests_schedulable": (node["cpu_millicores"] >= request_cpu
                                 and node["memory_mib"] >= request_memory),
        "current": shape,
        "current_allocatable_cpu_millicores": node["cpu_millicores"],
        "current_allocatable_memory_mib": node["memory_mib"],
        "current_hourly_usd": current_price,
        "recommended": recommended,
        "recommended_hourly_usd": recommended_price,
        "fits": (available_cpu >= needed_cpu * HEADROOM
                 and available_memory >= needed_memory * HEADROOM),
        "fits_without_headroom": (available_cpu >= needed_cpu
                                  and available_memory >= needed_memory),
    }


def exports(derived: dict) -> str:
    """The form ./Makefile puts these in the environment with, the same as vcpu-quota.py's."""
    recommended = derived["recommended"]
    return "\n".join([
        f"export GKE_CONTROLLER_NEEDS_CPU='{derived['needed_cpu_millicores']}'",
        f"export GKE_CONTROLLER_NEEDS_MEMORY_MIB='{derived['needed_memory_mib']}'",
        f"export GKE_CONTROLLER_FITS='{'true' if derived['fits'] else 'false'}'",
        f"export GKE_CONTROLLER_RECOMMENDED='{recommended['machine_type'] if recommended else ''}'",
    ]) + "\n"


def print_month(label: str, hourly: float) -> None:
    """One monthly figure, with the discount GCP applies to it that AWS does not."""
    print(f"  {label}: ${hourly * HOURS_PER_MONTH:,.0f} a month at list price, 24/7. GCP applies sustained")
    print("  use discounts automatically, up to about 30% on N1 and N2 for a full month, so the bill for a")
    print("  controller that never stops arrives under that figure.")


def print_check(derived: dict) -> None:
    current = derived["current"]
    print(f"Controller sizing for {derived['agent_nodes']} agent nodes, which is what the pools' max_size"
          f" figures allow.")
    print()
    print(f"  {'needs':<28} {derived['needed_cpu_millicores']:>7}m cpu"
          f"   {derived['needed_memory_mib']:>7} MiB memory"
          f"   (heap {derived['heap_mib']} MiB, {derived['launches_per_burst']:.0f} pods a burst"
          f" at idleMinutes {derived['idle_minutes']})")
    print(f"  {'with ' + str(HEADROOM) + 'x headroom':<28}"
          f" {int(derived['needed_cpu_millicores'] * HEADROOM):>7}m cpu"
          f"   {int(derived['needed_memory_mib'] * HEADROOM):>7} MiB memory")
    print(f"  {'pod requests ask':<28} {str(derived['request_cpu_millicores']) + 'm':>8} cpu"
          f"   {derived['request_memory_mib']:>7} MiB memory")
    print(f"  {'pod limits allow':<28} {('unlimited' if derived['limit_cpu_millicores'] is None else str(derived['limit_cpu_millicores']) + 'm'):>8} cpu"
          f"   {('unlimited' if derived['limit_memory_mib'] is None else str(derived['limit_memory_mib'])):>7} MiB memory")
    print(f"  {current['machine_type'] + ' capacity':<28} {current['vcpus'] * 1000:>7}m cpu"
          f"   {current['memory_mib']:>7} MiB memory")
    print(f"  {current['machine_type'] + ' allocatable':<28}"
          f" {derived['current_allocatable_cpu_millicores']:>7}m cpu"
          f"   {derived['current_allocatable_memory_mib']:>7} MiB memory"
          f"   (GKE's reservation and a {EVICTION_MEMORY_MIB} MiB eviction threshold)")
    print()

    # Before the sizing verdict, because it is a harder failure than anything the model reports: a pod asking
    # for more than the node has is never placed, so the sizing question never arises.
    if not derived["requests_schedulable"]:
        print(f"REFUSING: the controller's requests do not fit {current['machine_type']}, so its pod is never"
              f" scheduled and the StatefulSet stays Pending.")
        print(f"  Lower controller.resources.requests, or raise controller_pool.machine_types to hold"
              f" {derived['request_cpu_millicores']}m cpu and {derived['request_memory_mib']} MiB.")
        print("  Whichever values file sets `controller.resources` last is the one to edit; on this cluster")
        print("  that is 2-platform/jenkins-gke-overrides.yaml, after ../jenkins-deployment.yaml.")
        return

    if not derived["fits"] and ((derived["limit_cpu_millicores"] is not None and derived["limit_cpu_millicores"] < derived["needed_cpu_millicores"] * HEADROOM) or (derived["limit_memory_mib"] is not None and derived["limit_memory_mib"] < derived["needed_memory_mib"] * HEADROOM)):
        print("Controller pod limits also need raising; a larger node alone will not supply this capacity.")
    if derived["fits"]:
        print(f"The controller holds this. {current['machine_type']} is the size in use.")
        if derived["current_hourly_usd"]:
            print_month(current["machine_type"], derived["current_hourly_usd"])
        else:
            print("  Cost: no price was read, so no figure. Set GKE_SPEND_PRICE_PER_VCPU_HOUR, which the"
                  " spend guard already carries, for an estimate.")
        return

    if derived["fits_without_headroom"]:
        print(f"WARNING: the controller holds this with no margin. {current['machine_type']} covers the"
              f" figures above but not the {HEADROOM}x headroom, so a plugin update or a heavier profile"
              f" takes it over.")
    else:
        print(f"WARNING: the controller cannot hold this. {current['machine_type']} is short of the"
              f" figures above, so builds will queue behind a controller that is the bottleneck rather"
              f" than behind the agent ceiling.")

    recommended = derived["recommended"]
    if not recommended:
        print(f"  No size in the {family_of(current['machine_type'])} family holds it. Lower the pools'"
              f" max_size figures, or raise idleMinutes in ../jenkins-deployment.yaml, which cuts the pod"
              f" churn that drives the cpu figure.")
        return

    print(f"  Recommended: {recommended['machine_type']}, {recommended['vcpus']} vCPU and"
          f" {recommended['memory_mib']} MiB.")
    if derived["recommended_hourly_usd"] and derived["current_hourly_usd"]:
        delta = (derived["recommended_hourly_usd"] - derived["current_hourly_usd"]) * HOURS_PER_MONTH
        print_month(recommended["machine_type"], derived["recommended_hourly_usd"])
        print(f"  That is ${delta:,.0f} a month more than {current['machine_type']}'s"
              f" ${derived['current_hourly_usd'] * HOURS_PER_MONTH:,.0f}, before the same discount.")
    else:
        print("  Cost: no price was read, so no figure. There is no gcloud equivalent of AWS's pricing API;"
              " set GKE_SPEND_PRICE_PER_VCPU_HOUR, which the spend guard already carries, for an estimate.")
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
    print(f"  3. Raise controller_pool.machine_types to {recommended['machine_type']}, and raise")
    print("     controller.resources.limits and -Xmx in ../jenkins-deployment.yaml to match, or the bigger")
    print("     node changes nothing. This one costs money, every hour of every month:")
    print()
    print("     The agent nodes are billed by the second and scale to zero between builds. The controller is")
    print("     billed for all 730 hours whether a build runs or not, so this is the one ceiling in this")
    print("     directory that is not free to raise.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--pools", required=True,
                       help="JSON from `tofu output -json agent_node_pools` in 1-cluster.")
    parser.add_argument("--controller", required=True,
                       help="JSON from `tofu output -json controller_node_pool` in 1-cluster. Its `zone` is"
                            " read as well as its machine type, because machine types are zonal.")
    parser.add_argument("--deployment", required=True,
                       help="jenkins-deployment.yaml, read for the controller's limits, its -Xmx and each"
                            " podTemplate's idleMinutes")
    parser.add_argument("--overrides", action="append", default=[],
                       help="a values file run-ci applies after --deployment, merged over it the way Helm"
                            " merges two -f files. Repeatable, in the order run-ci passes them. Without it"
                            " this models the shared file alone, which is not what gets deployed")
    parser.add_argument("--zone", help="the zone to read machine shapes from. Defaults to the controller"
                                       " pool's own `zone`, which is the zone its nodes are created in")
    parser.add_argument("--project", default=os.environ.get("GOOGLE_PROJECT", ""),
                       help="GCP project id. Defaults to GOOGLE_PROJECT, and to gcloud's own default when"
                            " neither is set: a machine type's shape is the same in every project")
    parser.add_argument("--price-per-vcpu-hour", type=float,
                       help="USD an hour for one vCPU, for the cost figures. Defaults to"
                            " GKE_SPEND_PRICE_PER_VCPU_HOUR, which the spend guard carries. Unset, the cost"
                            " is reported as unknown and the sizing stands without it")
    parser.add_argument("--check", action="store_true", help="print the sizing and the recommendation")
    parser.add_argument("--exports-to", help="write the export lines to this file as well as stdout")
    parser.add_argument("--warn-only", action="store_true",
                       help="warn instead of refusing when the controller cannot hold the configured pools")
    args = parser.parse_args()

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

    # Machine types are zonal on GCP, unlike an EC2 instance type, so this is not optional.  The controller
    # pool pins one zone and layer 1 prints it, which makes the pool JSON the one place it has to come from.
    zone = args.zone or controller.get("zone") or ""
    if not zone:
        print("A zone is required: machine types are zonal on GCP. It comes from the controller pool's"
              " `zone`, so pass --zone or check `tofu output -json controller_node_pool`.", file=sys.stderr)
        return 2

    if not controller.get("machine_types"):
        print("The controller pool JSON carries no machine_types, so there is nothing to size against.",
              file=sys.stderr)
        return 2

    price = args.price_per_vcpu_hour
    if price is None:
        try:
            price = float(os.environ.get("GKE_SPEND_PRICE_PER_VCPU_HOUR", "") or 0.0)
        except ValueError:
            price = 0.0

    agent_nodes = sum(int(pool["max_size"]) for pool in pools.values())
    if agent_nodes < 1:
        print("Every agent pool has max_size 0, so there is no controller sizing to do.", file=sys.stderr)
        return 1

    try:
        derived = derive(agent_nodes, controller, deployment, zone, args.project, price)
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
              f" and {derived['request_memory_mib']} MiB, and {derived['current']['machine_type']} allocates"
              f" {derived['current_allocatable_cpu_millicores']}m and"
              f" {derived['current_allocatable_memory_mib']} MiB.", file=sys.stderr)
        print("The controller's pod would stay Pending. Lower the requests or raise the machine type.",
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
