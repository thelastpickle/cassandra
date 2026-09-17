#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
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
"""Report Compute Engine quotas and safe agent capacity within the configured pool maxima."""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

GLOBAL_CPU_METRIC = "CPUS_ALL_REGIONS"

# The three vCPU metrics that are not a series' own.  Every project reports all three; a series metric is
# reported only where the project has one, which is the difference the warnings below turn on.
BASE_CPU_METRICS = frozenset({"CPUS", "PREEMPTIBLE_CPUS", GLOBAL_CPU_METRIC})

DISK_METRICS = {
    "pd-standard": "DISKS_TOTAL_GB",
    "pd-balanced": "SSD_TOTAL_GB",
    "pd-ssd": "SSD_TOTAL_GB",
    "pd-extreme": "SSD_TOTAL_GB",
    "hyperdisk-balanced": "HDB_TOTAL_GB",
}
DEFAULT_DISK_METRIC = "SSD_TOTAL_GB"

# Shared-core types, whose name does not end in a vCPU count and whose fractional vCPU is charged to the
# quota in a way this script does not model.  Refused by name rather than guessed at.
SHARED_CORE_TYPES = frozenset({"e2-micro", "e2-small", "e2-medium", "f1-micro", "g1-small"})


def gcloud_json(args: list, project: str) -> dict:
    """One gcloud call returning JSON.  Raises CalledProcessError, which main() turns into a sentence."""
    command = ["gcloud"] + args + ["--format=json"]
    if project:
        command += ["--project", project]
    output = subprocess.run(command, capture_output=True, text=True, check=True).stdout
    return json.loads(output)


def quota_table(region: str, project: str) -> dict:
    """Every metric the project reports, as {metric: {"limit": float, "usage": float, "scope": str}}."""
    table = {}
    regional = gcloud_json(["compute", "regions", "describe", region], project)
    for quota in regional.get("quotas") or []:
        table[quota["metric"].replace("-", "_")] = {"limit": float(quota["limit"]),
                                  "usage": float(quota.get("usage") or 0.0),
                                  "scope": region}
    # Global second and with setdefault, so that a metric reported in both scopes keeps the regional figure:
    # the region is what governs a node created in it.
    project_info = gcloud_json(["compute", "project-info", "describe"], project)
    for quota in project_info.get("quotas") or []:
        table.setdefault(quota["metric"].replace("-", "_"), {"limit": float(quota["limit"]),
                                           "usage": float(quota.get("usage") or 0.0),
                                           "scope": "global"})
    return table


def machine_type_vcpus(machine_type: str) -> int:
    """The vCPU count of one predefined machine type, from its name."""
    name = machine_type.strip()
    if name in SHARED_CORE_TYPES:
        raise ValueError(f"{name} is a shared-core machine type, whose fractional vCPU this script does not"
                         " model. Use a predefined type whose name ends in its vCPU count")
    if "custom" in name:
        raise ValueError(f"{name} is a custom machine type, whose name ends in its memory rather than its"
                         " vCPU count. Use a predefined type, or teach this script the custom grammar")
    tail = name.rsplit("-", 1)[-1]
    if not tail.isdigit() or len(name.split("-")) < 3:
        raise ValueError(f"cannot read a vCPU count from the machine type {name}: a predefined type is"
                         " <series>-<family>-<vCPUs>, and this one is not")
    return int(tail)


def machine_series(machine_type: str) -> str:
    """The series a machine type belongs to, which is the first component of its name: n2, e2, c2d."""
    return machine_type.strip().split("-", 1)[0]


def pool_vcpus(machine_types: list) -> int:
    """The vCPU count to charge a node of this pool."""
    if not machine_types:
        raise ValueError("a pool with no machine_types cannot be charged to a quota")
    return max(machine_type_vcpus(machine_type) for machine_type in machine_types)


def charged_metrics(row: dict, table: dict = None) -> dict:
    """Charge ordinary CPU quotas until a separate Spot quota is reported."""
    serieses = {machine_series(kind).upper() for kind in row["machine_types"]}
    supported = {"E2", "N1", "N2", "N2D", "T2D", "C2", "C2D", "C3", "M1", "M2", "M3"}
    if serieses - supported:
        raise ValueError("quota sizing does not support these machine series: " + ", ".join(sorted(serieses - supported)))
    separate_spot = row["spot"] and table is not None and "PREEMPTIBLE_CPUS" in table
    charges = {GLOBAL_CPU_METRIC: row["vcpus"]}
    if separate_spot:
        charges["PREEMPTIBLE_CPUS"] = row["vcpus"]
    else:
        charges.update({"CPUS" if series in {"E2", "N1"} else f"{series}_CPUS": row["vcpus"] for series in serieses})
    if row["disk_gb"]:
        charges[row["disk_metric"]] = row["disk_gb"]
    return charges


def pool_row(name: str, pool: dict) -> dict:
    """One pool, reduced to the fields the arithmetic below needs."""
    machine_types = list(pool["machine_types"])
    disk_type = pool.get("disk_type") or ""
    row = {
        "name": name,
        "machine_types": machine_types,
        "max_size": int(pool["max_size"]),
        "declared_max_size": int(pool.get("declared_max_size", pool["max_size"])),
        "vcpus": pool_vcpus(machine_types),
        "spot": bool(pool.get("spot", False)),
        "disk_gb": int(pool.get("disk_gb") or 0),
        "disk_type": disk_type,
        "disk_metric": DISK_METRICS.get(disk_type, DEFAULT_DISK_METRIC),
    }
    row["charges"] = charged_metrics(row)
    return row


def metric_ceilings(rows: list, controller_row: dict, table: dict) -> list:
    """One entry per metric any node charges, with the node ceiling it imposes."""
    rows = [row for row in rows if row["max_size"] > 0]
    all_rows = rows + [controller_row]
    entries = []
    for metric in sorted({name for row in all_rows for name in row["charges"]}):
        paying = [row for row in all_rows if metric in row["charges"]]
        agent_paying = [row for row in rows if metric in row["charges"]]
        largest = max(row["charges"][metric] for row in paying)
        reported = table.get(metric)
        entry = {
            "metric": metric,
            # CPUS_ALL_REGIONS ends in neither word, so this matches on the stem rather than the suffix.
            "unit": "vCPU" if "CPUS" in metric else "GB",
            "scope": reported["scope"] if reported else "",
            "reported": reported is not None,
            "limit": reported["limit"] if reported else 0.0,
            "usage": reported["usage"] if reported else 0.0,
            "headroom": (reported["limit"] - reported["usage"]) if reported else 0.0,
            "largest": largest,
            "demand": sum(row["charges"][metric] * row["max_size"] for row in paying),
            "controller_pays": metric in controller_row["charges"],
            "agents_pay": bool(agent_paying),
            "nodes": None,
            "agent_nodes": None,
        }
        if reported:
            controller_charge = controller_row["charges"].get(metric, 0) * controller_row["max_size"]
            remaining = max(0, entry["limit"] - controller_charge)
            entry["nodes"] = int(entry["limit"] // largest)
            entry["controller_demand"] = controller_charge
            if agent_paying:
                # Count the most expensive possible composition first. Pool maxima bound each contribution.
                count = 0
                for row in sorted(rows, key=lambda row: row["charges"].get(metric, 0), reverse=True):
                    charge = row["charges"].get(metric, 0)
                    take = min(row["max_size"], int(remaining // charge)) if charge else row["max_size"]
                    count += take
                    remaining -= take * charge
                    if take < row["max_size"]:
                        break
                entry["agent_nodes"] = count
        entries.append(entry)
    return entries


def derive(pools: dict, controller: dict, region: str, project: str, pod_range_nodes) -> dict:
    """The ceiling, the metric that sets it, and the demand it is measured against."""
    rows = [pool_row(name, pool) for name, pool in sorted(pools.items())]
    controller_row = pool_row("controller", controller)

    table = quota_table(region, project)
    for row in rows + [controller_row]:
        row["charges"] = charged_metrics(row, table)
    metrics = metric_ceilings(rows, controller_row, table)

    # The vCPU the pools plus the controller ask for, across every metric.  One figure, because it is the one
    # an operator quotes in a quota request; the per-metric asks are in the table --check prints.
    demand = sum(row["vcpus"] * row["max_size"] for row in rows + [controller_row])

    bounded = [entry for entry in metrics if entry["agent_nodes"] is not None]
    binding = min(bounded, key=lambda entry: entry["agent_nodes"]) if bounded else None

    pool_agent_nodes = sum(row["max_size"] for row in rows)
    declared_agent_nodes = sum(row["declared_max_size"] for row in rows)

    quota_agent_nodes = binding["agent_nodes"] if binding else pool_agent_nodes
    quota_nodes = quota_agent_nodes + controller_row["max_size"]

    # A metric the controller's own node does not fit is a cluster that has no controller, which is a
    # different sentence from "no agent can run" and is reported as one.
    controller_short = [entry for entry in metrics
                        if entry["reported"] and entry["controller_pays"]
                        and entry["controller_demand"] > entry["limit"]]

    missing = [entry["metric"] for entry in metrics if not entry["reported"]]
    unreported_series = [metric for metric in missing
                         if metric.endswith("_CPUS") and metric not in BASE_CPU_METRICS]
    unreported_other = [metric for metric in missing if metric not in unreported_series]

    return {
        "region": region,
        "project": project,
        "quota": binding["limit"] if binding else 0.0,
        "quota_metric": binding["metric"] if binding else "",
        "quota_unit": binding["unit"] if binding else "",
        "quota_largest": binding["largest"] if binding else 0,
        "quota_metric_nodes": binding["nodes"] if binding else 0,
        "quota_controller_pays": binding["controller_pays"] if binding else False,
        "quota_headroom": binding["headroom"] if binding else 0.0,
        "checked": binding is not None,
        "demand": demand,
        "rows": rows,
        "controller": controller_row,
        "metrics": metrics,
        "quota_nodes": quota_nodes,
        "quota_agent_nodes": quota_agent_nodes,
        "pool_agent_nodes": pool_agent_nodes,
        "declared_agent_nodes": declared_agent_nodes,
        "scaled": declared_agent_nodes != pool_agent_nodes,
        "binds": "quota" if quota_agent_nodes < pool_agent_nodes else "pools",
        "max_nodes_total": min(quota_nodes, pool_agent_nodes + controller_row["max_size"]),
        "max_agent_nodes": min(quota_agent_nodes, pool_agent_nodes),
        "controller_short": controller_short,
        "unreported_series": unreported_series,
        "unreported_other": unreported_other,
        "over_ask": [entry for entry in metrics if entry["reported"] and entry["demand"] > entry["limit"]],
        "over_headroom": [entry for entry in metrics if entry["reported"]
                          and entry["limit"] >= entry["demand"] > entry["headroom"]],
        "pod_range_nodes": pod_range_nodes,
    }


def exports(derived: dict) -> str:
    """The form ./Makefile puts these in the environment with, the same as layer 1's `environment`."""
    return "\n".join([
        f"export GKE_VCPU_QUOTA='{int(derived['quota'])}'",
        f"export GKE_VCPU_QUOTA_METRIC='{derived['quota_metric']}'",
        f"export GKE_VCPU_DEMAND='{derived['demand']}'",
        f"export GKE_MAX_NODES_TOTAL='{derived['max_nodes_total']}'",
        f"export GKE_MAX_AGENT_NODES='{derived['max_agent_nodes']}'",
    ]) + "\n"


def print_check(derived: dict) -> None:
    print(f"Compute Engine quotas in {derived['region']}, project {derived['project']}:")
    for entry in derived["metrics"]:
        if not entry["reported"]:
            print(f"  {entry['metric']:<24}  not reported by this project,"
                  f" so nothing here bounds {entry['largest']} {entry['unit']} a node against it")
            continue
        print(f"  {entry['metric']:<24}  limit {entry['limit']:>9,.0f}  usage {entry['usage']:>9,.0f}"
              f"  headroom {entry['headroom']:>9,.0f}"
              f"  ask {entry['demand']:>9,.0f} {entry['unit']:<4}"
              f"  holds {entry['nodes']:>6} node(s)")
    print()
    # The widths are measured rather than guessed, because a pool may list two machine types and the Spot
    # marker lengthens the line again: a fixed column leaves the totals row out of line with the rest.
    heads = []
    for row in derived["rows"] + [derived["controller"]]:
        heads.append(f"{row['name']:<10}  max {row['max_size']:>4} nodes  x {row['vcpus']:>3} vCPU"
                     f"  {', '.join(row['machine_types'])}{', spot' if row['spot'] else ''}")
    width = max(len(head) for head in heads)
    for head, row in zip(heads, derived["rows"] + [derived["controller"]]):
        print(f"  {head:<{width}}  = {row['vcpus'] * row['max_size']:>6} vCPU")
    print(f"  {'total':<{width}}  = {derived['demand']:>6} vCPU")
    print()
    if derived["checked"]:
        print(f"After reserving controller capacity, the reported quotas allow"
              f" {derived['quota_agent_nodes']} agents within the configured pool maxima.")
    else:
        print("No reported quota bounds the active agent pools.")
    print(f"The node pools can create {derived['pool_agent_nodes']} agent nodes.")
    if derived["scaled"]:
        print(f"Layer 1 scaled the pools: var.agent_pools asked for {derived['declared_agent_nodes']}"
              f" agent nodes and the node pools were created with {derived['pool_agent_nodes']}."
              " `tofu output agent_node_ceiling` in 1-cluster says which ceiling bound.")
    print()
    if derived["binds"] == "quota":
        print(f"{derived['quota_metric']} binds: {derived['max_agent_nodes']} agents at once.")
    else:
        print(f"The node pools bind: {derived['max_agent_nodes']} agents at once.")
        print("Recheck each quota before raising a pool maximum.")


def resolve_controller(args) -> dict:
    """The controller's node, from --controller-pool where it is given and from the flags where they are."""
    controller = {
        "machine_types": os.environ.get("GKE_CONTROLLER_MACHINE_TYPES", "").split(),
        "max_size": int(os.environ.get("GKE_CONTROLLER_MAX_SIZE", "1")),
        "spot": False,
        "disk_gb": 0,
        "disk_type": "",
    }

    if args.controller_pool:
        loaded = json.loads(Path(args.controller_pool).read_text(encoding="utf-8"))
        if loaded.get("machine_types"):
            controller["machine_types"] = list(loaded["machine_types"])
        if loaded.get("max_size") is not None:
            controller["max_size"] = int(loaded["max_size"])
        controller["spot"] = bool(loaded.get("spot", False))
        controller["disk_gb"] = int(loaded.get("disk_gb") or 0)
        controller["disk_type"] = loaded.get("disk_type") or ""

    if args.controller_machine_types:
        controller["machine_types"] = args.controller_machine_types.split()
    if args.controller_max_size is not None:
        controller["max_size"] = args.controller_max_size
    if args.controller_spot:
        controller["spot"] = True
    if args.controller_disk_gb is not None:
        controller["disk_gb"] = args.controller_disk_gb
    return controller


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Derive the node ceiling the project's Compute Engine quotas impose.")
    parser.add_argument("--pools", required=True,
                        help="JSON from `tofu output -json agent_node_pools` in 1-cluster."
                             " Defaults to $GKE_AGENT_POOLS_FILE through ./Makefile.")
    parser.add_argument("--region", help="GCP region. Defaults to $GOOGLE_REGION, then $GKE_LOCATION.")
    parser.add_argument("--project", help="GCP project id. Defaults to $GOOGLE_PROJECT."
                                          " The quotas are the project's, so this is not optional.")
    parser.add_argument("--controller-pool",
                        help="JSON from `tofu output -json controller_node_pool` in 1-cluster. Its machine"
                             " type, spot flag and disk are read; the flags below each override it.")
    parser.add_argument("--controller-machine-types",
                        help="Space separated, as layer 1 exports GKE_CONTROLLER_MACHINE_TYPES."
                             " The controller's node is charged to the same quotas as an agent's.")
    parser.add_argument("--controller-max-size", type=int,
                        help="Defaults to $GKE_CONTROLLER_MAX_SIZE, then to 1.")
    parser.add_argument("--controller-spot", action="store_true",
                        help="The controller pool runs Spot, so its vCPU is charged to PREEMPTIBLE_CPUS.")
    parser.add_argument("--controller-disk-gb", type=int,
                        help="The controller's boot disk in GB. Defaults to the largest agent pool's,"
                             " because leaving it out over-reports the disk ceiling by one node.")
    parser.add_argument("--pod-range-node-ceiling", type=int,
                        help="How many nodes the cluster's pod range holds, as"
                             " `tofu output agent_node_ceiling` reports it. Given, a pod range lower than"
                             " the quota is warned about, because it cannot be raised afterwards.")
    parser.add_argument("--check", action="store_true",
                        help="Print the figures as a table instead of `export` lines.")
    parser.add_argument("--exports-to",
                        help="Also write the `export` lines to this file, so that two gcloud calls serve"
                             " both a reader and the environment ./Makefile builds.")
    args = parser.parse_args()

    region = args.region or os.environ.get("GOOGLE_REGION") or os.environ.get("GKE_LOCATION")
    if not region:
        print("A region is required: pass --region or set GOOGLE_REGION.", file=sys.stderr)
        return 2

    project = args.project or os.environ.get("GOOGLE_PROJECT", "")
    if not project:
        print("A project is required: pass --project or set GOOGLE_PROJECT. Quotas are a property of the"
              " project, so a run against the wrong one reports the wrong ceiling.", file=sys.stderr)
        return 2

    try:
        pools = json.loads(Path(args.pools).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        print(f"Could not read --pools {args.pools}: {error}", file=sys.stderr)
        return 2

    try:
        controller = resolve_controller(args)
    except (OSError, json.JSONDecodeError, ValueError) as error:
        print(f"Could not read the controller pool: {error}", file=sys.stderr)
        return 2
    if not controller.get("machine_types"):
        print("The controller's machine type is required: pass --controller-machine-types or"
              " --controller-pool, or set GKE_CONTROLLER_MACHINE_TYPES. `make env` exports it; see"
              " 1-cluster/outputs.tf.", file=sys.stderr)
        return 2
    if not controller.get("disk_gb"):
        # Stood in rather than left at zero: a controller counted with no disk over-reports the disk metric's
        # node ceiling by one node, and every pool here uses the same disk size.
        controller["disk_gb"] = max([int(pool.get("disk_gb") or 0) for pool in pools.values()] or [0])
        controller.setdefault("disk_type", "")
        if not controller.get("disk_type"):
            controller["disk_type"] = next((pool.get("disk_type") or "" for pool in pools.values()), "")

    # Caught so that the answer is a sentence naming the credential.  Uncaught, an expired token prints a
    # CalledProcessError traceback in the middle of `make platform`, which reads as a fault here.
    try:
        derived = derive(pools, controller, region, project, args.pod_range_node_ceiling)
    except subprocess.CalledProcessError as error:
        print(f"ERROR: gcloud would not answer for {project} in {region}, so the node ceiling is unknown."
              " Check the credential first: gcloud auth list", file=sys.stderr)
        print(f"       {(error.stderr or '').strip() or error}", file=sys.stderr)
        return 2
    except (KeyError, ValueError) as error:
        print(f"ERROR: could not derive the node ceiling: {error}", file=sys.stderr)
        return 2

    if args.check:
        print_check(derived)
    else:
        sys.stdout.write(exports(derived))
    if args.exports_to:
        Path(args.exports_to).write_text(exports(derived), encoding="utf-8")

    # Before the stderr lines below, because `make quota` merges the two streams and a buffered stdout would
    # put a warning above the figures it is about.
    sys.stdout.flush()

    for metric in derived["unreported_series"]:
        print(f"WARNING: the project reports no {metric} quota, so nothing above checks it. Read that as"
              " unknown and not as unlimited: this machine family uses its own quota and can bind"
              " before it, so if this project has one the figures above do not say so. Confirm with:"
              f" gcloud compute regions describe {derived['region']} --format='value(quotas)'",
              file=sys.stderr)

    for metric in derived["unreported_other"]:
        print(f"WARNING: the project reports no {metric} quota, and the nodes above are charged to it."
              " Nothing here bounds them against it, which is a table that was read short rather than a"
              " project without that limit.", file=sys.stderr)

    if not derived["checked"]:
        print("WARNING: no reported quota bounds the agent pools, so the ceiling above is the pools' own"
              " max_size and not a checked figure. That is a quota table that could not be read rather than"
              " a project without limits.", file=sys.stderr)

    if derived["pod_range_nodes"] is not None and derived["pod_range_nodes"] < derived["quota_agent_nodes"]:
        print(f"WARNING: the cluster's pod range holds {derived['pod_range_nodes']} agent node(s) and the"
              f" quota holds {derived['quota_agent_nodes']}, so the pod range binds first and raising a"
              " quota changes nothing. `tofu output agent_node_ceiling` in 1-cluster reports it. Unlike a"
              " quota, the pod range cannot be raised after the cluster is created: it takes a new cluster,"
              " so decide the figure before the first apply.", file=sys.stderr)

    for entry in derived["controller_short"]:
        print(f"ERROR: {entry['metric']} allows {entry['limit']:.0f} {entry['unit']} in"
              f" {entry['scope']}, which is {entry['nodes']} node(s) of the controller's"
              f" {', '.join(derived['controller']['machine_types'])}, and the controller pool asks for"
              f" {derived['controller']['max_size']}. Nothing runs at all until that quota is raised;"
              " the command is in README.md.", file=sys.stderr)
        return 1

    if derived["pool_agent_nodes"] < 1:
        print("ERROR: every agent pool has max_size 0, so no agent node can be created whatever the quota"
              " allows. Set a max_size in 1-cluster/variables.tf before deploying.", file=sys.stderr)
        return 1

    if derived["quota_agent_nodes"] < 1:
        held = (f"{derived['quota_metric_nodes']} node(s), and the controller needs"
                f" {derived['controller']['max_size']} of them,"
                if derived["quota_controller_pays"]
                else f"{derived['quota_metric_nodes']} agent node(s),")
        print(f"ERROR: {derived['quota_metric']} allows {derived['quota']:.0f} {derived['quota_unit']},"
              f" which is {held} so no agent can run at all. Raise that quota before deploying; the command"
              " is in README.md.", file=sys.stderr)
        return 1

    for entry in derived["over_ask"]:
        print(f"WARNING: the node pools may ask for {entry['demand']:.0f} {entry['unit']} of"
              f" {entry['metric']} and the project allows {entry['limit']:.0f}. The pools cannot all reach"
              " max_size, and the ceiling is not reported by the node pool, the managed autoscaler or"
              " Jenkins: an instance group that reaches it fails creation with a quota error, the autoscaler"
              " records a scale-up failure and backs off, and a build sees only a Pending pod.",
              file=sys.stderr)
        print(f"         Cap Jenkins at {derived['max_agent_nodes']} agents"
              " (agent.containerCap in .jenkins/k8s/jenkins-deployment.yaml), lower the pools' max_size in"
              f" 1-cluster/variables.tf, or raise {entry['metric']}. README.md has the request command.",
              file=sys.stderr)

    for entry in derived["over_headroom"]:
        print(f"WARNING: {entry['metric']} allows {entry['limit']:.0f} {entry['unit']} and"
              f" {entry['usage']:.0f} is already in use, so only {entry['headroom']:.0f} is left against an"
              f" ask of {entry['demand']:.0f}. The limit covers the ask and the headroom does not, so"
              " whatever else runs in this project decides whether these pools reach max_size.",
              file=sys.stderr)

    return 0

if __name__ == "__main__":
    sys.exit(main())
