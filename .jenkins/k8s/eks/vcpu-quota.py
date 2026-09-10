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
"""
How many nodes may this cluster run at once, given the account's on-demand vCPU quota?

The node groups ask for a number of nodes.  The account allows a number of vCPU.  Neither knows about
the other, and the account wins: an Auto Scaling group told to grow past the quota reports

    Could not launch On-Demand Instances. VcpuLimitExceeded - You have requested more vCPU capacity
    than your current vCPU limit of 384 allows

and the cluster autoscaler answers that by disabling scale-up on the group for thirty minutes.  What a
build sees is a pod that stays Pending.  Nothing in the node group, in the autoscaler or in Jenkins names
the quota, so the ceiling is invisible at every layer that could report it.  This script names it.

It lives beside the Makefile rather than in a numbered directory because both layers need the answer, and
they must not read it from each other: 2-platform caps the autoscaler with `max-nodes-total`, and 3-smoke
asserts that the pools do not ask for more than the account allows.  The Makefile is the join, so the
Makefile runs this and puts the answer in the environment; see README.md.

Two modes:

    ./vcpu-quota.py --pools .eks-agent-pools.json            shell `export` lines, for the Makefile
    ./vcpu-quota.py --pools .eks-agent-pools.json --check     the same figures as a table, for a human

There are two ceilings, and the lower one is what is published.  The quota is one; the node groups' own
`max_size` figures are the other, and a quota raised above them buys nothing, because no group will create
the nodes.  Publishing the quota's figure when the groups are lower would let Jenkins ask for pods that
wait for a node that cannot exist, which is the same churn loop with the node group in the account's place.

Exits 1 only when no agent can run at all: either the quota cannot hold one agent node, or every pool is at
`max_size` 0.  Pools that together ask for more than the quota allows are a warning and not a failure: that
cluster runs, it simply cannot reach the size its node groups are written for, and refusing to deploy it
would leave the operator with no cluster rather than a smaller one.  3-smoke reports the same thing against
a running cluster.  Exits 2 when the quota or an instance type could not be read.

Two limits worth knowing.  The quota is account-wide and region-wide, so anything else running in the
same account and region spends from the same 384; this script measures the ask, not what is left.  And
`L-1216C47A` covers only the standard instance families, so a pool on an accelerated family is counted
here against a quota that does not govern it, and says so.
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

# "Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances", counted in vCPU rather than in
# instances.  The name in the console is longer than the code and changes; the code does not.
QUOTA_SERVICE = "ec2"
QUOTA_CODE = "L-1216C47A"

# The families that quota governs.  A type outside this set has a quota of its own, under another code,
# and this script cannot check it.
STANDARD_FAMILY_LETTERS = frozenset("acdhimrtz")


def aws_json(args: list) -> dict:
    """One AWS CLI call returning JSON.  Raises CalledProcessError, which main() turns into a sentence."""
    output = subprocess.run(["aws"] + args + ["--output", "json"],
                            capture_output=True, text=True, check=True).stdout
    return json.loads(output)


def ondemand_vcpu_quota(region: str) -> float:
    """The account's applied on-demand standard vCPU quota in this region."""
    quota = aws_json(["service-quotas", "get-service-quota", "--region", region,
                      "--service-code", QUOTA_SERVICE, "--quota-code", QUOTA_CODE])
    return float(quota["Quota"]["Value"])


def instance_vcpus(instance_type: str, region: str) -> int:
    """The vCPU count of one instance type.

    The same EC2 call 3-smoke/check-pool-fit.py makes for the same instance types, and deliberately not
    shared with it: that script answers whether one pod fits one node, this one answers how many nodes the
    account allows, and a shared module between the join and a numbered layer would be a fourth thing to
    keep in step.
    """
    types = aws_json(["ec2", "describe-instance-types", "--region", region,
                      "--instance-types", instance_type])["InstanceTypes"]
    if not types:
        raise ValueError(f"EC2 reports no such instance type in {region}: {instance_type}")
    return int(types[0]["VCpuInfo"]["DefaultVCpus"])


def is_standard_family(instance_type: str) -> bool:
    return instance_type[:1].lower() in STANDARD_FAMILY_LETTERS


def group_vcpus(instance_types: list, region: str, cache: dict) -> int:
    """The vCPU count to charge a node of this group.

    The largest of the group's instance types, not the first.  A managed node group given several types
    may launch any of them, so the only figure that cannot under-count is the largest.  check-pool-fit.py
    takes the first instead, because its question is whether a pod fits and the answer there must not be
    flattered by a type the group might not launch.
    """
    counts = []
    for instance_type in instance_types:
        if instance_type not in cache:
            cache[instance_type] = instance_vcpus(instance_type, region)
        counts.append(cache[instance_type])
    return max(counts)


def derive(pools: dict, controller: dict, region: str) -> dict:
    """The ceiling, and the demand it is measured against."""
    cache = {}
    rows = []

    # `max_size` is what layer 1 created the node groups with; `declared_max_size` is what var.agent_pools
    # asked for, and the two differ when var.size_pools_to_quotas scaled the pools.  Absent from older
    # output, so it falls back to max_size and this reads a pre-scaling JSON unchanged.
    for name, group in sorted(pools.items()):
        rows.append({
            "name": name,
            "instance_types": group["instance_types"],
            "max_size": int(group["max_size"]),
            "declared_max_size": int(group.get("declared_max_size", group["max_size"])),
            "vcpus": group_vcpus(group["instance_types"], region, cache),
        })
    controller_row = {
        "name": "controller",
        "instance_types": controller["instance_types"],
        "max_size": int(controller["max_size"]),
        "declared_max_size": int(controller.get("declared_max_size", controller["max_size"])),
        "vcpus": group_vcpus(controller["instance_types"], region, cache),
    }

    quota = ondemand_vcpu_quota(region)
    demand = sum(row["vcpus"] * row["max_size"] for row in rows + [controller_row])

    # The ceiling is a node count, and the pools do not all run the same instance type, so it is computed
    # against the largest.  Any other choice can be exceeded: 48 nodes of 8 vCPU is 384, and 48 nodes that
    # happen to include one 16 vCPU node is 392.
    largest = max(row["vcpus"] for row in rows + [controller_row])
    quota_nodes = int(quota // largest)
    quota_agent_nodes = quota_nodes - controller_row["max_size"]

    # The other ceiling, and either one can be the lower.  A quota raised above the pools' ask does not give
    # Jenkins more agents, because no node group will create them: the agent pods beyond this figure wait for
    # a node that cannot exist, which is the churn loop again with the node group in the account's place.
    # Publishing the quota's figure unclamped would move that fault rather than fix it.
    pool_agent_nodes = sum(row["max_size"] for row in rows)
    declared_agent_nodes = sum(row["declared_max_size"] for row in rows)

    return {
        "region": region,
        "quota": quota,
        "demand": demand,
        "rows": rows,
        "controller": controller_row,
        "largest_vcpus": largest,
        "quota_nodes": quota_nodes,
        "quota_agent_nodes": quota_agent_nodes,
        "pool_agent_nodes": pool_agent_nodes,
        "declared_agent_nodes": declared_agent_nodes,
        "scaled": declared_agent_nodes != pool_agent_nodes,
        "binds": "quota" if quota_agent_nodes < pool_agent_nodes else "pools",
        "max_nodes_total": min(quota_nodes, pool_agent_nodes + controller_row["max_size"]),
        "max_agent_nodes": min(quota_agent_nodes, pool_agent_nodes),
        "nonstandard": [instance_type for instance_type in sorted(cache)
                        if not is_standard_family(instance_type)],
    }


def exports(derived: dict) -> str:
    """The form ./Makefile puts these in the environment with, the same as layer 1's `environment`."""
    return "\n".join([
        f"export EKS_ONDEMAND_VCPU_QUOTA='{int(derived['quota'])}'",
        f"export EKS_ONDEMAND_VCPU_DEMAND='{derived['demand']}'",
        f"export EKS_MAX_NODES_TOTAL='{derived['max_nodes_total']}'",
        f"export EKS_MAX_AGENT_NODES='{derived['max_agent_nodes']}'",
    ]) + "\n"


def print_check(derived: dict) -> None:
    print(f"On-demand standard vCPU quota ({QUOTA_CODE}) in {derived['region']}:"
          f" {int(derived['quota'])}")
    for row in derived["rows"] + [derived["controller"]]:
        print(f"  {row['name']:<10}  max {row['max_size']:>4} nodes"
              f"  x {row['vcpus']:>3} vCPU ({', '.join(row['instance_types'])})"
              f"  = {row['vcpus'] * row['max_size']:>5} vCPU")
    print(f"  {'total':<10}  {' ' * 32}  = {derived['demand']:>5} vCPU")
    print()
    print(f"At {derived['largest_vcpus']} vCPU per node, the quota holds {derived['quota_nodes']} nodes,"
          f" of which {derived['quota_agent_nodes']} may be agents.")
    print(f"The node groups can create {derived['pool_agent_nodes']} agent nodes.")
    if derived["scaled"]:
        print(f"Layer 1 scaled the pools: var.agent_pools asked for {derived['declared_agent_nodes']}"
              f" agent nodes and the node groups were created with {derived['pool_agent_nodes']}."
              " `tofu output agent_node_ceiling` in 1-cluster says which ceiling bound.")
    print()
    if derived["binds"] == "quota":
        print(f"The quota binds: {derived['max_agent_nodes']} agents at once.")
    else:
        print(f"The node groups bind: {derived['max_agent_nodes']} agents at once, with the quota holding"
              f" {derived['quota_agent_nodes'] - derived['pool_agent_nodes']} more.")
        print("Raising an agent pool's max_size needs no quota request while that is true.")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Derive the node ceiling the account's on-demand vCPU quota imposes.")
    parser.add_argument("--pools", required=True,
                        help="JSON from `tofu output -json agent_node_groups` in 1-cluster."
                             " Defaults to $EKS_AGENT_POOLS_FILE through ./Makefile.")
    parser.add_argument("--region", help="AWS region. Defaults to $AWS_REGION.")
    parser.add_argument("--controller-instance-types",
                        help="Space separated, as layer 1 exports EKS_CONTROLLER_INSTANCE_TYPES."
                             " The controller's node counts against the same quota as an agent's.")
    parser.add_argument("--controller-max-size", type=int,
                        help="Defaults to $EKS_CONTROLLER_MAX_SIZE, then to 1.")
    parser.add_argument("--check", action="store_true",
                        help="Print the figures as a table instead of `export` lines.")
    parser.add_argument("--exports-to",
                        help="Also write the `export` lines to this file, so that one AWS call serves both"
                             " a reader and the environment ./Makefile builds.")
    args = parser.parse_args()

    region = args.region or os.environ.get("AWS_REGION")
    if not region:
        print("A region is required: pass --region or set AWS_REGION.", file=sys.stderr)
        return 2

    controller_types = (args.controller_instance_types
                        or os.environ.get("EKS_CONTROLLER_INSTANCE_TYPES", "")).split()
    if not controller_types:
        print("The controller's instance type is required: pass --controller-instance-types or set"
              " EKS_CONTROLLER_INSTANCE_TYPES. `make env` exports it; see 1-cluster/outputs.tf.",
              file=sys.stderr)
        return 2
    controller = {
        "instance_types": controller_types,
        "max_size": args.controller_max_size
                    if args.controller_max_size is not None
                    else int(os.environ.get("EKS_CONTROLLER_MAX_SIZE", "1")),
    }

    try:
        pools = json.loads(Path(args.pools).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        print(f"Could not read --pools {args.pools}: {error}", file=sys.stderr)
        return 2

    # Caught so that the answer is a sentence naming the credential.  Uncaught, an expired session prints
    # a CalledProcessError traceback in the middle of `make platform`, which reads as a fault here.
    try:
        derived = derive(pools, controller, region)
    except subprocess.CalledProcessError as error:
        print(f"ERROR: AWS would not answer in {region}, so the vCPU ceiling is unknown."
              " Check the credential first: aws sts get-caller-identity", file=sys.stderr)
        print(f"       {(error.stderr or '').strip() or error}", file=sys.stderr)
        return 2
    except (KeyError, ValueError) as error:
        print(f"ERROR: could not derive the vCPU ceiling: {error}", file=sys.stderr)
        return 2

    if args.check:
        print_check(derived)
    else:
        sys.stdout.write(exports(derived))
    if args.exports_to:
        Path(args.exports_to).write_text(exports(derived), encoding="utf-8")

    for instance_type in derived["nonstandard"]:
        print(f"WARNING: {instance_type} is not a standard instance family, so {QUOTA_CODE} does not"
              " govern it and the figures above neither charge nor check it correctly. Find its own"
              " quota code with: aws service-quotas list-service-quotas --service-code ec2",
              file=sys.stderr)

    if derived["quota_agent_nodes"] < 1:
        print(f"ERROR: the quota holds {derived['quota_nodes']} node(s) in total and the controller"
              f" needs {derived['controller']['max_size']}, so no agent can run at all. Raise the quota"
              " before deploying; the command is in README.md.", file=sys.stderr)
        return 1

    # A different fault with the same symptom, and it is not the account's: every agent pool is at
    # max_size 0, so the quota is irrelevant and no build can run either.  Named separately, because
    # "raise the quota" would be wrong advice here.
    if derived["pool_agent_nodes"] < 1:
        print("ERROR: every agent pool has max_size 0, so no agent node can be created whatever the quota"
              " allows. Set a max_size in 1-cluster/variables.tf before deploying.", file=sys.stderr)
        return 1

    if derived["demand"] > derived["quota"]:
        print(f"WARNING: the node groups may ask for {derived['demand']} vCPU and the account allows"
              f" {int(derived['quota'])}. The pools cannot all reach max_size, and the ceiling is not"
              " reported by the node group, the autoscaler or Jenkins: an Auto Scaling group that reaches"
              " it fails with VcpuLimitExceeded and the autoscaler then disables scale-up on that group"
              " for thirty minutes, which a build sees only as a Pending pod.", file=sys.stderr)
        print(f"         Cap Jenkins at {derived['max_agent_nodes']} agents"
              " (agent.containerCap in .jenkins/k8s/jenkins-deployment.yaml), cap the autoscaler at"
              f" {derived['max_nodes_total']} nodes (EKS_MAX_NODES_TOTAL, which ./Makefile exports), or"
              " raise the quota. README.md has the request command.", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
