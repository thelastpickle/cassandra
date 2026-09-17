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
"""Check agent pod requests and provisioning deadlines against node-pool capacity."""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import yaml

# .jenkins/k8s/jenkins-deployment.yaml, two directories up from 3-smoke/.
DEFAULT_VALUES = Path(__file__).resolve().parent.parent.parent / "jenkins-deployment.yaml"

# The cloud-neutral arithmetic, shared with the other clouds' directories; see ../../shared/README.md.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from k8s_values import (GIB, MIB, cpu_millicores, human_cpu,  # noqa: E402 - after the sys.path line above
                        human_memory, human_storage, memory_bytes)

# The kubelet's default hard eviction threshold on memory.  Held back from allocatable, so a pod sized to
# the last byte of allocatable is a pod the node evicts.
EVICTION_MEMORY_MIB = 100

EPHEMERAL_RESERVED_FRACTION = 0.10
EPHEMERAL_RESERVED_GIB = 2

# Under this much headroom on any dimension, in estimated mode, a pass is reported as marginal.
MARGINAL_HEADROOM = 0.10

COLD_NODE_SECONDS = 300

# The kubernetes-plugin's own default, applied to a template that names no slaveConnectTimeout.  Well under
# the floor above, so silence in a template is a failure here rather than a value this script cannot judge.
PLUGIN_CONNECT_TIMEOUT = 100


def agent_requests(values_path: Path) -> dict:
    """The most demanding agent pod a pool must hold, keyed by the size in its template's nodeSelector."""
    with values_path.open(encoding="utf-8") as handle:
        values = yaml.safe_load(handle)

    requests = {}
    for name, raw in ((values.get("agent") or {}).get("podTemplates") or {}).items():
        try:
            template = yaml.safe_load(raw)[0]
        except (yaml.YAMLError, IndexError, TypeError):
            print(f"WARNING: could not parse podTemplate {name}, skipping it", file=sys.stderr)
            continue

        selector = dict(pair.split("=", 1) for pair in str(template.get("nodeSelector", "")).split(",")
                        if "=" in pair)
        size = next((key.rsplit(".", 1)[1] for key in selector
                     if key.startswith("cassandra.jenkins.agent.")), None)
        if size is None:
            print(f"WARNING: podTemplate {name} has no cassandra.jenkins.agent.<size> nodeSelector,"
                  " so it is tied to no pool and is not checked", file=sys.stderr)
            continue

        total = {"cpu": 0.0, "memory": 0, "ephemeral": 0}
        for container in template.get("containers") or []:
            total["cpu"] += cpu_millicores((container.get("resourceRequestCpu") if container.get("resourceRequestCpu") not in (None, "") else container.get("resourceLimitCpu", 0)))
            total["memory"] += memory_bytes((container.get("resourceRequestMemory") if container.get("resourceRequestMemory") not in (None, "") else container.get("resourceLimitMemory", 0)))
            total["ephemeral"] += memory_bytes((container.get("resourceRequestEphemeralStorage") if container.get("resourceRequestEphemeralStorage") not in (None, "") else container.get("resourceLimitEphemeralStorage", 0)))
        held = requests.setdefault(size, {"cpu": 0.0, "memory": 0, "ephemeral": 0, "templates": []})
        for resource in ("cpu", "memory", "ephemeral"):
            held[resource] = max(held[resource], total[resource])
        held["templates"].append(name)
    for held in requests.values():
        held["template"] = ", ".join(sorted(held["templates"]))
    return requests


def connect_deadlines(values_path: Path) -> dict:
    """Every deadline that ends a pod's wait for a node, in seconds."""
    with values_path.open(encoding="utf-8") as handle:
        values = yaml.safe_load(handle)

    agent = values.get("agent") or {}
    deadlines = {}
    for name, raw in (agent.get("podTemplates") or {}).items():
        try:
            template = yaml.safe_load(raw)[0]
        except (yaml.YAMLError, IndexError, TypeError):
            continue
        given = template.get("slaveConnectTimeout")
        deadlines[name] = (PLUGIN_CONNECT_TIMEOUT if given in (None, "") else int(given),
                           given is not None)

    wait_for_pod = agent.get("waitForPodSec")
    return {
        "templates": deadlines,
        "waitForPodSec": None if wait_for_pod in (None, "") else int(wait_for_pod),
    }


def check_deadlines(deadlines: dict) -> list:
    """The deadlines that sit below a cold node's time to ready, each as one sentence."""
    faults = []

    wait_for_pod = deadlines["waitForPodSec"]
    if wait_for_pod is not None and wait_for_pod < COLD_NODE_SECONDS:
        faults.append(
            f"agent.waitForPodSec is {wait_for_pod}s, under the {COLD_NODE_SECONDS}s a cold node needs."
            " It applies to every template, so raising a template's own slaveConnectTimeout above it"
            " changes nothing: the shorter deadline is the one that fires.")

    for name, (seconds, declared) in sorted(deadlines["templates"].items()):
        if seconds >= COLD_NODE_SECONDS:
            continue
        source = f"slaveConnectTimeout is {seconds}s" if declared else \
            f"names no slaveConnectTimeout, so the plugin applies {seconds}s"
        faults.append(
            f"podTemplate {name} {source}, under the {COLD_NODE_SECONDS}s a cold node needs. Every pod"
            " that waits for a node is deleted and requested again, which reaches the node group's"
            " maxSize, corrupts the plugin's instanceCap accounting, and connects no agent.")

    return faults


def kubectl(args: list, context: str = None) -> str:
    command = ["kubectl"]
    if context:
        command += ["--context", context]
    command += args
    return subprocess.run(command, capture_output=True, text=True, check=True).stdout


def aws_instance_type(instance_type: str, region: str) -> dict:
    """vCPUs, memory and network limits for one instance type, from the EC2 API."""
    output = subprocess.run(
        ["aws", "ec2", "describe-instance-types", "--region", region,
         "--instance-types", instance_type, "--output", "json"],
        capture_output=True, text=True, check=True).stdout
    types = json.loads(output).get("InstanceTypes") or []
    if not types:
        raise ValueError(f"EC2 reports no such instance type in {region}: {instance_type}")
    return types[0]


def eks_reserved_cpu(vcpus: int) -> float:
    """Millicores the EKS AMI reserves for the kubelet and the container runtime."""
    percentages = [6.0, 1.0, 0.5, 0.5]
    reserved = 0.0
    for core in range(vcpus):
        share = percentages[core] if core < len(percentages) else 0.25
        reserved += 10.0 * share
    return reserved


def eks_max_pods(interfaces: int, addresses_per_interface: int, vcpus: int) -> int:
    """The pod ceiling the EKS AMI computes, which is what its memory reservation is a function of."""
    cap = 110 if vcpus < 30 else 250
    return min(interfaces * (addresses_per_interface - 1) + 2, cap)


def eks_reserved_memory_mib(max_pods: int) -> int:
    """MiB the EKS AMI reserves for the kubelet: 11 MiB per pod it could run, plus 255."""
    return 11 * max_pods + 255


def estimated_capacity(instance_type: str, region: str, disk_gib: int) -> dict:
    """What one node of this pool is modelled to offer a pod."""
    instance = aws_instance_type(instance_type, region)
    vcpus = int(instance["VCpuInfo"]["DefaultVCpus"])
    # Not `memory_bytes`: that is the imported quantity parser, and shadowing it here would break any later
    # call to it in this function.
    node_memory = int(instance["MemoryInfo"]["SizeInMiB"]) * MIB
    network = instance.get("NetworkInfo") or {}
    max_pods = eks_max_pods(int(network.get("MaximumNetworkInterfaces", 1)),
                            int(network.get("Ipv4AddressesPerInterface", 1)),
                            vcpus)

    reserved_memory = (eks_reserved_memory_mib(max_pods) + EVICTION_MEMORY_MIB) * MIB
    ephemeral = (disk_gib * (1 - EPHEMERAL_RESERVED_FRACTION) - EPHEMERAL_RESERVED_GIB) * GIB

    return {
        "mode": "estimated",
        "cpu": vcpus * 1000 - eks_reserved_cpu(vcpus),
        "memory": node_memory - reserved_memory,
        "ephemeral": ephemeral,
        "note": f"model: {vcpus} vCPU, {node_memory // MIB} MiB, maxPods {max_pods}",
    }


def measured_capacity(label: str, disk_gib: int, context: str = None) -> dict:
    """What a running node of this pool actually offers a pod, or None when the pool is at zero."""
    nodes = json.loads(kubectl(["get", "nodes", "-l", f"{label}=true", "-o", "json"], context))
    ready = [node for node in nodes.get("items", [])
             if any(condition.get("type") == "Ready" and condition.get("status") == "True"
                    for condition in (node.get("status") or {}).get("conditions", []))]
    if not ready:
        return None

    node = ready[0]
    name = node["metadata"]["name"]
    allocatable = (node.get("status") or {}).get("allocatable") or {}

    pods = json.loads(kubectl(["get", "pods", "--all-namespaces", "-o", "json",
                               "--field-selector", f"spec.nodeName={name}"], context))
    daemon = {"cpu": 0.0, "memory": 0, "ephemeral": 0}
    daemon_names = []
    for pod in pods.get("items", []):
        owners = (pod.get("metadata") or {}).get("ownerReferences") or []
        if not any(owner.get("kind") == "DaemonSet" for owner in owners):
            continue
        daemon_names.append(pod["metadata"]["name"])
        for container in (pod.get("spec") or {}).get("containers", []):
            container_requests = ((container.get("resources") or {}).get("requests") or {})
            daemon["cpu"] += cpu_millicores(container_requests.get("cpu", 0))
            daemon["memory"] += memory_bytes(container_requests.get("memory", 0))
            daemon["ephemeral"] += memory_bytes(container_requests.get("ephemeral-storage", 0))

    ephemeral = (disk_gib * (1 - EPHEMERAL_RESERVED_FRACTION) - EPHEMERAL_RESERVED_GIB) * GIB

    return {
        "mode": "measured",
        "cpu": cpu_millicores(allocatable.get("cpu", 0)) - daemon["cpu"],
        "memory": memory_bytes(allocatable.get("memory", 0)) - daemon["memory"],
        "ephemeral": ephemeral - daemon["ephemeral"],
        "note": f"node {name}, less {len(daemon_names)} DaemonSet pods",
    }


def evaluate(size: str, pool: dict, request: dict, capacity: dict) -> dict:
    """One pool's verdict: how many agents fit, on which resource it runs out, and how close it is."""
    dimensions = {
        "cpu": (request["cpu"], capacity["cpu"], human_cpu),
        "memory": (request["memory"], capacity["memory"], human_memory),
        "ephemeral": (request["ephemeral"], capacity["ephemeral"], human_storage),
    }

    fits = {}
    headroom = {}
    for name, (wanted, available, _) in dimensions.items():
        fits[name] = int(available // wanted) if wanted > 0 else None
        headroom[name] = (available - wanted) / available if available > 0 else -1.0

    # A pod template that requests nothing at all fits, in the sense the scheduler means: it is
    # BestEffort on every dimension and lands anywhere.  There is nothing to compare, so nothing fails.
    countable = [count for count in fits.values() if count is not None]
    agents = min(countable) if countable else 1
    binding = min(((headroom[name], name) for name in dimensions))[1]

    if agents == 0:
        verdict = "FAILS"
    elif capacity["mode"] == "estimated" and headroom[binding] < MARGINAL_HEADROOM:
        verdict = "MARGINAL"
    else:
        verdict = "fits"

    return {
        "size": size,
        "pool": pool,
        "request": request,
        "capacity": capacity,
        "dimensions": dimensions,
        "agents": agents,
        "binding": binding,
        "headroom": headroom,
        "verdict": verdict,
    }


def report(results: list) -> None:
    for result in results:
        pool = result["pool"]
        groups = ", ".join(pool["node_group_names"])
        print(f"{result['size']}  ({groups}, {', '.join(pool['instance_types'])},"
              f" {pool['disk_gib']} GiB disk, max {pool['max_size']} nodes across them)")
        print(f"  pod template  {result['request']['template']}")
        print(f"  capacity      {result['capacity']['mode']}: {result['capacity']['note']}")
        for name, (wanted, available, render) in result["dimensions"].items():
            marker = " <- binds" if name == result["binding"] else ""
            print(f"  {name:<12}  requests {render(wanted):>10}  of {render(available):>10} available"
                  f"  ({result['headroom'][name] * 100:5.1f}% spare){marker}")
        print(f"  verdict       {result['verdict']}: {result['agents']} agent(s) fit per node,"
              " and the podAntiAffinity allows one")
        print()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check that one agent pod fits on one node of its EKS node group.")
    parser.add_argument("--pools", required=True,
                        help="JSON from `tofu output -json agent_node_groups` in ../1-cluster.")
    parser.add_argument("--region", help="AWS region, for the instance-type lookup. Defaults to $AWS_REGION.")
    parser.add_argument("--values", default=str(DEFAULT_VALUES),
                        help="Helm values holding the agent podTemplates. Defaults to .jenkins/k8s/jenkins-deployment.yaml.")
    parser.add_argument("--kubecontext", help="kubectl context to read live nodes from.")
    parser.add_argument("--estimate-only", action="store_true",
                        help="Model every pool from the instance type. Runs no kubectl, so it needs no cluster.")
    args = parser.parse_args()

    region = args.region or os.environ.get("AWS_REGION")
    if not region:
        print("A region is required: pass --region or set AWS_REGION.", file=sys.stderr)
        return 2

    pools = json.loads(Path(args.pools).read_text(encoding="utf-8"))
    requests = agent_requests(Path(args.values))
    parsed_deadlines = connect_deadlines(Path(args.values))
    deadline_faults = check_deadlines(parsed_deadlines)
    capacity_faults = []
    if "small" in pools:
        workers = int(pools["small"].get("small_workers_per_build", 3))
        if pools["small"]["max_size"] <= workers:
            capacity_faults.append(
                f"the small pool needs at least {workers + 1} agent slots: one outer pipeline"
                f" holds its agent while JAR tasks request {workers} workers. Apply the corrected pool sizes,"
                " then run `make jenkins` to update the template caps.")

    results, missing = [], []
    for size, pool in sorted(pools.items()):
        request = requests.get(size)
        if request is None:
            missing.append(size)
            continue

        capacity = None
        if not args.estimate_only:
            try:
                capacity = measured_capacity(pool["node_selector_label"], pool["disk_gib"], args.kubecontext)
            except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError) as error:
                print(f"WARNING: could not read live nodes for {size}, falling back to the model: {error}",
                      file=sys.stderr)
        if capacity is None:
            try:
                capacity = estimated_capacity(pool["instance_types"][0], region, pool["disk_gib"])
            except subprocess.CalledProcessError as error:
                print(f"ERROR: EC2 would not describe {pool['instance_types'][0]} in {region}, so pool"
                      f" {size} cannot be checked either way. Check the credential first:"
                      " aws sts get-caller-identity", file=sys.stderr)
                print(f"       {(error.stderr or '').strip() or error}", file=sys.stderr)
                return 2

        results.append(evaluate(size, pool, request, capacity))

    report(results)

    print("deadlines on a pod's wait for a node, the shorter of which fires:")
    print(f"  agent.waitForPodSec      {parsed_deadlines['waitForPodSec'] or 'unset'}s,"
          " for every template")
    for name, (seconds, declared) in sorted(parsed_deadlines["templates"].items()):
        note = "" if declared else "  (unset: the plugin's own default)"
        print(f"  {name:<24} {seconds}s{note}")
    print(f"  floor for a cold node    {COLD_NODE_SECONDS}s")
    print()

    failed = [result for result in results if result["verdict"] == "FAILS"]
    marginal = [result for result in results if result["verdict"] == "MARGINAL"]

    for size in missing:
        print(f"ERROR: pool {size!r} exists but no agent podTemplate selects"
              f" cassandra.jenkins.agent.{size}, so nothing will ever run on it", file=sys.stderr)
    for result in marginal:
        print(f"WARNING: {result['size']} passes on {result['binding']} with"
              f" {result['headroom'][result['binding']] * 100:.1f}% to spare, in estimated mode."
              " The estimate is a model, not a measurement: treat this as a pool that may not fit."
              " Confirm it against a running node before relying on it.", file=sys.stderr)
    for result in failed:
        print(f"ERROR: {result['size']} fits no agent at all. Its pod template requests"
              f" {result['dimensions'][result['binding']][2](result['request'][result['binding']])} of"
              f" {result['binding']}, against"
              f" {result['dimensions'][result['binding']][2](result['capacity'][result['binding']])}"
              f" on a {result['pool']['instance_types'][0]}."
              " Every agent requested for this pool will stay Pending.", file=sys.stderr)
    for fault in capacity_faults + deadline_faults:
        print(f"ERROR: {fault}", file=sys.stderr)

    if failed or missing or capacity_faults or deadline_faults:
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
