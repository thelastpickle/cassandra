#!/usr/bin/env python3
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

"""Generate GKE Jenkins values from site settings and applied pool capacities."""

import argparse
import copy
import json
import re
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    sys.exit("PyYAML is needed. It is a .build/run-ci dependency too, so install it as run-ci asks.")

# Helm's merge rule, shared with the other clouds' directories and with ../controller-fit.py, which has to
# compute the same answer this does; see ../../shared/README.md.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from k8s_values import deep_merge, load_yaml  # noqa: E402 - after the sys.path line above

REQUESTS_PER_AGENT_NUMERATOR = 32
REQUESTS_PER_AGENT_DENOMINATOR = 3


def tls_notice(certificate_map, hostname):
    """What carries TLS on this cluster and what the operator has left to do, as lines of prose."""
    if certificate_map:
        return [
            "TLS: certificate map %r is NOT applied by these values, and cannot be." % certificate_map,
            "A GKE Service of type LoadBalancer is a passthrough Network Load Balancer, which holds no",
            "certificate, and GKE Ingress does not read a Certificate Manager map.  Attach the map to a",
            "Gateway with the networking.gke.io/certmap annotation, add an HTTPRoute to the cassius-jenkins",
            "Service, then set controller.jenkinsUrl to https://%s in the site overrides file."
            % (hostname or "<the public name>"),
        ]
    return [
        "TLS: none.  The Service serves plain HTTP on port 80, which is what run-ci connects to.",
        "../1-cluster creates a Certificate Manager map when var.jenkins_hostname is set; serving it",
        "needs a Gateway, which no annotation in these values can stand in for.",
    ]


def build_overlay(hostname, container_cap=None):
    """The keys a public name needs, and the project's agent ceiling."""

    # GKE uses the Service load balancer, with or without a DNS name.
    # Disable the shared chart's Ingress, which would need a separate load balancer and controller.
    overlay = {"controller": {"ingress": {"enabled": False}}}

    if container_cap is not None:
        requests_per_host = -(-container_cap * REQUESTS_PER_AGENT_NUMERATOR
                              // REQUESTS_PER_AGENT_DENOMINATOR)
        overlay["agent"] = {
            "containerCap": container_cap,
            "maxRequestsPerHostStr": str(requests_per_host),
        }

    if not hostname:
        # No public name: Jenkins is reachable at the load balancer's own address, which Kubernetes
        # chooses and this script cannot know.
        return overlay

    annotations = {
        "external-dns.alpha.kubernetes.io/hostname": hostname,
    }

    # Assigned into the overlay rather than replacing it: the agent ceiling above is already in there.
    overlay["controller"].update({
        "jenkinsUrl": "http://%s" % hostname,
        "serviceAnnotations": annotations,
    })

    return overlay

# `instanceCap: 20` and `instanceCapStr: "20"` inside a podTemplate string, anchored to their own line so
# that neither `containerCap` above nor any prose in a comment can match.
INSTANCE_CAP_LINE = re.compile(r"^(?P<indent>[ \t]*)instanceCap:[ \t]*\S+[ \t]*$", re.MULTILINE)
INSTANCE_CAP_STR_LINE = re.compile(r"^(?P<indent>[ \t]*)instanceCapStr:[ \t]*\S+[ \t]*$", re.MULTILINE)

# Which pool a podTemplate selects.  run-ci's own check_agent_capacity attributes a template to a size the
# same way, from this label in the template's nodeSelector, so the two agree by construction.
NODE_SELECTOR_SIZE = re.compile(r"cassandra\.jenkins\.agent\.(?P<size>[a-z0-9]+)\s*=\s*true")


def scaled_pod_templates(deployment, pools, container_cap=None):
    """Match template caps to pool capacity and reserve small workers for concurrent pipelines."""
    templates = deployment.get("agent", {}).get("podTemplates", {})
    scaled = {}
    for name, body in templates.items():
        if not isinstance(body, str):
            continue
        selector = NODE_SELECTOR_SIZE.search(body)
        if not selector:
            # A template that selects no agent pool is not ours to size.  run-ci warns about the same case.
            continue
        pool = pools.get(selector.group("size"))
        if not pool or "max_size" not in pool:
            continue
        cap = int(pool["max_size"])
        rewritten = INSTANCE_CAP_LINE.sub(lambda match: f"{match.group('indent')}instanceCap: {cap}", body)
        rewritten = INSTANCE_CAP_STR_LINE.sub(
            lambda match: f"{match.group('indent')}instanceCapStr: \"{cap}\"", rewritten)
        if rewritten != body:
            scaled[name] = rewritten
    if "small" in pools:
        split_pipeline_template(deployment, scaled, pools["small"], container_cap)
    return scaled


def small_agent_caps(pool, container_cap=None):
    """Maximise admitted pipelines while retaining the configured worker budget for each."""
    workers_per_build = int(pool.get("small_workers_per_build", 3))
    if workers_per_build < 1:
        raise ValueError("small_workers_per_build must be positive")
    slots = int(pool["max_size"])
    budget = min(slots, container_cap) if container_cap is not None else slots
    concurrent = budget // (workers_per_build + 1)
    if concurrent < 1:
        raise ValueError(f"the small pool and container-cap need at least {workers_per_build + 1} slots "
                         "for one pipeline and its workers; apply the pool sizes first")
    return concurrent, budget - concurrent


def split_pipeline_template(deployment, scaled, pool, container_cap=None):
    """Reserve separate template caps for outer pipelines and their small workers."""
    concurrent, workers = small_agent_caps(pool, container_cap)
    templates = dict(deployment["agent"]["podTemplates"])
    templates.update(scaled)
    candidates = []
    for key, body in templates.items():
        for template in yaml.safe_load(body):
            if "cassandra-small" in template.get("label", "").split():
                candidates.append((key, template))
    if len(candidates) != 1 or "agent-dind-pipeline" in templates:
        raise ValueError("expected one cassandra-small template and no agent-dind-pipeline override")
    key, worker = candidates[0]
    selector = NODE_SELECTOR_SIZE.search(worker.get("nodeSelector", ""))
    if (not selector or selector.group("size") != "small"
            or "cassandra-amd64-small" not in worker["label"].split()
            or len(yaml.safe_load(templates[key])) != 1):
        raise ValueError("cassandra-small must share one small-pool template with cassandra-amd64-small")

    pipeline = copy.deepcopy(worker)
    pipeline.update(name="agent-dind-pipeline", id="gke-pipeline", label="cassandra-small")
    worker.update(id="gke-small-worker",
                  label=" ".join(label for label in worker["label"].split() if label != "cassandra-small"))
    for name, template, cap in (("agent-dind-pipeline", pipeline, concurrent),
                                (key, worker, workers)):
        template.update(instanceCap=cap, instanceCapStr=str(cap), nodeUsageMode="EXCLUSIVE")
        scaled[name] = yaml.safe_dump([template], sort_keys=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base", required=True,
                       help="jenkins-gke-overrides.yaml, the committed GCP values this is built on")
    parser.add_argument("--out", required=True, help="file to write")
    parser.add_argument("--hostname", default="",
                       help="the public name, as `ci.example.org`. Omit to leave Jenkins reachable at the "
                            "load balancer's own address")
    parser.add_argument("--certificate-map", default="",
                       help="the Certificate Manager map ../1-cluster created for --hostname. It changes no "
                            "key in the output: TLS on GKE is not a Service annotation, so this is recorded "
                            "in the generated file's header and printed as the step left to take. See the "
                            "README for the Gateway it needs")
    parser.add_argument("--container-cap", type=int,
                       help="agent.containerCap: the most agent pods this project can hold at once, which "
                            "is what ../vcpu-quota.py derives from its Compute Engine vCPU quota. Omit to "
                            "leave the shared default in jenkins-deployment.yaml alone")
    parser.add_argument("--deployment",
                       help="jenkins-deployment.yaml, read with --pools for the podTemplate strings whose "
                            "instanceCap is rewritten to its pool's created size. Needed by --pools and "
                            "read for nothing else, the sibling's controller.targetPort having no counterpart "
                            "here")
    parser.add_argument("--pools",
                       help="JSON from `tofu output -json agent_node_pools` in ../1-cluster. Caps follow "
                            "the applied pool sizes; small slots are divided between outer pipelines "
                            "and their worker budgets. Needs --deployment. Omit to leave the shared "
                            "templates unchanged")
    args = parser.parse_args()
    if args.pools and not args.deployment:
        parser.error("--pools requires --deployment")

    base = load_yaml(args.base)
    deployment = load_yaml(args.deployment) if args.deployment else {}

    overlay = build_overlay(args.hostname, args.container_cap)
    if (base.get("controller") or {}).get("jenkinsUrl"):
        overlay.get("controller", {}).pop("jenkinsUrl", None)

    # The per-pool caps, after the cluster-wide one: both live under `agent`, and this must not replace the
    # containerCap build_overlay just put there.
    if args.pools and deployment:
        try:
            with open(args.pools, encoding="utf-8") as handle:
                pools = json.load(handle)
            if "small" not in pools:
                raise ValueError("the small pool is missing; apply layer 1 before installing Jenkins")
            effective = deep_merge(deep_merge(deployment, base), overlay)
            container_cap = int(effective["agent"]["containerCap"])
            concurrent, workers = small_agent_caps(pools["small"], container_cap)
            scaled = scaled_pod_templates(deployment, pools, container_cap)
        except (OSError, ValueError) as error:
            parser.error(str(error))
        if scaled:
            overlay.setdefault("agent", {})["podTemplates"] = scaled
            for name in sorted(scaled):
                print("%s: instanceCap set from the pool layer 1 created" % name, file=sys.stderr)
            print(f"At most {concurrent} pipelines hold outer agents; "
                  f"{workers} small worker slots remain.")

    values = deep_merge(base, overlay)

    jenkins_url = (values.get("controller") or {}).get("jenkinsUrl", "")
    notice = tls_notice(args.certificate_map, args.hostname)
    if jenkins_url.startswith("https://"):
        notice = ["TLS: using the site's HTTPS Jenkins URL; TLS termination must be configured separately."]
    header = (
        "# Generated by %s. Do not edit, and do not commit: this file names a Google project.\n"
        "#\n"
        "# Built from %s, plus what only the project knows: the public name\n"
        "# ../1-cluster made, and the agent ceiling ../vcpu-quota.py derived.\n"
        "# Rebuilt by every `make jenkins`.\n"
        "#\n"
        "%s"
        % (parser.prog, args.base, "".join("# %s\n" % line for line in notice))
    )
    with open(args.out, "w", encoding="utf-8") as handle:
        handle.write(header)
        yaml.safe_dump(values, handle, default_flow_style=False, sort_keys=True)

    if jenkins_url:
        print("Wrote %s: Jenkins reports itself as %s" % (args.out, jenkins_url))
    else:
        print("Wrote %s: no public name, so Jenkins answers at the load balancer's own address."
              % args.out)
    for line in notice:
        print(line)

    if args.container_cap is not None:
        print("Agents are capped at %d at once, which is what this project's vCPU quota holds."
              % args.container_cap)

if __name__ == "__main__":
    main()
