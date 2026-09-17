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

"""Write the Helm values that hold what only this account knows: a public name, TLS for it, and the
number of agents the account's vCPU quota allows.

Run through ../Makefile, which fills every argument from `tofu output` and from ../vcpu-quota.py:

    make -C .jenkins/k8s/eks jenkins

Two constraints together are why this script exists rather than a second committed values file.

`.build/run-ci` takes exactly one --values-override, so the AWS values in
jenkins-eks-overrides.yaml and the name and certificate of one site have to arrive in a single
file.  And a certificate ARN contains the account number, which does not belong in this
repository, so that single file has to be generated.  Its output is gitignored.

The agent ceiling is here for a different reason: it is not a fact about this repository at all.
The account's on-demand vCPU quota changes when AWS grants an increase, which no `tofu apply`
sees, so the number is read live on every run and written here rather than committed anywhere.

Almost nothing here is merged from jenkins-deployment.yaml.  run-ci passes that file itself, ahead
of this one; copying its content here would freeze it, and each agent podTemplate in it is one
multi-line string that a later repository change could then no longer reach.  agent.containerCap is
a scalar under a map, so it overrides exactly itself and costs nothing.

With --pools, template caps follow the applied pool sizes. The small template is split into outer
pipelines and workers, with separate labels and caps. Pipeline concurrency follows the available small
slots, allowing one outer agent and small_workers_per_build workers for each admitted build. Both
resulting templates retain the source pod configuration.
"""

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


# The port the Jenkins container listens on, if it cannot be read from the shared values file.  The
# chart's own default is the same number.
DEFAULT_TARGET_PORT = 8080

# maxRequestsPerHostStr per agent in containerCap, as a fraction so that integer ceiling division can
# carry it.  The ratio is the one ../../jenkins-deployment.yaml has committed twice: containerCap 300
# against 3200, and containerCap 480 against 5120.  32 over 3 is exact for both.
REQUESTS_PER_AGENT_NUMERATOR = 32
REQUESTS_PER_AGENT_DENOMINATOR = 3


def build_overlay(hostname, certificate_arn, target_port, container_cap=None):
    """The keys a public name needs, the further ones TLS needs, and the account's agent ceiling."""

    overlay = {}

    if container_cap is not None:
        # The cap across every agent podTemplate, set to the agent nodes the account's vCPU quota allows.
        # jenkins-deployment.yaml carries a larger default, being shared with GKE, and a node ceiling belongs
        # to one cloud account.
        #
        # Left unset, the queue asks for more pods than the account holds, each waiting on a node the ASG
        # cannot launch, until the plugin loses count: `KubernetesProvisioningLimits ... went below zero`,
        # after which no cap binds at all.  See ../README.md.
        #
        # maxRequestsPerHostStr goes with it, at the ratio the shared file documents.  Both are scalars under
        # one map, so each overrides exactly itself; the chart takes this one as a string.
        requests_per_host = -(-container_cap * REQUESTS_PER_AGENT_NUMERATOR
                              // REQUESTS_PER_AGENT_DENOMINATOR)
        overlay["agent"] = {
            "containerCap": container_cap,
            "maxRequestsPerHostStr": str(requests_per_host),
        }

    if not hostname:
        # No public name: Jenkins is reachable at the load balancer's own address, which Kubernetes
        # chooses and this script cannot know.  Nothing to add, and nothing to add TLS to either.
        return overlay

    annotations = {
        # external-dns watches Services for this annotation and writes the record pointing the name at the
        # load balancer.  Kubernetes creates that load balancer, so its address is unknowable until this
        # Service exists, which is why ../1-cluster does not write the record.
        #
        # An alias to the load balancer, not a CNAME: external-dns recognises an ELB hostname and writes an
        # alias, which is what lets the name be a zone apex, where a CNAME is invalid.
        "external-dns.alpha.kubernetes.io/hostname": hostname,
    }

    # Assigned into the overlay rather than replacing it: the agent ceiling above is already in there.
    overlay["controller"] = {
        # What Jenkins reports as its own address: email URLs, the advertised JNLP endpoint, and the UI's
        # "Jenkins URL".  It changes neither what the Service listens on nor how agents connect, which stays
        # the in-cluster service name.
        "jenkinsUrl": "%s://%s" % ("https" if certificate_arn else "http", hostname),
        "serviceAnnotations": annotations,
    }

    if certificate_arn:
        # run-ci prefers the public DNS name and this HTTPS listener. Keep HTTP for clients using the
        # load balancer's own name, which the site's certificate generally does not cover.
        # A Classic Load Balancer cannot redirect HTTP to HTTPS; that needs an Application Load Balancer.
        overlay["controller"]["extraPorts"] = [
            {"name": "https", "port": 443, "targetPort": target_port},
        ]
        annotations.update({
            # The certificate the load balancer serves.  It must be ISSUED: a Classic Load Balancer
            # given a PENDING_VALIDATION certificate is created without a working listener on 443.
            # ../Makefile checks the status and omits every key in this block when it is not ISSUED.
            "service.beta.kubernetes.io/aws-load-balancer-ssl-cert": certificate_arn,
            # Which listeners terminate TLS, by port name.  `https` is the name the chart gives the
            # extra port above; the first port stays named `http` and stays plain.
            "service.beta.kubernetes.io/aws-load-balancer-ssl-ports": "https",
            # How the load balancer reaches the pod, once TLS is terminated.  Without this the whole
            # Service is treated as TCP, and the load balancer then passes bytes through to a
            # container that speaks no TLS.
            "service.beta.kubernetes.io/aws-load-balancer-backend-protocol": "http",
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
    """Match template caps to pool capacity and reserve small workers for concurrent pipelines.

    Layer 1 no longer creates each pool at the max_size written in var.agent_pools: it scales all of them by
    the lowest ceiling the account allows, so the numbers in jenkins-deployment.yaml are one account's
    answer.  Left alone, both directions of that scaling are wrong.  Scaled down, an instanceCap above its
    pool's nodes makes run-ci refuse the deploy, correctly, because those pods could never be scheduled.
    Scaled up, the cap never moves and the extra nodes are never asked for.

    Small agents also need separate pipeline and worker caps. Both templates are rebuilt from the same
    source pod configuration, so container, volume and affinity changes reach both.
    """
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
    pipeline.update(name="agent-dind-pipeline", id="eks-pipeline", label="cassandra-small")
    worker.update(id="eks-small-worker",
                  label=" ".join(label for label in worker["label"].split() if label != "cassandra-small"))
    for name, template, cap in (("agent-dind-pipeline", pipeline, concurrent),
                                (key, worker, workers)):
        template.update(instanceCap=cap, instanceCapStr=str(cap), nodeUsageMode="EXCLUSIVE")
        scaled[name] = yaml.safe_dump([template], sort_keys=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base", required=True,
                       help="jenkins-eks-overrides.yaml, the committed AWS values this is built on")
    parser.add_argument("--out", required=True, help="file to write")
    parser.add_argument("--hostname", default="",
                       help="the public name, as `ci.example.org`. Omit to leave Jenkins reachable at the "
                            "load balancer's own address")
    parser.add_argument("--certificate-arn", default="",
                       help="an ISSUED ACM certificate for --hostname. Omit to leave Jenkins on plain HTTP")
    parser.add_argument("--container-cap", type=int,
                       help="agent.containerCap: the most agent pods this account can hold at once, which "
                            "is what ../vcpu-quota.py derives from its on-demand vCPU quota. Omit to leave "
                            "the shared default in jenkins-deployment.yaml alone")
    parser.add_argument("--deployment",
                       help="jenkins-deployment.yaml, read for controller.targetPort and, with --pools, for "
                            "the podTemplate strings whose instanceCap is rewritten to its pool's created "
                            "size. The TLS port has to reach the same container port the plain one does, and "
                            "writing that number down twice is how the two drift apart")
    parser.add_argument("--pools",
                       help="JSON from `tofu output -json agent_node_groups` in ../1-cluster. Caps follow "
                            "the applied pool sizes; small slots are divided between outer pipelines and "
                            "their worker budgets. Needs --deployment. Omit to leave the shared templates "
                            "unchanged")
    args = parser.parse_args()
    if args.pools and not args.deployment:
        parser.error("--pools requires --deployment")

    base = load_yaml(args.base)
    target_port = DEFAULT_TARGET_PORT
    deployment = {}
    if args.deployment:
        deployment = load_yaml(args.deployment)
        target_port = deployment.get("controller", {}).get("targetPort", target_port)

    overlay = build_overlay(args.hostname, args.certificate_arn, target_port, args.container_cap)

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

    header = (
        "# Generated by %s. Do not edit, and do not commit: this file names an AWS account.\n"
        "#\n"
        "# Built from %s, plus what only the account knows: the public name and certificate\n"
        "# ../1-cluster made, and the agent ceiling ../vcpu-quota.py derived.\n"
        "# Rebuilt by every `make jenkins`.\n"
        % (parser.prog, args.base)
    )
    with open(args.out, "w", encoding="utf-8") as handle:
        handle.write(header)
        yaml.safe_dump(values, handle, default_flow_style=False, sort_keys=True)

    if args.hostname:
        scheme = "https" if args.certificate_arn else "http"
        print("Wrote %s: Jenkins reports itself as %s://%s" % (args.out, scheme, args.hostname))
        if not args.certificate_arn:
            print("No certificate given, so the load balancer serves plain HTTP only.")
    else:
        print("Wrote %s: no public name, so Jenkins answers at the load balancer's own address."
              % args.out)

    if args.container_cap is not None:
        print("Agents are capped at %d at once, which is what this account's vCPU quota holds."
              % args.container_cap)


if __name__ == "__main__":
    main()
