<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# What was measured, and on which run

tl;dr: this file is the evidence behind the figures in `README.md` and in the code comments.  Nothing here is needed to run or debug the cluster.  It is here so that a figure somebody wants to change can be traced to the run it came from, and so that `README.md` can state a conclusion without arguing for it.

All of it is one account, in `us-west-2`, between 2026-08-26 and 2026-09-09.  Every constant it justifies is one measurement at one load: read them as the right order of magnitude with the right slope, not as significant figures.

## Contents

- [What has run against AWS](#what-has-run-against-aws)
- [The builds](#the-builds)
- [Where the agent deadlines came from](#where-the-agent-deadlines-came-from)
- [Where the address targets came from](#where-the-address-targets-came-from)
- [Where the controller model came from](#where-the-controller-model-came-from)
- [The four faults of the spend guard](#the-four-faults-of-the-spend-guard)
- [What is still unproven](#what-is-still-unproven)

## What has run against AWS

All three layers, in one account.

Layer 1 applied in full: 82 resources, Kubernetes 1.36 resolved from `data.aws_eks_cluster_versions`, six add-ons, seven node groups, and the Auto Scaling group tags on each.  The public name and TLS added five more: the certificate, its validation `CNAME`, the external-dns add-on on `v0.21.0-eksbuild.8`, and that add-on's role and policy.

Layer 2 deployed.  The autoscaler ran on the controller node, on image `v1.36.1` chosen from the control plane's 1.36, and discovered all seven Auto Scaling groups.  `.build/run-ci --only-setup` installed Jenkins behind a Classic Load Balancer and seeded its jobs.

Layer 3 passed, 22 checks, including the account's vCPU ceiling and the Metrics API.

The node group ceilings were confirmed against the live `cluster-autoscaler-status` configmap, by the arithmetic `run-ci` itself uses: each group attributed to its pool by the size word, `jenkins-controller` skipped, and the per-zone maxima summed.  25 + 25, 50 + 50, and 80 + 80 gave 50, 100 and 160, which were the pool totals at the time.  That is the one place a per-zone `max_size` can go wrong silently, so repeat the check after every change to those totals.

The public name is confirmed end to end except for a browser.  Against a public resolver the zone answers with its own four Route 53 nameservers, the apex holds an A record on both load balancer addresses, the ACM validation `CNAME` resolves, and the certificate is `ISSUED`.  `make jenkins` then generated values carrying the `https` URL, the `443` entry in `controller.extraPorts` and the three `aws-load-balancer-ssl-*` annotations.  No browser has fetched Jenkins over that name: the network this was developed on filters the domain at its resolver and blocks the load balancer's addresses at a proxy, which answers `403` to a request made by IP.  A hosts-file entry addresses only the first of those two.

## The builds

| build | profile | targets | result | duration | failures | passes |
|---|---|---|---|---|---|---|
| #9 | `post-commit` | 30 | FAILURE | 3 h 30 m | 2753 | 319280 |
| #11 | `custom`, `post-commit` less its five upgrade targets | 25 | UNSTABLE | 1 h 55 m | 16 | 315915 |
| #10 | `pre-commit` | 16 | UNSTABLE | 1 h 10 m | 15 | 113586 |
| #4 | `pre-commit` | 16 | UNSTABLE | 1 h 49 m | 13 | 113588 |

Three of those separate the cluster from the code under test.  2737 of #9's 2753 failures were on an upgrade path, 1794 carrying `IllegalArgumentException: Unsupported metadata version (10)`, thrown where a node replays a Transactional Cluster Metadata log its own 6.0 self wrote.  Commit `28999f0480` added `V10` to `cassandra-6.0` and trunk on 2026-08-26; the branch under test was based four days earlier and stopped at `V9`.  The upgrade dtests always build the apache tip rather than the branch under test, so a branch behind trunk upgrades from a newer 6.0 into an older trunk.  899 further failures are ccm's 120-second `is now UP` deadline for the node that had died of it.

#11 settles it: 3365 fewer passes than #9 and 16 failures instead of 2753, every one a seeded simulator or Accord case, a flaky `cqlsh` copy count, a compaction test, or a TCM listener assertion.  None names an agent, a pod, a node, or a Kubernetes event.

#10 against #4 is the like-for-like pair for the cluster work, being the same profile with test counts within 4 of each other: 1 h 10 m against 1 h 49 m.  Both saturated the pools, so `pre-commit` is enough to reach the ceiling and `post-commit` only queues deeper behind it.  #10 went from 8 to 475 busy executors of 476 in under four minutes, and #11 held 457 of 457 with 437 tasks queued.  #10's four minutes is not a cold start: it began ten minutes after #9 ended, inside the autoscaler's default `scale-down-unneeded-time`, so most nodes were still up.

Earlier runs, for the record.  Build `cassandra` #3 is why the agent pools are `ON_DEMAND`; see below.  Build `cassandra` #7 is the churn loop; see below.  Build `cassandra-eks-k8s` #8 reached 313 concurrent busy executors and 306 nodes before subnet addresses stopped it, and ended in FAILURE after 3 h 56 m with `Retryable interruption: Timeout has been exceeded` and four containers killed at exit 143, which is a cell reaching its own `timeout_hours`.  Build #9, with the address targets applied, held 476 nodes and 475 busy executors with 1035 tasks queued, which is 476 of the autoscaler's 481.

### Why the agent pools are not SPOT

Build `cassandra` #3 failed, and it failed for spot.  In the 23 minutes from 21:56 to 22:19 UTC the Auto Scaling groups recorded eleven `EC2 Spot Instance interruption notice` activities, ten of them in one zone.  EKS drains a node it is told to give back, and a drain evicts through the Kubernetes Eviction API; five agent pods went that way.  The pipeline's `finally { cleanAgent() }` then ran a shell step on an agent that had gone, which throws `AgentOfflineException`; `retry` does not retry that, and `failFast` discarded the other 313 branches.  The same activities record 253 `UnfulfillableCapacity` and 50 `MaxSpotInstanceCountExceeded` launch failures, so the pools were also failing to fill while being reclaimed.  The cluster autoscaler is not implicated: its log for that window shows 94 `removing empty node` decisions and no pod eviction at all.

### Where the pool shares came from

Counted from build #11's archived stage logs, one per cell:

| profile | small | medium | large | total cells |
|---|---|---|---|---|
| `skinny` | 0 | 117 | 194 | 311 |
| `pre-commit` | 18 | 189 | 398 | 605 |
| `post-commit` less its upgrades | 18 | 441 | 644 | 1103 |

Build #4 held 262 agents at its peak with medium pinned on 100 and large on 160, each exactly its own cap, and 202 and 467 tasks queued behind them.  small used 19 pods across the whole build and never held more than 2, against a cap of 50.

`skinny` and `pre-commit` both fit entirely, with 3.8x and 2.3x headroom on medium, so the ratio binds for neither.  It binds only above the ceiling, which is why it follows the widest profile that still fits.  `post-commit` cannot fit at any ratio: its four upgrade targets are 1200 large cells alone.

### The caps do not hold

Build #8 was sampled every 30 seconds for its whole 3 h 56 m, 209 samples:

| cap | value | samples over it | peak |
|---|---|---|---|
| `instanceCap` large | 306 | 144 of 209 | 543 |
| `instanceCap` medium | 150 | 82 of 209 | 226 |
| `agent.containerCap` | 480 | 116 of 209 | 768 |

At the busiest single moment the caps were exact: 313 busy executors, and 19 small, 150 medium and 306 large agents, each its own `instanceCap`.  For most of the build they were not.  At the widest overshoot the cluster held 489 large agents against 306 and 688 in total against 480, with 390 offline, each holding a `Pending` pod that no node could take.  The pods and the agents matched one for one, so these were agents still launching and not stale `Computer` objects.

The mechanism is the Kubernetes plugin's own counter and the evidence is circumstantial: the controller log had rotated past every `KubernetesProvisioningLimits` line by the time the overshoot was found.  What survived shows both termination paths firing for each agent, `KubernetesSlave#_terminate` and `Reaper$RemoveAgentOnPodDeleted#onEvent`, on 9 of 9 agents in the window.  That counter is held in memory and decremented on termination, so a second decrement for one registration drifts it below the truth and the cap admits pods it should refuse.  This is what build #7 reported as `went below zero` 483 times, and raising `slaveConnectTimeout` to 600 did not remove it.  Plugin version `4547.v52f3080db_8cd` under Jenkins 2.516.3; an upgrade has not been tried.

Read it from the pods rather than the log, because the log will have rotated:

```shell
kubectl -n default get pods --no-headers | grep -c agent-dind-large        # against instanceCap
```

## Where the agent deadlines came from

Build `cassandra` #7 finished after 2 h 19 m, having held a mean of 31.4 executors against a ceiling of 47 and accumulated 28.7 hours of queue time.  Its console log holds 310 `Still waiting to schedule task` and 116 `Waiting for next available executor on 'cassandra-amd64-medium'`.  No `large` or `small` label ever waited.

In the 58 minutes of controller log that survived rotation:

| measure | count |
|---|---|
| `Created Pod` lines, all of them `agent-dind-medium` | 1,110 |
| `KubernetesClientTimeoutException ... [30000] milliseconds` | 1,110 |
| agents that connected | 0 |
| `deleteSlavePod: Terminated` | 1,158 |
| `KubernetesProvisioningLimits ... went below zero` | 483 |

Peak creation rate was 59 pods per minute.

The cause is a deadline shorter than the thing it waits for.  Every pod template set `slaveConnectTimeout: '30'`, and a pod whose node does not exist yet waits for an EC2 launch, an AL2023 boot, a kubelet join and two image pulls, one of them under `alwaysPullImage`.  Nothing completes in 30 seconds, so the pod was deleted, the queue asked again, and the loop ran for as long as the build did.

The 483 warnings are the harm.  Once that count is corrupt neither `instanceCap` nor `containerCap` binds, which is why the controller created 59 pods per minute against an `instanceCap` of 100.

The loop also misled the autoscaler: a node launched for a pod Jenkins had already deleted arrives empty and then holds 8 vCPU for the ten minutes of the default `scale-down-unneeded-time`.  The churn consumed the resource the churn was short of.

Only the medium pool starved, and the deadline explains that too.  Large tasks are long, so a warm large node stays warm and its pod connects on reuse; medium tasks are short, so medium needed a cold node constantly and every cold-node wait was killed at 30 seconds.  `idleMinutes: 1` compounded it by reaping a medium pod one minute after its task while the autoscaler held the node regardless.

The deadline was not the whole fault.  During build #7 no node could launch at all, because of the vCPU quota, so a longer deadline would not have produced a medium agent.  What the deadline did was make that stall unbounded, silent, and confined to one pool.

The controller was not implicated.  Maximum GC pause was 117 ms across 24,307 GC log lines, on a fixed 8 GiB G1 heap with `AlwaysPreTouch`, on its own m7a.2xlarge, with no API-server throttling.  It was creating a pod a second because of the loop, not because it lacked room.

### The cold path, measured

On the small pool scaling from zero, its Auto Scaling group empty for two hours:

| moment | time | elapsed |
|---|---|---|
| pod created, no node exists | 16:32:27Z | — |
| ASG begins the EC2 launch | 16:32:44Z | 17 s |
| node registers with the API server | 16:32:59Z | 32 s |
| node `Ready`, pod scheduled | 16:33:11Z | 44 s |
| pod `Ready`, agent connected | 16:33:21Z | **54 s** |

54 seconds is the figure `slaveConnectTimeout: '30'` did not clear.  The 300-second floor in `3-smoke/check-pool-fit.py` keeps its margin over it on purpose: the measurement was taken on an idle cluster, and every way this path gets longer is a way it gets longer under load, which are the conditions the churn loop needs.

## Where the address targets came from

Build #8 stopped at 314 nodes with 451 pods `Pending` and every node group in `OutOfResource placeholder-cannot-be-fulfilled` backoff.  552 interfaces held 8182 addresses, which is 26.1 a node and every address both subnets had.  The scaling activities named it and nothing else did:

```
Could not launch On-Demand Instances. InsufficientFreeAddressesInSubnet - There are not enough
free addresses in subnet '<subnet>' to satisfy the requested number of instances.
```

The pools were configured for 480 nodes, the account allowed 622 on vCPU and 512 on storage, and `make quota` reported headroom throughout.  It was right about the two ceilings it read.

The cause is `WARM_ENI_TARGET=1`, the VPC CNI's default, which keeps one whole spare network interface attached to every node.  An interface on these instance types carries 15 addresses, so a node held 30 while needing 3: its own, the agent pod's, and `ebs-csi-node`'s.

With `MINIMUM_IP_TARGET` and `WARM_IP_TARGET` set, build #9 measured 476 nodes holding 2437 addresses, which is 5.12 a node against 26.1 before, leaving 5745 free across the same two subnets.  Counting the interfaces agrees: 523 interfaces on 475 instances held 2428 addresses, 5.11 a node, and only 4 of the 2432 addresses in use were on anything other than a node.  So those two subnets hold about 1600 nodes rather than 313.  The gain is a factor of five, not the order of magnitude the 30-against-3 arithmetic suggests, because a node that holds no spare interface still holds `MINIMUM_IP_TARGET`.

Reclaim takes effect at once rather than as the pools cycle.  Measured 25 minutes after the add-on update rolled the `aws-node` DaemonSet, across 475 instances:

| launched | instances | addresses a node | interfaces a node |
|---|---|---|---|
| before the roll | 92 | 5.58 | `{1: 44, 2: 48}` |
| after it | 383 | 5.00 | `{1: 383}` |

Those 92 nodes had held about 26 addresses each an hour earlier.  Reclaim is not quite complete: 48 of them kept a second interface, whose own primary address is the sixth they hold.

## Where the controller model came from

Measured on builds #12 and #13 on one m7a.2xlarge with a fixed 8 GiB heap:

| driven by | measurement |
|---|---|
| memory, by the agent count | 18105 MiB at 1087 connected agents, flat across four minutes of pod churn |
| CPU, by the pod churn rate | 1301m to 7633m at that same 1087 agents, in bursts |

The bursts are `CloudRetentionStrategy#check` reaping agents that reached `idleMinutes` and the provisioner replacing them: the controller log shows 49 provisioned and 42 terminated inside one minute, then eight of each in the next.  So memory follows the agent count and CPU follows the churn rate, which makes `idleMinutes` a controller-sizing knob as well as an agent one.

Against that measurement, `controller-fit.py`'s model produced 7583m and 18463 MiB for 1122 agents, against 7633m and 18105 MiB observed.

The cluster could not answer this question for itself at first: `kubectl top` said `Metrics API not available`, and both `/prometheus/` and `/monitoring` answered 404, so the GC log was the only instrumentation.  That is why layer 2 installs `metrics-server`.

## The four faults of the spend guard

The guard has run against AWS once, and it braked the cluster on its first evaluation.  Each fault below is why one part of `spend-guard.py` is written the way it is.

**1. A written-down sample interval.**  `make spend` reported the pools braked with `UNKNOWN: the vCPU metric is published every 300s and this arithmetic divides by 60s, so every figure would be out by a factor of 5`.  Both halves of that are the design working: the interval check caught the assumption, and unknown spend stopped the pools rather than letting them run on a figure a fifth of the truth.

**2. A fit with no slope to find.**  The fit reported `$0.00 a vCPU-hour and $3.27 a day standing still +/- $0.98 a day` over 12 settled days, and called itself calibrated.  Those days were an idle cluster: a few tens of vCPU-hours each, differing by a few, so least squares had no slope and answered with a price near zero.  Nothing was wrong with the arithmetic and the answer was useless, because a price near zero estimates a full-size build at nothing.  Hence `CALIBRATION_MIN_SPAN_HOURS`, and hence a refused fit being printed rather than discarded: the $3.27 a day it measured is a better figure for `var.spend_fixed_usd_per_day` than the $5 default.

**3. `SampleCount` is not the count of raw samples.**  Deriving the interval as `3600 / max(SampleCount)` read the daily figure as $338 against a budget showing $10.59 actual, a factor of about thirty: the interval came out as an hour and every sample was charged as if it stood for one.  The interval now comes from the metric's own timestamps.  Two further changes came out of the same evidence: a settled day takes the bill rather than the larger of the two figures, so a model error cannot reach into the past, and the report audits itself against the bill on the days where both exist.

**4. A bound around the guess the measurement exists to replace.**  With the interval fixed the daily figure read $353 where AWS Budgets showed $10.59, and the day table said why: the fit had found $0.0010 a vCPU-hour with a $3.27 floor over six days spanning 384 to 22,599 vCPU-hours, with a residual under a dollar a day, and a bound of a factor of four around the configured $0.06 refused it.  The fallback then over-read the bill by 42x and braked the cluster.  The fit was right and the bound was wrong: this account's bill is a fortieth of the on-demand list price per vCPU-hour the metric reports, which a Savings Plan or a discount held in a management account will do.  Three changes: the bound is now absolute, a refused fit has its configured figures scaled so their total over the settled days matches the bill, and the rate prints to four decimal places, because $0.0010 shown as `$0.00` is how a fitted figure reads as zero.

## What is still unproven

- No cap has been met by real spend, so no release has been observed and no alert email has arrived.  The SNS topic policy and the two AWS Budgets were created and accepted; whether the budgets are inside the free tier is a line on a bill rather than something an apply reports.
- No build has run under the brake, so two properties AWS documents remain documented rather than measured: that a suspended `Launch` leaves running agents alone, and that the autoscaler still scales the pools in while it is suspended.
- Whether `ON_DEMAND` is enough.  Cluster-autoscaler scale-down, EKS node auto repair and a node group rolling update all drain through the Eviction API, and the Jenkinsfile turns any of them into a failed run.  `make smoke SMOKE_ARGS=--build` covers scale-up, not this.
- The controller's own ceiling.  It has been seen to serve 475 busy executors on 476 nodes, so `agent.containerCap` at 480 and the observed maximum are within five of each other, and the ceiling has still not been found.
- Jenkins over the public name in a browser; see [What has run against AWS](#what-has-run-against-aws).
