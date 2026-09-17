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

# Jenkins CI on Amazon EKS

`../README.md` provisions this cluster on GKE with four `gcloud` commands.  This directory is the AWS equivalent, replacing a console click-through runbook.  It has been applied against one account, in `us-west-2`, and has run builds at 476 nodes; `NOTES.md` records what was measured on which run, and what is still unproven.

Read `make plan` before `make apply`.  These pools can hold hundreds of on-demand nodes, and no AWS service quota is a limit on money, which is what [A cap on what this cluster may spend](#a-cap-on-what-this-cluster-may-spend) exists for.

## Contents

- [The three layers](#the-three-layers)
- [Prerequisites](#prerequisites)
- [Quick start](#quick-start)
- [What cannot be scripted, and is not attempted](#what-cannot-be-scripted-and-is-not-attempted)
- [A cap on what this cluster may spend](#a-cap-on-what-this-cluster-may-spend)
- [When something stops part way](#when-something-stops-part-way)
- [Decisions worth knowing before you apply](#decisions-worth-knowing-before-you-apply)
- [Two findings about the shared files, not fixed here](#two-findings-about-the-shared-files-not-fixed-here)
- [Layout](#layout)
- [Not goals](#not-goals)

## The three layers

| Layer | Directory | Tool | Owns |
|---|---|---|---|
| 1 | `1-cluster/` | OpenTofu | cluster, IAM, add-ons, node groups |
| 2 | `2-platform/` | Helmfile, and `.build/run-ci` | cluster autoscaler, metrics-server, Jenkins site overrides |
| 3 | `3-smoke/` | Python, bash | post-deploy assertions |

Data flows one way.  Layer 1 owns AWS and prints what it made; the `Makefile` turns that into environment variables; layers 2 and 3 read only those.  Nothing in `2-platform/` or `3-smoke/` opens a tfstate file, and nothing in `1-cluster/` talks to Kubernetes.

The `Makefile` is the join, and the only place the three layers meet.

## Prerequisites

Install `tofu` (1.10.0 or newer), `helmfile`, `helm`, `kubectl`, `aws`, `python3`, and `jq`.  Do not install `eksctl`; nothing here uses it.

Install the `helm-diff` plugin as well.  `helmfile apply` computes a diff before it deploys, and `helm diff` is a plugin rather than a helm subcommand:

```shell
helm plugin install --verify=false https://github.com/databus23/helm-diff     # helm 4
helm plugin install https://github.com/databus23/helm-diff                    # helm 3
```

Helm 4 verifies plugin signatures by default and `helm-diff` publishes none, which is what `--verify=false` is for.  Helm 3 does not accept that flag.  `make autoscaler` checks for the plugin before it runs helmfile.

Authenticate to an account you may create a cluster in.  Confirm which one, before the first apply:

```shell
aws sts get-caller-identity
```

## Quick start

```shell
cd .jenkins/k8s/eks

cp 1-cluster/terraform.tfvars.example 1-cluster/terraform.tfvars
$EDITOR 1-cluster/terraform.tfvars      # at least cluster_name and region

make test                               # no credentials needed
make caps                               # what this may spend a day, a week and a month
make plan                               # read this, including the agent_node_ceiling output
make apply
make kubeconfig
make quota                              # how many agents the account allows, and whether the controller fits
make platform                           # autoscaler and metrics-server, then Jenkins through run-ci
make smoke
make spend                              # what it has spent against those caps
```

`make help` lists every target.

**`make quota` comes after `make apply`, not before.**  It reads the node groups layer 1 actually created, which do not exist until then.  Before the first apply the same three ceilings are in `make plan`, as the `agent_node_ceiling` output: layer 1 computes them from the account and scales the pools to the lowest, so the plan already says how big this cluster will be.  See [Four ceilings, and the lowest one wins](#four-ceilings-and-the-lowest-one-wins).

`make caps` before `make apply`, and `make apply` asks for the caps itself when nothing has answered yet.  `none` is an answer to all three questions.

`make smoke SMOKE_ARGS='--build --sample-seconds 3600'` adds the one check no offline test can reach: a real build, an agent pool scaling up from zero, and a sample of executor and queue depth through it.  Add `RUN_CI_ARGS='-r <url> -b <branch>'` when the current branch tracks no remote, because `run-ci` cannot otherwise name what to build.

To serve Jenkins on a name of your own, over HTTPS, set three more variables in `terraform.tfvars` and delegate a zone; see [A public name, and TLS for it](#a-public-name-and-tls-for-it).

To delete the cluster, run `make destroy`.  The Jenkins controller's persistent volume goes with it.

## What cannot be scripted, and is not attempted

Four things are outside this directory, and none of them fails cleanly.  Each one shows up as an apply that stops part way, so do them first.

**AWS account and organisation bootstrap.**  An account, its place in an organisation, and the credentials you authenticate with are all upstream of layer 1, which assumes a credential that can create a cluster, IAM roles and node groups.

**Service quota increases.**  The quota is counted in vCPU, not in instances: `L-1216C47A`, `Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances`.  A new account gets 5.  Every agent pool is `ON_DEMAND`, so this one quota carries all of them and the spot quota does not apply.

Layer 1 reads it at apply and scales the pools to it, so a small quota gives a small cluster that works rather than node groups whose launches fail with `VcpuLimitExceeded`.  Raising it is therefore optional, and `make quota` prints the figure to ask for.  Read the current value first:

```shell
aws service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A --query 'Quota.Value'

aws service-quotas request-service-quota-increase \
    --service-code ec2 --quota-code L-1216C47A --desired-value <the ask> --region us-west-2
```

Nothing here runs that second command.  It opens a support request against the account, which is a commitment the operator makes and not a step in a deploy, and the approval is a human at AWS taking hours to days.

**EBS storage is a second quota, and layer 1 reads it too.**  Every node carries a `disk_gib` root volume, so 480 agent nodes and the controller's are 481 volumes of 100 GiB, which is 46.97 TiB against a 50 TiB quota.  Which of the two storage quotas applies is not pinned here: `1-cluster/node-groups.tf` sets `disk_size` and no launch template, so the volume type comes from the EKS AMI's own default.  Both default to 50 TiB and layer 1 takes the smaller of the two, which is right whichever it is.  Read both, and pin the type in a launch template if that is not good enough:

```shell
# gp3, and gp2, in TiB
aws service-quotas get-service-quota --service-code ebs --quota-code L-7A658B76 --query 'Quota.Value'
aws service-quotas get-service-quota --service-code ebs --quota-code L-D18FCD1D --query 'Quota.Value'
```

The controller's own 500Gi `jenkins_home` claim is a further volume, in whichever class the site's `persistence.storageClass` names.  On this cluster that is `gp2`, so it is charged separately from the node volumes if those are gp3.

**DNS delegation.**  Everything else about a public name is scripted; see [A public name, and TLS for it](#a-public-name-and-tls-for-it).  A hosted zone in this account resolves for nobody until the registrar that holds the domain is told the zone's four nameservers, and that is a form on a registrar's website.  For a name under a domain this account does not hold at all, such as `ci-cassandra.apache.org`, it is a conversation with whoever does.

Until the delegation lands, the certificate stays `PENDING_VALIDATION` and `make jenkins` leaves Jenkins on plain HTTP.  Nothing fails; it waits.

**The sizing judgements.**  Every instance type, `max_size` and disk size in `1-cluster/variables.tf` is a judgement about cost against build latency, made against the pod templates in `../jenkins-deployment.yaml` as they are today.  `3-smoke/check-pool-fit.py` checks that an agent fits on its node.  It cannot tell you whether the node is the right node to be paying for.

## A cap on what this cluster may spend

The ceilings below are ceilings on size, and none of them is a ceiling on money.  A cluster inside every quota holds hundreds of on-demand nodes for as long as builds keep queueing, and nothing in AWS stops that: a service quota limits what may exist at once, and a bill limits nothing at all.  One observed run held 476 nodes for three and a half hours, which at 6 cents a vCPU-hour is about $800 of instance time.

So there are three caps, in US dollars: a day, a week and a month.  When one is met, the agent pools stop starting nodes, every running agent finishes, and the pools drain to zero.

### What to run

```shell
make caps                               # asks three questions, and writes 1-cluster/spend-caps.auto.tfvars
make apply                              # creates the guard from that file
make spend                              # what has been spent, against what, and whether the pools are stopped
```

`make caps` needs no credentials and touches no account.  It prints the arithmetic before it asks, because a number typed into a spend cap with nothing to compare it against is a guess: what this cluster costs standing still, what the pools cost an hour at full size, and each answer expressed back as how many hours of that it holds.

`make spend` reads with your own credential, so it needs `ce:GetCostAndUsage` alongside the CloudWatch, SSM and Auto Scaling reads; the Lambda has its own role for the same calls.  `make spend SPEND_ARGS=--days` prints every day behind the totals: what ran, what Cost Explorer billed, what the estimate made of it, and which of the two was taken.  A cap is met by a sum, and a sum nobody can decompose is a figure nobody can argue with.

The answers go in `1-cluster/spend-caps.auto.tfvars`, which OpenTofu loads on its own and which is gitignored.  It is a separate file from `terraform.tfvars` so that a generated file and a hand-edited one never share a line.  `make apply` asks once, when that file does not exist, and never again whatever is in it.  `none` for all three windows is a recorded answer, and it creates no guard at all: no Lambda, no schedule, no topic and no cost.

Every window is UTC, because that is what AWS bills and reports in.  The day starts at midnight, the week on Monday, and the month on the first.

### How the three windows are measured

AWS publishes no low-latency spend feed.  Cost Explorer is refreshed at least once a day and its figure for the current day is partial, so a daily cap enforced from Cost Explorer alone reacts a day late, which at a few hundred dollars an hour is the whole cap and more.

So spend is measured twice, and which of the two figures a day takes depends on whether its bill has settled:

$$\text{spend}(w) = \sum_{d \in w} \begin{cases} c_d & d \text{ settled} \\ \max\Big(\underbrace{c_d}_{\text{Cost Explorer}},\; \underbrace{p V_d + f\,e_d}_{\text{what ran}}\Big) & \text{otherwise} \end{cases}$$

$V_d$ is vCPU-hours on day $d$, from the CloudWatch metric `AWS/Usage` `ResourceCount`, which is free to read and is the same measurement Service Quotas alarms `L-1216C47A` on.  $e_d$ is how much of the day has elapsed.

That metric is a gauge, so an hour's `Sum` is the total of its samples and one sample is worth however long it stands for:

$$V_{\text{hour}} = \text{Sum}_{\text{hour}} \cdot \frac{\Delta}{3600}$$

where $\Delta$ is the smallest gap between consecutive datapoints, measured from the metric's own timestamps on every evaluation.  `make spend` prints the interval it measured.  Two earlier versions inferred $\Delta$ instead and were each wrong by a factor; see `NOTES.md`.

A settled day takes the bill, whatever the model makes of it: 48 hours after a day ends there is nothing left for a model to add, and a model error can then reach only the last two days rather than every past day of every window.  The current day has no bill to fall back on, so there the model is the whole figure.

$p$ and $f$ are fitted rather than written down.  Over the settled days of the last fortnight, $c_d = f + p V_d$ is two unknowns against several days of different load, so least squares gives both and the residual is the error margin `make spend` prints.  A fitted price covers the root volumes, the cross-zone transfer, the load balancer and the control plane without any of them being listed anywhere, and it follows a price change or a new instance type on its own.  When the days cannot answer, `var.spend_price_per_vcpu_hour` and `var.spend_fixed_usd_per_day` are used instead, and `make spend` says which mode is in force and why.

$p$ is not a list price.  It is dollars of bill per vCPU-hour that the metric reports, on this account, so a Savings Plan, a discount held elsewhere in the organisation, a Reserved Instance and any scaling between the metric and reality all land inside it.  That is what a cap needs: a transfer function from something observable every five minutes to the bill.  This account fits $0.0010 where the on-demand list price is $0.058.

Four things refuse the fit.  Three are about whether the days can identify a slope at all: fewer than three settled days, a span of vCPU-hours under 500, and a relative spread under 15%.  The fourth is absolute sanity, a rate above zero and at most a dollar, and it is deliberately not a bound around `var.spend_price_per_vcpu_hour`: bounding the measurement by the guess it exists to replace is how a written-down number outvotes six days of evidence.

A refused fit is still printed, because a fortnight of an idle cluster measures the standing-still cost well even when it cannot identify a rate.  And the configured figures are not used raw when the bill contradicts them: over the settled days their total is known, so the rate is set to whatever makes the model's total match the bill, with the standing-still figure held.  `make spend` calls that scaled and says how.

The estimate is checkable, and `make spend` checks it.  Over the settled days both figures exist, so their ratio is the model's error against ground truth; more than a factor of two either way and the report says so, with both totals.  It reports and does not act: a guard that ignored its own cap because it distrusted its own figure would be worse than one that brakes early.

**The cap is on the whole account's bill in this region**, and not on what is tagged for this cluster.  Both halves matter.  The region is a `REGION` filter on the Cost Explorer call, and it is there because $V_d$ is a per-region metric: an unfiltered bill regresses every region's spend onto one region's vCPU, and both fitted figures are then wrong with nothing to say so.  Spend in another region is therefore outside these caps, as is anything global.  Within the region it is the account's whole bill, so anything else running there stops these pools, which is the conservative reading of a cap on a bill and an argument for giving this cluster an account of its own.

### What the brake is, and what a build sees

Suspending the `Launch` and `AZRebalance` processes on every agent Auto Scaling group.  The group accepts whatever target size the autoscaler asks for and no instance appears.

| | |
|---|---|
| no new agent node starts | `Launch` is suspended |
| every running agent finishes | nothing is drained, evicted or terminated by the brake |
| the nodes then go away | `Terminate` is not suspended, so the autoscaler removes each node as it goes idle |
| the controller keeps running | its own group is never touched: it holds the build queue and `jenkins_home` |

`AZRebalance` goes with `Launch` because a rebalance under a suspended `Launch` can terminate an instance without replacing it.  Every group here is one zone, so nothing rebalances and suspending it costs nothing.

Two other levers were rejected.  A node group's `max_size` is owned by `1-cluster`, which puts it back on the next apply, and lowering it below the running count makes AWS terminate nodes, which abandons the builds on them.  Scaling the autoscaler's Deployment to zero stops scale-down as well, so the nodes already up would stay up at full price until somebody deleted them.

The brake's state is the state of the thing it brakes, `SuspendedProcesses` on the group, so there is no second record of it to drift and `tofu plan` sees nothing.  Read it directly:

```shell
aws autoscaling describe-auto-scaling-groups \
    --auto-scaling-group-names "$(tofu -chdir=1-cluster output -json spend_guard | jq -r '.agent_asg_names[0]')" \
    --query 'AutoScalingGroups[].SuspendedProcesses'
```

**A braked cluster is a stopped cluster, and it looks exactly like a broken autoscaler**: pods pending, groups at zero, nothing launching, and no failed scaling activity to read, because a suspended `Launch` produces none.  `make spend` and `make smoke` both name the brake for that reason.

The interval is the overshoot.  The guard evaluates every five minutes by default, so at this account's full size, which is 1122 agent nodes and about $540 an hour, the pools can spend some $45 after a cap is met before they stop.  `var.spend_guard_interval_minutes` is that trade, and each evaluation costs a fraction of a cent.

### Unknown spend stops the pools

Spend cannot always be read, and unknown is treated as over the cap: the pools stop, SNS is told, and the next evaluation that reads a figure releases them.  A guard that cannot see stops CI loudly rather than leaving the pools running unwatched.

Three things are unknown, and every other failure is a warning:

1. the caps parameter cannot be read, so there is nothing to compare against;
2. the CloudWatch read fails;
3. CloudWatch returns no vCPU datapoints at all while instances are running in the region.

The third is a discriminator and not a guess.  An empty metric and an idle account give the same empty answer, so the guard asks EC2 whether anything is running: nothing running is a spend of zero, and instances running is a read that has broken.

Cost Explorer failing is **not** unknown.  Every day in every window can be estimated from CloudWatch alone, so the figure survives; what is lost is the fit, and the fallback price is then named in the report.  Cost Explorer also has to be enabled once per account before it answers at all, and takes up to 24 hours to do so afterwards.

### What watches the guard

A guard that has stopped running reports nothing, which is what a cluster with no guard also reports.  So the guard publishes `EvaluationOk` on every evaluation, 1 when it read a figure and 0 when it could not, and a CloudWatch alarm on that metric treats missing data as breaching.

The alarm's window is six evaluations, derived from `var.spend_guard_interval_minutes` rather than written down, which is half an hour at the default interval.  `make smoke` reads its state and separates `INSUFFICIENT_DATA`, which is a guard that has never reported, from `ALARM`, which is one that has stopped.

The other four metrics are the figures themselves, under `CassandraJenkins/SpendGuard`: `EstimatedSpendUsd`, one per window, and `Braked`.  They are what a graph of this cluster's spend is drawn from, and the reason the guard publishes anything at all rather than only writing to its log.

AWS Budgets is a second alert path that shares no code with the guard: `var.enable_spend_budgets` creates one budget for the daily cap and one for the monthly, notifying the same topic at 80% and 100%.  They enforce nothing.  They exist because a budget is where whoever pays the bill looks, and because two independent readings of the same billing data disagreeing is itself worth knowing.  There is no weekly budget: AWS Budgets has no weekly period, which is most of why the guard computes its own windows.  Two budgets and no more: the first two in an account are free and each one after that is charged per day.

Note that a budget is not filtered to one region, and the guard now is, so the two figures are not the same measurement on an account that runs more than one region.

### What the guard itself costs

About $3 a month, against a controller node at $338:

| | |
|---|---|
| Cost Explorer | 4 calls a day at $0.01, which is $1.20 a month; `var.spend_cost_refresh_hours` sets the rate |
| custom metrics | five at $0.30 a month, which is $1.50 |
| the alarm | $0.10 a month |
| `GetMetricData` | two metrics an evaluation, about $0.17 a month |
| Lambda | 8,640 invocations a month of a second or two at 256 MB on arm64, which is cents |

Every figure there is a published price rather than a measurement.  The one to watch is Cost Explorer: at a cent a call, an unthrottled guard evaluating every five minutes would spend about $86 a month asking what it had spent, which is why the figures it reads are kept between evaluations in an SSM parameter and refreshed four times a day.

### What the cap does not cover

**The floor keeps running.**  The controller is one instance all 730 hours of a month whether a build runs or not, the EKS control plane is charged by the hour beside it, and the 500Gi `jenkins_home` volume and the load balancer are charged whatever happens.  The brake does not touch any of them, so a monthly cap under that floor is met with no build having run and then holds the pools stopped for the rest of the month.  `make caps` refuses such a cap; a figure set by hand in `terraform.tfvars` is not checked, because nothing in `1-cluster` knows what an instance costs.

**A cap raised out of band is temporary.**  The caps live in `/<cluster>/spend-guard/caps`, and the guard reads them on every evaluation so that a cap can be raised while somebody is watching a build stop:

```shell
aws ssm put-parameter --name /<cluster>/spend-guard/caps --overwrite --type String \
    --value '{"daily":1800,"weekly":6000,"monthly":18000}'
```

Layer 1 owns that value, so the next `make apply` puts the configured figures back.  `make caps` is what changes them for good.

**Credits are not netted out.**  `var.spend_cost_metric` is `UnblendedCost`, which is what the account is charged before credits are applied, because an account running on credits still has a bill and credits run out.  `NetUnblendedCost` nets them out, so on a sponsored account it reads near zero and no cap is ever met.  Which one is right depends on whether the question is what is being paid or what is being consumed.

**Jenkins is not told.**  The brake stops nodes appearing and does not stop Jenkins asking for them, so the controller keeps creating pods that expire after `agent.waitForPodSec` and asks again.  That costs nothing in AWS and it is a small version of the churn loop in `NOTES.md`.  `agent.containerCap` can be lowered with `make jenkins`, and the guard does not do it, because a deploy is not something to run from a Lambda.

## When something stops part way

Each of these has happened.  All but the credential expiry are fixed in the code; that one is a property of the environment and will happen again.

### `NoSuchEntity`, attaching a managed policy

```
attaching IAM Policy (arn:aws:iam::aws:policy/AmazonEBSCSIDriverPolicy) to IAM Role: NoSuchEntity:
Policy arn:aws:iam::aws:policy/AmazonEBSCSIDriverPolicy does not exist or is not attachable.
```

AWS files a managed policy under one of three paths, and which one is a fact about that policy, not about its name.  `AmazonEBSCSIDriverPolicy` is under `service-role/`; the other five this configuration attaches are not.  Confirm the path for any policy added later, rather than pattern-matching from a neighbour:

```shell
aws iam list-policies --scope AWS --query "Policies[?PolicyName=='AmazonEBSCSIDriverPolicy'].Arn" --output text
```

### `unknown command "diff" for "helm"`, from `make platform`

The `helm-diff` plugin is not installed.  `helmfile apply` computes a diff before it deploys, and that diff comes from the plugin.  The error names helm, under fifteen numbered helm arguments, so it reads as a fault in `helmfile.yaml`; it is a missing prerequisite.  Install it as in [Prerequisites](#prerequisites).  `make autoscaler` now checks for it first and prints the install command.

### `Output "agent_node_groups" not found`, before the first apply

Every target below `make plan` reads the cluster's own state, so none of them can run until layer 1 has been applied.  `make env` now says so; if you see the raw tofu error, the Makefile predates that.  Run `make apply` first, or `make plan` for the ceilings alone.

### An add-on create fails on a conflict

`var.bootstrap_self_managed_addons` is true on a cluster where EKS installed vpc-cni, kube-proxy and CoreDNS itself.  `1-cluster/addons.tf` then creates the managed add-ons over those copies with `resolve_conflicts_on_create = "NONE"`, and the vpc-cni address targets are a field the bootstrapped copy does not carry.

The variable defaults to false, which is right for a new cluster.  It exists for a cluster created before this configuration did: set it true there, and know that changing it afterwards replaces the cluster.

### Builds hold small agents while their workers stay queued

The outer pipeline holds a `cassandra-small` agent until it finishes.  Its JAR stage requests separate `cassandra-amd64-small` workers.  With one template for both labels, overlapping pipelines can occupy every small slot and wait indefinitely for workers.

The generated values now split that template into `agent-dind-pipeline` for outer agents and `agent-dind-small` for workers.  Both use exclusive label matching and stable IDs.  With `S` applied small-node slots, global Jenkins cap `C`, and `W = small_workers_per_build` (default 3), the caps are:

```text
budget       = min(S, C)
pipeline cap = floor(budget / (1 + W))
worker cap   = budget - pipeline cap
```

Four small slots admit one pipeline with three workers; eight admit two with six; twelve admit three with nine.  Extra builds wait before acquiring an outer agent.  This keeps the configured worker budget per admitted build; actual throughput still depends on the profile and stage timings.  Medium and large pools keep their proportional allocations.

Pool scaling preserves at least `W + 1` small slots and rejects allocations that exceed CPU, storage or per-zone subnet budgets.  This is a floor on maximum capacity; idle pools can still scale to zero.  Set `small_workers_per_build` in `1-cluster/terraform.tfvars` if the build matrix needs a different worker budget.  Keep any explicit `agent_pools.small.max_size` at least `W + 1`.

For the first migration, stop stalled builds and remove their idle or offline small agents through Jenkins before `make jenkins`; existing agents retain the old labels and IDs.  Run `make plan`, `make apply`, `make jenkins`, then `make smoke` from this directory.  Start replacement builds after deployment.  Raising only the node-group maximum leaves Jenkins's template caps unchanged.

### The Jenkins controller's pod stays `Pending`

`controller.resources.requests` asks for more than the controller node allocates.  `make quota` refuses this before the deploy, names both figures, and says which values file to edit; `--warn-only` does not cover it, because an unschedulable pod is not a trade anybody can accept.

Neither `../jenkins-deployment.yaml` nor `2-platform/jenkins-eks-overrides.yaml` commits a controller `resources` block sized for one account any more.  If you add one, `make quota` is what checks it, and it reads both files merged the way Helm merges them.

### `nil pointer evaluating interface {}.enabled`, from the Jenkins chart

```
Error: jenkins/templates/jenkins-controller-ingress.yaml:2:14
  executing "jenkins/templates/jenkins-controller-ingress.yaml" at <.Values.controller.ingress.enabled>:
    nil pointer evaluating interface {}.enabled
```

A `controller.ingress:` key with nothing but comments under it is `ingress: null` to YAML, and Helm coalesces a null over the chart's default map rather than under it, so the map is gone and the chart's own template dereferences it.  The error names the chart, which is where nobody will look.

The rule for `2-platform/jenkins-eks-overrides.yaml`: a key you do not want must be commented out entirely, not left with an empty body.  Check with

```shell
python3 -c "import yaml,sys; print(yaml.safe_load(open(sys.argv[1])))" \
    2-platform/jenkins-eks-overrides.yaml
```

which should print no `None` value.  `run-ci` runs helm with its output captured, so it reports this as a `CalledProcessError` traceback with the failing command in it and no helm error; re-run that command by hand, with `--dry-run=client`, to see the cause.

### `no Ready node carries cassandra.jenkins.controller=true`, on a cluster that has one

kubectl's jsonpath cannot nest a filter inside another filter: it answers `unterminated filter` on stderr and prints nothing, which with `2>/dev/null` and `|| true` became a count of zero.  Fixed with a flat expression, and the `2>/dev/null` removed so that a real `kubectl` failure is no longer indistinguishable from an empty result.

### The credential expired while the control plane was being created

```
waiting for EKS Cluster create: operation error EKS: DescribeCluster, get identity: get credentials:
failed to refresh cached credentials ... The provided authorization grant is invalid, expired, revoked,
or malformed
```

A control plane takes eight to twelve minutes to become `ACTIVE`, and OpenTofu polls `DescribeCluster` for all of it.  A session that expires inside that window fails the poll, not the creation: the cluster is created, and OpenTofu never sees it succeed.

This one costs a cluster if you re-apply straight away.  A create that errors after the resource exists leaves that resource **tainted**, and the next apply destroys and recreates it.  Check before you re-apply:

```shell
aws eks describe-cluster --name "$(tofu -chdir=1-cluster output -raw cluster_name)" \
    --region "$(tofu -chdir=1-cluster output -raw region)" --query 'cluster.status'
```

If it is `ACTIVE`, drop the taint, and then re-apply:

```shell
tofu -chdir=1-cluster untaint aws_eks_cluster.this
make apply
```

`tofu plan` says which of the two you are about to get.  `Plan: N to add, 0 to change, 0 to destroy` is the recovered case; anything reporting the cluster `must be replaced` means the taint is still there.  Renew the session first if it has less than fifteen minutes left.

### Agent pods churn and no agent connects

The symptom is a pod created and deleted every second or so, `KubernetesClientTimeoutException`, no agent ever connecting, and `KubernetesProvisioningLimits ... went below zero` in the controller log.  Once that last warning appears, neither `instanceCap` nor `containerCap` binds at all.

The cause is a deadline shorter than the wait it covers.  A pod whose node does not exist yet waits for an EC2 launch, a boot, a kubelet join and two image pulls; the measured cold path is 54 seconds on an idle cluster, and longer under load.  `slaveConnectTimeout` is `'600'` on every template and `agent.waitForPodSec` is `"900"`; both are deadlines on the same wait and the shorter one fires, so raising one alone fixes nothing.  `check-pool-fit.py` asserts both against a 300-second floor.

If it recurs, check those three values first, then whether any node can launch at all: during the original occurrence the vCPU quota meant none could, and the deadline only made that stall unbounded and silent.  `NOTES.md` has the forensics.

### `no agent node appeared within 900s`, on a branch that was never pushed

`make smoke SMOKE_ARGS=--build` reported the timeout and the cluster was not at fault.  `.build/run-ci` had refused the submission:

```
Branch mck/eks-k8s/trunk tracks no remote, so the fork and branch to build cannot be detected.
```

`run-ci` takes the fork and branch to build from the current branch's tracking remote, so a local branch names nothing it can build.  Name them instead:

```shell
make smoke SMOKE_ARGS=--build RUN_CI_ARGS='-r https://github.com/apache/cassandra -b trunk'
```

The harness now separates the two failures: a `run-ci` that exits before it submits is reported as its own failure, with its last lines printed inline, and only a build that queued and got no node is reported as a scale-up failure.

### `[Errno 9] Bad file descriptor`, from `run-ci` under `make smoke --build`

```
OSError: [Errno 9] Bad file descriptor
  ... Max retries exceeded with url: /api/v1/namespaces/default/services/cassius-jenkins
```

That reads as an expired credential and is not one.  `run-ci` was launched inside a subshell that exited as soon as it had written a pid file, which orphans the child, and an orphaned child on macOS then fails every outbound `connect`.  Launching it as a direct child of the script, whose pid `$!` names without a file, connects normally.

Two further defects in the same section each made a blank measurement look like a real one.  `curl` reads `[` and `]` in a URL as a glob range whatever the shell quoting, so `?tree=items[id]` failed before it sent anything and the queue depth read 0 for the whole sample; `-g` fixes it.  And a `kubectl top` that returns nothing now warns once, because a blank column and a column of zeros are the same file afterwards and opposite answers.

### `make spend` says UNKNOWN, and the agent pools are braked

```
  BRAKED: Launch is suspended on all 16 agent group(s). No new agent can start; the ones running
  will finish and their nodes will scale in.
```

This is the guard working, and not a fault in the cluster.  Read which of the three unknowns it names before anything else, because they need different answers.  A caps parameter that cannot be read is an IAM or a name fault; a CloudWatch read that fails is usually an expired credential when run from a terminal, and throttling when it is the Lambda; no datapoints at all with instances running means the metric's dimensions are not what `spend-guard.py` asks for.

Nothing has to be released by hand.  Fix the cause, run `make apply`, and the next evaluation resumes the pools; the Lambda's code hash changes with the file, so the apply redeploys it.  To settle it in seconds rather than at the next tick:

```shell
aws lambda invoke --function-name "$(tofu -chdir=1-cluster output -raw cluster_name)-spend-guard" \
    --region "$(tofu -chdir=1-cluster output -raw region)" /dev/stdout
make spend
```

Releasing the groups directly is possible and rarely right, because the guard re-suspends them on its next evaluation while the cause stands:

```shell
aws autoscaling resume-processes --auto-scaling-group-name <name> \
    --scaling-processes Launch AZRebalance
```

## Decisions worth knowing before you apply

### State is not configured

`1-cluster/versions.tf` leaves the backend unset, so state is a local file.  That is right for a cluster one person builds and destroys, and wrong for a shared one: local state cannot be locked, and two people applying at once corrupt it.  A shared cluster needs shared state, and there is a commented S3 example in that file.

A tfstate holds account identifiers and every value a plan resolved, in clear.  The repository's root `.gitignore` excludes it, along with the generated environment and any real `terraform.tfvars`.  The root file is where those patterns go: this repository ignores nested `.gitignore` files, deliberately, on the first line of that same file.

`.terraform.lock.hcl` **is** committed, because its checksums are what pin the providers to an exact build.  OpenTofu rewrites it on every `init` and keeps only its own header comment, so a licence header put in it would not survive; `.build/build-rat.xml` excludes it from the licence check instead, alongside the other config files listed there.

### Versions are derived from AWS, not written down

The Kubernetes version comes from `data "aws_eks_cluster_versions"`: the newest that EKS reports in standard support wins.  Add-on versions come from `data "aws_eks_addon_version"`, keyed on the version the cluster actually got.  The node AMI is whatever the node group defaults to.

The cost of that is a plan which changes without the configuration changing.  With `kubernetes_version` unset, a plan run after AWS promotes a new minor proposes a control-plane upgrade.  It is visible rather than silent, and `make plan` is where you see it, but it is still there.  Set `kubernetes_version` to pin the minor.

A control-plane upgrade rolls the node groups with it, and the agent groups are `max_unavailable_percentage = 25`, which drains a quarter of each pool through the Eviction API.  The Jenkinsfile turns a drained agent into a failed run, so apply an upgrade between builds, not during one.

If you pin it, keep it current.  EKS refuses to skip a minor, so a pin left stale for two releases must be stepped one minor at a time.

### Four ceilings, and the lowest one wins

Four separate limits decide how many agent nodes this cluster can hold.  None of them knows about the others:

$$N_{\text{agents}} = \min\left(N_{\text{addresses}},\; N_{\text{pools}},\; N_{\text{storage}},\; N_{\text{quota}}\right)$$

| | ceiling | on this account | read by |
|---|---|---|---|
| $N_{\text{addresses}}$ | IPv4 addresses the chosen subnets hold, divided by the addresses a node holds | about 1600 | `1-cluster` |
| $N_{\text{pools}}$ | $\sum_{g \in \text{agents}} \text{max\_size}_g$ | 480 declared | `1-cluster`, `make quota` |
| $N_{\text{storage}}$ | EBS quota divided by `disk_gib` | 512 | `1-cluster` |
| $N_{\text{quota}}$ | $\lfloor Q / v_{\max} \rfloor - n_{\text{controller}}$ | 622 | `1-cluster`, `make quota` |

$Q$ is the `L-1216C47A` quota in vCPU and $v_{\max}$ is the vCPU count of the largest instance type any node group may launch.  The largest type sets the figure because a managed node group may launch any type it lists, and a smaller one would under-count.  The demand the quota is measured against is $D = \sum_{g} v_g \cdot \text{max\_size}_g$, over every group, the controller's included.

Three of the four have bound in turn, and each announced itself differently.  A vCPU quota that binds fails a launch with `VcpuLimitExceeded`.  Subnet addresses fail one with `InsufficientFreeAddressesInSubnet`.  A ceiling published too high is not a slack constraint either: Jenkins then accepts pods that wait 600 seconds each for a node that cannot exist, which is the churn loop with the node group in the account's place.

The lesson worth carrying is not that a number was wrong.  It is that the two ceilings this directory first computed were the two that are easy to compute, and a cluster stops at whichever ceiling nobody wrote code for.

So `1-cluster` reads all three of the ceilings it does not set, at apply, and scales every pool's `max_size` by the lowest.  `var.size_pools_to_quotas` turns the two quota reads off for an account whose credentials lack `servicequotas:GetServiceQuota`; the address ceiling is always applied, because the subnets are already read for the node groups.  One factor is applied to every pool, so the ratios in `var.agent_pools` stay as written, and no pool falls below one node.

The factor works in both directions, which is the point.  Above 1 the pools grow into a raised quota with no edit here, so a quota increase is a support request and not a code change.  Below 1 they shrink, so this configuration applied to a smaller account produces a cluster that runs slowly rather than node groups whose launches fail.  `tofu output agent_node_ceiling` prints the three ceilings, which one bound, and the factor; `agent_node_groups` reports both the scaled `max_size` the groups were created with and the `declared_max_size` that was asked for, and `make quota` says when the two differ.

The address ceiling is computed from the subnets' own size and not from `available_ip_address_count`.  A plan run during a build would read the free count with hundreds of nodes already holding addresses, and would then shrink the pools underneath those nodes.  Addresses a node holds comes from the `MINIMUM_IP_TARGET` in `1-cluster/addons.tf`, which is why both of that add-on's targets live in `locals.tf`: the ceiling is computed from them, and a value changed in one place only would leave the arithmetic describing the previous one.

`vcpu-quota.py` owns that arithmetic and sits beside the `Makefile` rather than in a layer.  Two layers need the answer, `2-platform` to cap the autoscaler and `3-smoke` to check the pools, and neither may read it from the other without inverting the one-way data flow.  The `Makefile` runs the script and writes four variables:

| Variable | Read by | For |
|---|---|---|
| `EKS_ONDEMAND_VCPU_QUOTA` | `3-smoke` | reporting $Q$ as AWS gives it |
| `EKS_ONDEMAND_VCPU_DEMAND` | `3-smoke` | reporting $D$, which is the figure to request |
| `EKS_MAX_NODES_TOTAL` | `2-platform` | the autoscaler's `max-nodes-total`, at the lower ceiling |
| `EKS_MAX_AGENT_NODES` | `2-platform`, `3-smoke` | Jenkins' `agent.containerCap`, at the lower ceiling |

The first two are what AWS says; the last two are what this cluster can do.  A reader comparing them sees the headroom, and `make quota` prints it in words.

The quota is read live on every run, and is deliberately not in the tfstate.  AWS grants an increase on its own schedule, which no `tofu apply` sees, so a value captured at apply time goes stale.  For the same reason `agent.containerCap` reaches Jenkins through the generated values file and not through `../jenkins-deployment.yaml`: a node ceiling is a fact about one cloud account, and that file is shared with GKE.

**A live read at deploy time is not a live read.**  The figure is computed on every `make autoscaler` and then frozen in the Deployment until the next one.  A quota granted afterwards is invisible to the running autoscaler, and nothing reports the difference.  Read the two together after any quota grant:

```shell
make quota | tail -3
kubectl -n kube-system get deploy cluster-autoscaler-aws-cluster-autoscaler \
    -o jsonpath='{.spec.template.spec.containers[0].command}' | tr ',' '\n' | grep max-nodes-total
```

`max-nodes-total` converts a `VcpuLimitExceeded` launch failure into a clean refusal.  Without it the autoscaler learns the ceiling by hitting it, and answers with a thirty-minute scale-up freeze on the group under `errorClass=OutOfResource; errorCode=placeholder-cannot-be-fulfilled`, which a build sees only as a pod that stays `Pending`.  The controller's own Auto Scaling group carries the same discovery tags the agent groups do, so it is inside that count, which is why the controller's node is subtracted above.

Each pool's share is capped, and the shares sum to exactly what the pools can create.  While the quotas stay above that, no pool can starve another, because every pool's cap is met out of its own node groups.  Under a ceiling that binds they can, so `agent.containerCap` is set to the sum and not below it: a shared cap under the sum hands the shortfall to whichever pool holds its agents longest.  Those caps do not always hold in practice; see `NOTES.md`.

### The fifth ceiling is the controller, and it is the one that costs money to raise

The four above are the account's, and every one of them is free to raise: a quota increase is a support request, and agent nodes are billed by the minute and scale to zero between builds.  The controller is one instance, running all 730 hours of a month whether a build runs or not, and it is the ceiling that binds once the others are out of the way.

`controller-fit.py` sizes it against the pools, beside `vcpu-quota.py` and for the same reason: it is a cluster-wide fact that no numbered layer owns.  `make quota` runs both.

Two drivers, and they are not the same axis: memory follows the agent count, CPU follows the pod churn rate.

$$\text{memory}_{\text{MiB}} = \text{heap} + 1856 + 7.5 N \qquad \text{cpu}_{m} = 1300 + 140 L \qquad L = 0.04 N \cdot \frac{5}{\text{idleMinutes}}$$

$N$ is the agents the pools' `max_size` figures allow and $L$ is how many pods launch at once.  Both constants are one measurement at one load, so read the model as the right slope rather than four significant figures; `NOTES.md` has the measurement.  A controller sized on the agent count alone is sized for the wrong axis on CPU, which makes `idleMinutes` in `../jenkins-deployment.yaml` a controller-sizing knob and the cheapest of the three levers.

`controller-fit.py` prints the recommendation, the monthly cost of it, and the three ways past a refusal.  It reads `../jenkins-deployment.yaml` and every values file applied after it, merged the way Helm merges them, because the heap and the limits come from whichever file sets them last.  It refuses two different things: a controller without the headroom for the configured pools, which `--warn-only` overrides, and a `requests` figure above the node's allocatable, which it does not, because that is a pod that never starts rather than one that runs slowly.

At the declared default pools an m7a.2xlarge holds the controller with the headroom.  Above roughly 900 agents it does not, and the next size is **$677 a month against $338**.

### Addresses run out before any quota does

Every node takes IPv4 addresses out of the subnet it sits in, and the VPC CNI takes far more of them than the pods need.  Its default is `WARM_ENI_TARGET=1`, which keeps one whole spare network interface attached to every node.  An interface on these instance types carries 15 addresses, so a node holds 30 while needing 3: its own, the agent pod's, and `ebs-csi-node`'s.  `aws-node`, `kube-proxy`, `eks-pod-identity-agent` and `eks-node-monitoring-agent` all use host networking and need none.

`1-cluster/addons.tf` sets `MINIMUM_IP_TARGET` and `WARM_IP_TARGET` on the `vpc-cni` add-on, which makes a node hold $\max(\text{MINIMUM\_IP\_TARGET},\; \text{in use} + \text{WARM\_IP\_TARGET})$ addresses instead of a whole spare interface.  Measured, that is a factor of five: about 5.1 addresses a node against 26.1, so two default-VPC subnets hold about 1600 nodes rather than 313.  Read the current headroom with:

```shell
aws ec2 describe-subnets --query 'Subnets[].[AvailabilityZone,CidrBlock,AvailableIpAddressCount]' --output table
```

Three properties of this ceiling are worth knowing before it is met rather than after.

It takes effect at once, not as the pools cycle.  Changing either key updates the add-on, which rolls the `aws-node` DaemonSet; ipamd restarts with the new targets and releases the surplus from nodes already running.  Reclaim is not quite complete: a node that came up under the old targets can keep a second interface.  Read what one node holds with:

```shell
kubectl -n kube-system exec <aws-node pod> -c aws-node -- \
    curl -s localhost:61678/metrics | grep '^awscni_\(total\|assigned\)_ip_addresses'
```

`ENABLE_PREFIX_DELEGATION` is the answer AWS documents first, and it is the wrong one here.  It raises the pods per node well above the default, and `3-smoke/check-pool-fit.py` models the kubelet's memory reservation from the default `maxPods`; turning it on without changing that model makes the estimated mode wrong in the direction that reads as "fits".  The two address targets leave `maxPods` alone, because the EKS bootstrap computes it from the instance type's own interface and address limits.

A default VPC is the wrong place for a cluster this size, and this is the concrete reason: it gives one subnet per zone, holding 4091 usable addresses each.  `availability_zone_count` is unset by default, which uses every zone the region offers, so four zones in `us-west-2` is 16364 addresses rather than 8182.  Those addresses are not idle capacity, because `1-cluster` sizes the pools against the lowest of three ceilings and the subnets are one of them.

What all four zones cost is cross-zone transfer on every agent-to-controller connection, and twice the node groups.  Change the count between builds and not during one: each pool's `max_size` is divided by the zone count, so adding a zone drops every existing group's maximum, and a managed node group refuses a maximum below the nodes it is running.  Setting `vpc_id` and `subnet_ids` to a purpose-built VPC is still the answer for a long-lived cluster.

### The API endpoint is public and unrestricted by default

`endpoint_public_access` is true, because `.build/run-ci` runs on a laptop, and `var.public_access_cidrs` is unset, which EKS reads as everywhere.  In a default VPC the node subnets are public as well, so every agent node gets a public address.  Narrow the first to the addresses that actually run `kubectl` and `run-ci`, and give a long-lived cluster a VPC with private node subnets.  Neither is defaulted to a literal here, because no address that belongs to a person or an office belongs in this repository.

### Controller telemetry, and what it is for

`2-platform/helmfile.yaml` installs `metrics-server` beside the autoscaler, pinned to the controller node and at `system-cluster-critical`, because without it `kubectl top` answers `Metrics API not available` and the controller's own CPU and memory are unobservable.  It runs with `--kubelet-insecure-tls`, which is what the upstream manifest AWS documents for EKS also does: the kubelet's serving certificate is self-signed unless the node is bootstrapped to request a signed one, and that is a change in layer 1 rather than here.  What it gives up is authentication of the kubelet to the metrics scraper, inside the cluster's own network.

Both it and the autoscaler carry resource requests and no limits.  Neither chart sets either, and a pod with no request is BestEffort, which the kubelet evicts first on the node that also holds the Jenkins controller.  No limits, because both grow with the cluster and a guessed ceiling would OOM-kill them at the top of a scale-up.

`3-smoke/smoke-test.sh --sample-seconds N` is the other half.  Under `--build` it records `busyExecutors`, `totalExecutors`, queue length and `kubectl top pod` output every 15 seconds into a CSV, and reports the peak of each.

### Every node group is one availability zone

An agent pool is not one node group.  It is one group per zone: `agents-large-a`, `agents-large-b`, and so on.  `availability_zone_count` is unset by default, which takes every zone the region offers, so four pools in `us-west-2` are sixteen agent groups.

A managed node group handed several subnets spreads its Auto Scaling group over all of them, and then nothing can say which zone the next node will land in.  One zone per group is what makes the autoscaler's simulation of a group at zero nodes exact, and it stops one zone running short of capacity from starving the pool.

`max_size` is divided between the zones, not copied into each.  A pool of 306 across two zones is two groups of 153.  This is not cosmetic: `.build/run-ci` **sums** the maxima of every group it attributes to a size, so four groups of 306 would report a ceiling of 1224 and would accept an `instanceCap` of 1224 that could never be met.  The division is exact, with the remainder going to the earliest zones.  What it costs is that a pool can no longer reach its ceiling out of one zone alone.

That is also why layer 2 sets `balance-similar-node-groups`.  Without it the autoscaler treats each zone's group as unrelated, fills one to its share of the ceiling, and only then considers the next, so a pool stalls at a fraction of its ceiling with capacity idle beside it.  The groups differ only in labels the AWS comparator already ignores, so no `--balancing-ignore-label` is needed; that matters because the chart renders `extraArgs` as a map and cannot repeat a flag.

The controller is the exception: one group, in the first zone, not one per zone.  Its 500Gi volume binds to the zone the controller first started in and cannot move.

### Node group names are an interface

`.build/run-ci` attributes an Auto Scaling group to an agent size by finding the size word delimited by `-` or `_`, and skips the controller's pool by matching the literal `jenkins-controller`.  The names here are `jenkins-controller` and `agents-<size>-<zone letter>`.  The zone suffix is safe under that rule, because the size stays a whole word between two dashes.  Renaming them breaks the pool-ceiling check in `run-ci`, silently: `run-ci` treats a pool it cannot find as unchecked rather than as failed.

`2-platform/` sets the autoscaler's `status-config-map-name` explicitly for the same reason.  That configmap is what `run-ci` reads those ceilings from.

### Jenkins is installed by `run-ci`, and by nothing else

Layer 2 installs the cluster autoscaler and metrics-server.  It does not install the Jenkins chart.  `.build/run-ci --only-setup --values-override <file>` already does that, warns before it drops a live customisation, and refuses an `instanceCap` no pool could meet.  A second `helm upgrade` from a helmfile would do none of that, and the two would fight.

`make jenkins` runs `run-ci` with `2-platform/jenkins-eks-overrides.yaml`, or with a generated file built from it.

### A public name, and TLS for it

Set three variables together in `terraform.tfvars` and Jenkins serves on a name of your own, over HTTPS:

```hcl
enable_external_dns = true
jenkins_hostname    = "ci.example.org"
dns_hosted_zone_id  = "<hosted zone identifier>"
```

Leave all three unset and the cluster is reachable at its load balancer's own name, which is what a throwaway cluster wants.  Nothing below is created.

The hosted zone is not created by layer 1.  It outlives any one cluster, and creating it would make its identifier an output of the configuration that needs it as an input.  Create it once:

```shell
aws route53 create-hosted-zone --name example.org --caller-reference example-org-1
```

Then set the four nameservers it prints at the registrar that holds the domain.  `tofu -chdir=1-cluster output dns_nameservers` prints them again later.

Four pieces then do the rest, and the split between them is not obvious:

| Where | What |
|---|---|
| `1-cluster/tls.tf` | requests the ACM certificate, and writes the record that proves the name is yours |
| the external-dns add-on | writes the record that points the name at the load balancer |
| `Makefile` | checks the certificate is `ISSUED`, and generates the values for layer 2 |
| `2-platform/build-jenkins-values.py` | the values themselves: the URL, the annotations, the TLS port |

The address record is not written by layer 1 because it cannot be.  The load balancer is created by Kubernetes when the Jenkins Service is created, which is layer 2, so its address is not knowable in layer 1.  external-dns reads it from an annotation on that Service instead, and writes an alias record rather than a `CNAME`, which is what lets the name be a zone apex.

There is deliberately no `aws_acm_certificate_validation` resource.  That resource waits for the certificate to be issued, and ACM issues it only once the delegation above has landed, which is a person at a registrar.  With the wait, `tofu apply` blocks for its timeout and then fails with the cluster already built.  Without it, apply finishes, and the certificate becomes `ISSUED` on its own minutes after the delegation appears.  `make jenkins` reads the status on every run, so re-running it is what turns TLS on.

**Plain HTTP stays open on port 80, and that is a real limitation.**  `.build/run-ci` reaches Jenkins at `http://<load balancer>`, with the scheme written into the source, and appends `:<port>` when the Service's first port is not 80.  Moving Jenkins to 443 alone does not secure `run-ci`; it stops `run-ci` submitting builds.  So port 80 answers plain HTTP for `run-ci`, and port 443 answers TLS for a browser.  A Classic Load Balancer cannot redirect one to the other; that needs an Application Load Balancer, and so the AWS Load Balancer Controller.  Teaching `run-ci` to speak HTTPS is the fix, and it is outside this directory.

The generated values file, `.eks-jenkins-values.yaml`, is gitignored.  It exists because `run-ci` takes exactly one `--values-override`, and because a certificate ARN contains the account number.

### No account identifiers are committed

No file here contains an account number, VPC identifier, subnet identifier, elastic IP allocation, or address range.  The runbook this replaces leaked all four, including its author's home address.  Keep it that way: those belong in `terraform.tfvars`, which is gitignored, or in the environment.

The same rule covers figures that describe one account rather than one repository.  A pool `max_size`, a controller `resources` block or an `instanceCap` fitted to the quota one account happens to hold is the same mistake in a different currency: layer 1 scales the pools to the account at apply, and `build-jenkins-values.py` rewrites each `instanceCap` from what layer 1 created.

### The load balancer annotations are commented out

`2-platform/jenkins-eks-overrides.yaml` carries three commented service annotations, which name the load balancer, its subnets, and an elastic IP allocation.  They belong to the AWS Load Balancer Controller, not to the in-tree cloud provider, and this directory does not install that controller.  Without it, nothing reads the elastic IP annotation, so the address is not stable across a reinstall.

A name does not need that stability.  external-dns re-points the record at whatever load balancer the Service has now, so `ci.example.org` survives a reinstall even though the address under it does not.  A fixed address is needed only by something that can hold nothing but an address, such as a firewall rule written by hand.  Install the controller for that, and then uncomment the annotations.

### Two add-ons are off, against the runbook

`enable_cloudwatch_observability` and `enable_external_dns` both default to `false`.  The runbook turned both on.  CloudWatch Observability bills per log and per metric on every node, which the agent pools then multiply.  external-dns has nothing to manage until a hosted zone exists in this account, so it is turned on by the same change that names the zone.  Turn either on deliberately, in `terraform.tfvars`.

Neither gets the managed policy AWS documents for it.  For external-dns that policy is `AmazonRoute53FullAccess`, which is every record in every zone in the account; `1-cluster/iam-addons.tf` grants the three actions it needs instead, in the named zones only.

### Add-on roles are Pod Identity, and scoped to this cluster

Every add-on role but the autoscaler's is assumed through Pod Identity, because the association is an argument on `aws_eks_addon`: the role, the add-on and the binding between them are one resource graph, with no OIDC provider or certificate thumbprint in the middle.  The trust policy carries `aws:SourceArn` on this cluster and `aws:SourceAccount` on the account, without which `pods.eks.amazonaws.com` is every cluster in the account, and this directory's naming deliberately allows a second one.

The cluster autoscaler is the exception and uses IRSA, because the autoscaler maintainers document that path and do not mention Pod Identity at all.  `1-cluster/iam-autoscaler.tf` says what deleting that exception would remove.

## Two findings about the shared files, not fixed here

Both are in files this change deliberately does not touch, and each needs its own ticket.

1. **`controller.node-selector` in `../jenkins-deployment.yaml` does nothing.**  The Jenkins chart's key is `controller.nodeSelector`.  On GKE the mistake is harmless, because the controller lands on the only pool that exists at install time.  Here it is harmful: the controller can land on an agent node, which the autoscaler then scales back to zero with the build queue on it.  `2-platform/jenkins-eks-overrides.yaml` sets the correct key for EKS, which covers this cluster.  The shared file is still wrong for GKE.

2. **`hostName`, not `hostname`.**  The chart's ingress key is `controller.ingress.hostName`, with a capital N.  A lower-case `hostname` is accepted by Helm, ignored by the chart, and produces an ingress with no host rule.

## Layout

The arithmetic that is not about AWS lives one directory up, in `../shared`, so that the sibling directory for
another cloud shares it rather than copying it: Kubernetes quantity parsing, Helm's merge rule, the Jenkins
controller sizing model, and the spend windows and least-squares fit.  What stays here is everything AWS
decides.  `../shared/README.md` says what is deliberately not shared, and why.

```
Makefile                              the join between layers; `make help`
README.md                             this file: how to run it, and how to debug it
NOTES.md                              what was measured, on which run, and what is unproven
vcpu-quota.py                         the account's vCPU quota, as a node ceiling for layers 2 and 3
controller-fit.py                     whether the controller can hold the configured pools
spend-caps.py                         asks what this may spend, and writes it where OpenTofu reads it
spend-guard.py                        what has been spent against those caps, and the brake; also a Lambda
*-test.sh                             regression tests for each of the above, every one stubbing `aws`
../shared/                               the cloud-neutral arithmetic these four share with the other clouds
1-cluster/
  versions.tf                         required_version, providers, unconfigured backend
  variables.tf                        every input, each with the consequence of changing it
  terraform.tfvars.example            copy to terraform.tfvars
  data.tf                             VPC, subnets, versions, quotas; everything read from AWS
  locals.tf                           the node ceiling, the pool scaling, names, labels, ASG tags
  iam.tf                              cluster and node roles, and the Pod Identity trust policy
  iam-addons.tf                       Pod Identity roles, one per add-on that needs one
  iam-autoscaler.tf                   OIDC provider and the autoscaler's IRSA role
  cluster.tf                          the control plane and its log group
  node-groups.tf                      one group per zone per pool, and the ASG tags on them
  addons.tf                           the add-ons, in a deliberate order
  tls.tf                              the certificate and its validation record
  spend-guard.tf                      the caps, the Lambda that acts on them, and who is told
  outputs.tf                          the whole interface to layers 2 and 3
2-platform/
  helmfile.yaml                       two releases: the cluster autoscaler and metrics-server
  cluster-autoscaler-values.yaml.gotmpl
  metrics-server-values.yaml.gotmpl
  jenkins-eks-overrides.yaml          passed to run-ci --values-override
  build-jenkins-values.py             adds this site's name, certificate and agent ceiling to that file
  build-jenkins-values-test.sh
3-smoke/
  smoke-test.sh                       assertions against a deployed cluster
  check-pool-fit.py                   does one agent fit on one node of its pool
  check-pool-fit-test.sh
```

`make test` runs every `*-test.sh` above plus `tofu fmt -check` and `tofu validate`, needs no credentials, and is what `.github/workflows/jenkins-check.yaml` runs on every change under `.jenkins/`.

## Not goals

**Karpenter.**  `.build/run-ci` reads each pool's ceiling out of the `cluster-autoscaler-status` configmap, which Karpenter does not publish.  Moving to Karpenter means changing `run-ci` first.

**Anything outside this directory.**  The `Jenkinsfile`, `.build/run-ci` and `../jenkins-deployment.yaml` are not touched by this directory's work.

Four values in `../jenkins-deployment.yaml` matter to it and are already there, from the earlier change that measured them: `agent.waitForPodSec`, and `slaveConnectTimeout` and `idleMinutes` on every template, which are the deadlines and the reuse discussed in [Agent pods churn and no agent connects](#agent-pods-churn-and-no-agent-connects).  Every one of them is wrong for GKE too, and none of them names an account.

The same earlier change added the `agent-dind-report` template and moved `generateTestReports` onto it.  What belongs here is the `report` pool in `1-cluster/variables.tf`, which is the node group that template selects, sized so that the build's last task cannot queue behind another template's agents and so that the merge's concurrent `ant junitreport` jvms have the memory the template's 9G dind limit assumes.
