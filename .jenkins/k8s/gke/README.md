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

# Jenkins CI on Google Kubernetes Engine

`../README.md` provisions this cluster on GKE with four `gcloud container` commands.  They work, and they are still the fastest way to get a cluster to look at.  What they do not do is record what was created, size the pools to what the project allows, or put any limit on what the result may spend, and a cluster built from them cannot be replayed by anybody but the person who typed them.

This directory is the same cluster as an OpenTofu configuration, a Helmfile and a set of assertions, alongside the AWS equivalent in `../eks/`.  It is written against that sibling rather than from scratch, so where a decision here differs from the one there, the comment says which and why.

`NOTES.md` distinguishes deployed-cluster observations, offline tests and estimates carried over from EKS. The controller throughput model still needs measurement on GKE.

Run `make plan` before `make apply`, and `make caps` before either.  No project quota is a limit on money.

## Contents

- [The three layers](#the-three-layers)
- [Prerequisites](#prerequisites)
- [Quick start](#quick-start)
- [What cannot be scripted, and is not attempted](#what-cannot-be-scripted-and-is-not-attempted)
- [A cap on what this cluster may spend](#a-cap-on-what-this-cluster-may-spend)
- [What run-ci cannot check here](#what-run-ci-cannot-check-here)
- [When something stops part way](#when-something-stops-part-way)
- [Decisions worth knowing before you apply](#decisions-worth-knowing-before-you-apply)
- [Layout](#layout)
- [Not goals](#not-goals)

## The three layers

| Layer | Directory | Tool | Owns |
|---|---|---|---|
| 1 | `1-cluster/` | OpenTofu | cluster, node pools, identity, certificate, the spend guard |
| 2 | `2-platform/` | Helmfile, kubectl, and `.build/run-ci` | storage class, external-dns, Jenkins site overrides |
| 3 | `3-smoke/` | Python, bash | post-deploy assertions |

Data flows one way.  Layer 1 owns GCP and prints what it made; the `Makefile` turns that into environment variables and dot-files; layers 2 and 3 read only those.  Nothing in `2-platform/` or `3-smoke/` opens a tfstate file, and nothing in `1-cluster/` talks to Kubernetes.  The `Makefile` is the join and the only place the layers meet.

Layer 2 is thinner than the sibling's, and that is GKE doing the work rather than a gap.  The cluster autoscaler, metrics-server, kube-proxy, kube-dns and the persistent disk CSI driver are all part of GKE, so where `../eks/2-platform` installs two Helm releases and `../eks/1-cluster` installs six add-ons with four IAM roles between them, here the autoscaler is three lines of `google_container_cluster` and the disk driver is a boolean.  What is left in layer 2 is external-dns, which GKE has no add-on for, and the Jenkins values.

## Prerequisites

`tofu` (1.9.0 or newer), `helmfile`, `helm`, `kubectl`, `gcloud`, `python3`, `jq`.

The `helm-diff` plugin, because `helmfile apply` diffs before it applies:

```shell
helm plugin install --verify=false https://github.com/databus23/helm-diff     # helm 4
helm plugin install https://github.com/databus23/helm-diff                    # helm 3
```

Helm 4 verifies plugin signatures by default and helm-diff publishes none, which is what `--verify=false` is for.  Helm 3 does not accept that flag.  `make externaldns` checks for the plugin first and prints the right one of those two, because helm's own error for a missing plugin reads as a fault in `helmfile.yaml`.

A credential, and confirmation that it is the one you meant:

```shell
gcloud auth login
gcloud auth application-default login
gcloud config get-value project
```

Two logins, and both are needed: `gcloud auth login` is what the `gcloud` commands in the `Makefile` and in layer 3 use, and `gcloud auth application-default login` is what the OpenTofu provider uses.  Having one without the other produces a failure in whichever half you reach first, saying nothing about the other.

`project` in `terraform.tfvars` is the only value with no default, and it is the boundary everything else is scoped to: billing, quota and IAM at once.  **Run this cluster in a project of its own.**  The spend caps below are caps on the whole project's bill, and the vCPU quotas the pools are sized against are the project's too.

## Quick start

Choose a short `cluster_name`, such as `mck--jenkins`: at most 18 characters with spend caps, 17 with external-dns, or 25 with neither.  Service account suffixes must fit Google's 30-character account id limit.

```shell
cd .jenkins/k8s/gke

cp 1-cluster/terraform.tfvars.example 1-cluster/terraform.tfvars
$EDITOR 1-cluster/terraform.tfvars      # at least project, and probably cluster_name and region

make test                               # no credentials needed
make caps                               # what this may spend a day, a week and a month
make plan                               # read this, including the agent_node_ceiling output
make apply
make kubeconfig
make quota                              # how many agents the project allows, and whether the controller fits
make platform                           # storage class, external-dns, then Jenkins through run-ci
make smoke
make spend                              # what it has spent against those caps
```

Five things about that order.

`make quota` comes after `make apply`, not before: it reads the node pools that were created, not the ones that were asked for, and those differ whenever the project's quotas scaled them.  Before the first apply, `make plan` prints the same ceilings as the `agent_node_ceiling` output.

`make caps` comes before `make apply`, and `make apply` asks for the caps itself when they have not been set.  Answering `none` to all three is an answer and it means nothing watches what this cluster spends.

`make smoke SMOKE_ARGS='--build --sample-seconds 3600'` submits a real build and samples the controller while it runs.  It is the only check the offline suites cannot reach, because everything that matters about a CI cluster is whether a queued task gets a node.

A public name needs three more values in `terraform.tfvars` and one step nobody can script.  **Read `1-cluster/tls.tf` first**: on GKE a certificate cannot be attached to a plain LoadBalancer Service, so what layer 1 gives you is an issued certificate and no way to serve it until an Ingress exists.  See [A public name, and what TLS costs here](#a-public-name-and-what-tls-costs-here).

`make destroy` takes the controller's disk with it.  The PersistentVolumeClaim carries `helm.sh/resource-policy: keep`, which protects it from a `helm uninstall` and not from the cluster being deleted underneath it.

## What cannot be scripted, and is not attempted

Four things upstream of this directory.  Each one shows up as an apply or a build stopping part way, so they are worth doing first.

**The project, and its APIs.**  A project has to exist and have billing attached before anything here runs.  `1-cluster/cluster.tf` enables the APIs it needs, which covers the ordinary case; what it cannot do is attach billing, and an unbilled project refuses to enable an API at all.

**The quotas.**  A new project's regional `CPUS` quota is small, and the pools here ask for hundreds of vCPU.  Layer 1 reads the quotas and scales the pools to them, so raising one is optional and a smaller project simply gets a smaller cluster.  What to read, and why it is more than one number:

```shell
gcloud compute regions describe us-central1 --format='json(quotas)'
gcloud compute project-info describe --format='json(quotas)'
```

E2 and N1 use `CPUS`; N2 and the other supported series use their own regional CPU quota. Spot uses `PREEMPTIBLE_CPUS` when the project reports it, otherwise ordinary CPU quotas. Every node also consumes `CPUS_ALL_REGIONS` when that global quota is reported. Disk capacity uses `DISKS_TOTAL_GB` for pd-standard, `SSD_TOTAL_GB` for pd-balanced and pd-ssd, and `HDB_TOTAL_GB` for Hyperdisk Balanced. See [Google's quota definitions](https://cloud.google.com/compute/resource-usage).

The quota reader supports E2, N1, N2, N2D, T2D, C2, C2D, C3, M1, M2 and M3. It rejects newer series whose quotas need a `vm_family` dimension, rather than treating an unread quota as unlimited. Hyperdisk IOPS and throughput quotas are not modeled.

**The pod IP range.**  This one is different from the others: it cannot be raised afterwards.  GKE gives every node a slice of the cluster's pod range, and the range is fixed when the cluster is created, so it caps how many nodes the cluster may ever hold.  Leaving `pod_range_name` unset lets GKE create a /14, which at the default 110 pods a node is 1,024 nodes.  To choose it:

```shell
gcloud compute networks subnets update <subnetwork> --region <region> \
    --add-secondary-ranges cassandra-jenkins-pods=10.4.0.0/14,cassandra-jenkins-services=10.8.0.0/20
```

**Cloud NAT.** Private nodes need outbound connectivity for image and plugin downloads. With `enable_cloud_nat = true`, layer 1 creates it. Otherwise provide NAT or equivalent egress; the following commands create NAT manually:

```shell
gcloud compute routers create <cluster>-router --network=default --region=<region>
gcloud compute routers nats create <cluster>-nat --router=<cluster>-router \
    --region=<region> --auto-allocate-nat-external-ips --nat-all-subnet-ip-ranges
```

`3-smoke/smoke-test.sh` checks that a public NAT covers the configured subnet's primary IP range. It does not prove end-to-end connectivity or recognize a proxy or transit-VPC route.

**DNS delegation**, if you want a public name.  Create the zone once, then set its nameservers at the registrar that holds the domain.  Nothing here can do the second half.

```shell
gcloud dns managed-zones create example-org --dns-name=example.org. --visibility=public
gcloud dns managed-zones describe example-org --format='value(nameServers)'
```

## A cap on what this cluster may spend

Every ceiling in the next section is a ceiling on size, and none of them is a ceiling on money: a quota limits what may exist at once, and a bill limits nothing.  These pools can hold hundreds of nodes.  The sibling held 476 nodes for three and a half hours on one build, which is about $800 of instance time.

Three caps, in US dollars, over UTC windows: a day from midnight, a week from Monday, and a calendar month.  When one is met, every agent node pool's autoscaling maximum is set to zero.

### What to run

```shell
make caps      # asks for the three caps and an address; needs no credential
make apply     # creates the guard
make spend     # what has been spent against them, and whether the pools are stopped
```

`make caps` prints the arithmetic before it asks: what the cluster costs standing still, what the pools cost an hour at full size, and how many hours of that each answer holds.  It refuses a cap at or below the floor, because the floor runs whether a build does or not.

The answers land in `1-cluster/spend-caps.auto.tfvars`, which is gitignored and which OpenTofu loads on its own, so a generated file and a hand-edited `terraform.tfvars` never share a line.

### How the three windows are measured, and the one real difference from AWS

The sibling fits the price of a vCPU-hour from the account's own bill, over the settled days of the last fortnight, using Cost Explorer.  Within three days of a cluster existing, its configured price is unused.

GCP has no Cost Explorer.  The only per-day, per-SKU cost source is the BigQuery billing export, and it is enabled on the *billing account* rather than the project, backfills nothing, and is a table this configuration cannot create.  So there are two modes, and `make spend` says which it is in on every run:

| `spend_billing_table` | What the caps are compared against |
|---|---|
| unset, the default | `price × vCPU-hours + fixed × elapsed day`, with both figures as configured |
| set | the same estimate, fitted against the bill exactly as the sibling does, with the residual reported as an error margin |

In both modes the vCPU-hours come from Cloud Monitoring's `compute.googleapis.com/instance/cpu/reserved_cores`, which is free and is published whatever `monitoring_components` is set to.  The guard asks Monitoring to align and sum for it over the hour, which sidesteps the fault that cost the sibling two separate factor errors: it derived a sampling interval and was wrong by five, then wrong by thirty.  What it must still handle is that an alignment window is anchored on its end, so an hour can straddle midnight and has to be apportioned across the two days; the daily cap is exactly the figure that breaks if it is not.

Set `spend_price_per_vcpu_hour` from what `make spend` reports rather than from a list price.  GCP applies sustained-use discounts automatically, up to about 30% on N1 and N2 over a full month, so a list price over-reads a month and under-reads a day.  The sibling's account billed a fortieth of the list price per vCPU-hour, which is how badly this can be wrong.

The billing query selects this project's rows and converts net cost, including credits, to USD using each row's currency conversion rate. The table can therefore contain multiple projects and a non-USD billing account.

The deployed function calls Google REST APIs with its service-account token from the metadata server. Local reports use the operator's CLI credentials. After a successful state write, the guard retains the three newest non-destroyed secret versions plus OpenTofu's bootstrap version, and destroys older versions. Keeping the bootstrap prevents a later apply from recreating empty state.

Monitoring sends email when a cap is exceeded, the guard reports braking, or spend is unknown, and on incident closure. Cap notifications do not wait for GKE to finish the brake operation. A separate `absent_over_time` alert covers silence even before the first successful invocation. Pub/Sub also receives detailed transition reports; reading those requires a subscriber. Verify email delivery after deployment, including the recovery notification.

### What the brake is, and what a build sees

| | |
|---|---|
| no new agent node starts | the pool's autoscaling maximum is zero, so the autoscaler cannot ask for one |
| every running agent finishes | nothing is drained, evicted or deleted by the brake |
| the nodes then go away | the autoscaler still scales in, so the pools drain to zero as builds finish |
| the controller keeps running | its own pool is never touched: it holds the build queue and `jenkins_home` |

Two things this is not.  It is not `--num-nodes 0`, which deletes running nodes and kills every in-flight build.  And it is not cordoning the nodes, which would stop pods scheduling and therefore make *more* pods pending, which is what the autoscaler adds nodes for.

The deployed pool maxima are authoritative when restoring capacity. Saved state records which pools the guard braked; older state falls back to comparing observed and normal maxima. Reducing a pool in OpenTofu therefore cannot restore an obsolete saved maximum. A pool configured for one node is not braked merely because its maximum is one. Read the live maximum with:

```shell
gcloud container node-pools describe <pool> --cluster <cluster> --region <region> \
    --format='value(autoscaling.maxNodeCount)'
```

Three GKE-specific things the sibling does not have to deal with.

A node pool update is a long-running operation, and a pool with one in flight rejects a second, so an evaluation that lands mid-upgrade cannot brake that pool and converges on the next one instead.  One busy pool never leaves the others running.

The update is also asynchronous, so reading the brake back immediately afterwards still reports the old maximum: the operation has been accepted and has not finished.  Suspending a process on an Auto Scaling group takes effect by the time the call returns, so the sibling can simply read the brake and believe it.  Here `make spend` reports `STOPPING` when every pool has accepted the change and none has shown it yet, and `STOPPED` once GKE agrees.  Reporting only what is observed would say the pools were running seconds after they were stopped, which is the reading that would have somebody stop them twice.

Whether GKE accepts a maximum of *zero* is unverified.  If GKE refuses zero, the guard falls back to one and reports the remaining allowance.  That still permits builds on one node per pool per zone.  For a pool whose normal maximum is already one, the guard retries zero on later evaluations; a maximum of one cannot establish that such a pool was braked.

**A braked cluster has every symptom of a broken autoscaler**: pods Pending, pools at zero, and no failed operation anywhere, because a maximum of zero produces no failure.  `make spend` and `make smoke` both name the brake for that reason.

### Unknown spend stops the pools

A guard that cannot see stops CI loudly rather than leaving the pools running unwatched.  Four things are unknown, and each brakes:

- the caps cannot be read, so there is nothing to compare against;
- the Cloud Monitoring read fails;
- the latest metric is absent or older than one alignment interval plus ten minutes while Compute reports running instances;
- the node pools cannot be read, or setting a maximum failed.

The billing export failing is deliberately **not** unknown.  Every day in every window can still be estimated, so what is lost is the fit and not the figure.

`make spend` and `make smoke` use the local gcloud identity.  A local `secretmanager.versions.access` denial does not show that the deployed guard's service account also lacks access.  Grant the operator read access to the caps and state secrets, scoped to those two resources:

```shell
. ./.gke-env
for secret in "$GKE_SPEND_CAPS_SECRET" "$GKE_SPEND_STATE_SECRET"; do
    gcloud secrets add-iam-policy-binding "$secret" --project="$GOOGLE_PROJECT" \
        --member='user:<operator-email>' --role=roles/secretmanager.secretAccessor --condition=None
done
```

This command requires permission to update each secret's IAM policy.  Check the deployed guard separately with `gcloud functions logs read "$GKE_SPEND_GUARD_FUNCTION" --gen2 --project="$GOOGLE_PROJECT" --region="$GKE_LOCATION" --limit=50`.

### What watches the guard

Every evaluation writes an `evaluation_ok` point, and a Cloud Monitoring alert policy fires on its *absence*.  That is the direct equivalent of the sibling's "treat missing data as breaching", and a cleaner spelling of it: the alarm is on the silence rather than on a value.  A dead guard is silent by nature, and silence otherwise reads as everything being fine.

The window is derived from the interval rather than fixed, so a long interval does not page for a working guard.

A Cloud Billing budget is the second opinion, and it enforces nothing: it is Google's reading of Google's own billing data, so the two disagreeing is worth knowing.  It is off unless `billing_account` is set, because it needs a role on the billing account rather than on the project, which is usually a different person.  There is one budget where the sibling creates two: Cloud Billing budgets have no daily or weekly period at all, so those two windows are the guard's alone and nothing in the console reports them.

### What the cap does not cover

The floor keeps running.  The controller is one instance for all 730 hours of a month, the cluster carries a management fee beside it, and its 500Gi disk and the load balancer are charged whatever happens.  A monthly cap under that floor is met on the first with no build having run.  `make caps` refuses such a cap; a figure set by hand in `terraform.tfvars` is not checked, because nothing here knows what a machine costs.

Jenkins is not told.  It keeps queueing tasks that cannot be given a node, and each one expires after `agent.waitForPodSec`.

## What run-ci cannot check here

`.build/run-ci` refuses to deploy an `agent.podTemplates.*.instanceCap` larger than the nodes its pool can hold.  It establishes that ceiling by reading the `cluster-autoscaler-status` configmap, which the upstream cluster autoscaler writes.

**GKE publishes no such configmap.**  Its autoscaler is part of the control plane.  So on GKE run-ci finds nothing, reports the check as not established, and deploys.  That is correct behaviour and it is already in run-ci, written before this directory existed; what it means is that one safety check the sibling has is absent here.

Two things replace it, and both are in this directory:

- `make quota` reads every pool's maximum from GKE itself, and `make jenkins` sets `agent.containerCap` and each `instanceCap` from those figures rather than from what is committed.  So the caps deployed here are derived from the pools that exist, not merely checked against them.
- `3-smoke/smoke-test.sh` asserts every expected pool exists and is autoscaled with a maximum of at least one, which is the fact the configmap would otherwise have carried.

Self-managing the autoscaler would restore the configmap.  It is a [not goal](#not-goals).

## When something stops part way

**`logging.buckets.create` is denied for `_Default`.**  Set `log_retention_days = null` to leave retention under project administration; cluster log collection continues.  To manage retention here, the applying identity needs `logging.buckets.get` and `logging.buckets.update`, plus `logging.buckets.create` if the bucket does not exist.  The project administrator can grant `roles/logging.configWriter`.  The provider attempts creation when its initial bucket read fails, so this error can also indicate missing read permission or a bucket outside the configured `global` location.

**`container.managed.disableRBACSystemBindings` rejects cluster creation.**  The cluster explicitly disables non-default bindings to `system:authenticated`, `system:unauthenticated` and `system:anonymous`.  Keep both `rbac_binding_config` options false to satisfy this organisation policy.

**The function location is not found or access is denied.**  Choose a `region` supported by the Cloud Functions v2 API and the configured node types, and permitted by the organisation.  `europe-north1` supports the default node types and second-generation functions.  Cloud Run availability alone does not establish Cloud Functions API availability.  Review `make plan` after a region change: existing regional resources can require replacement, and regional quota increases do not transfer between regions.

**Cloud Scheduler rejects `europe-north1` as an invalid location.**  Cloud Scheduler has its own [supported regions](https://cloud.google.com/scheduler/docs/locations), which exclude Finland.  Set `spend_scheduler_region = "europe-west1"` to put the job in Belgium.  Keep `region = "europe-north1"` for the cluster and function.  The job invokes the function's HTTPS URL with its existing service account and OIDC audience.  Without an override, the job uses `region`.  The environment output and smoke check use the job's region for Scheduler commands.

**The spend-guard alert cannot find `evaluation_ok`.**  Layer 1 creates the heartbeat metric descriptor before the alert policy.  The descriptor matches the guard's `DOUBLE` values and `cluster` label.  If Cloud Monitoring still reports a propagation delay immediately after descriptor creation, retry after the delay reported by the API.

**The function cannot clone its source object after a bucket replacement.**  Replacing a bucket under the same name deletes its objects even though their configured bucket name remains unchanged.  The source object now follows bucket replacement, and the function uses its uploaded generation.  To recover an earlier failed apply, run `make apply TOFU_ARGS='-replace=google_storage_bucket_object.spend_guard[0]'` from this directory.

**The source object reports `root object was present, but now absent` after a bucket replacement.**  Cloud Storage [bucket recreation is eventually consistent](https://cloud.google.com/storage/docs/consistency#eventually_consistent_operations), so reusing the same bucket name can leave it inaccessible for several minutes.  This can occur when changing `region` or rebuilding immediately after destruction.  Wait several minutes, then retry `make apply`.  If the failed source object is absent from state, a normal apply uploads it again.  If the error persists, inspect the live object and provider logs before attributing it to propagation.

**The function build fails with a missing permission on the build service account.**  The default build account can lack permissions in projects that disable automatic IAM grants.  The function now uses a separate `<cluster_name>-spend-build` account with `roles/logging.logWriter`, `roles/artifactregistry.writer` and `roles/storage.objectViewer` in the project, following [Google's custom build account guidance](https://cloud.google.com/functions/docs/building#provide_a_service_account_for_building_functions).  These grants cover Google's source copies and image repository.  Function creation waits for the IAM policy writes to finish.  Run `make apply` to create the account and retry the build.

**The build still reports source-bucket access denied just after the grants succeed.**  IAM policy writes can complete before the permissions become effective.  [Google documents propagation times](https://cloud.google.com/iam/docs/access-change-propagation) of typically two minutes, potentially seven minutes or longer.  Wait several minutes after the grants, then retry `make apply`.  If the denial persists, check the live project policy and build or audit logs for an IAM deny policy or VPC Service Controls restriction.

**`Variables may not be used here`, on an output.**  An output's `description` cannot interpolate.  It is worth knowing because the message names the line and not the rule, and because `tofu init` catches it while `tofu fmt` does not.

**`Failed to read any lines from plugin's stdout`.**  Not a fault in the configuration: OpenTofu could not launch the provider as a subprocess.  Seen inside a sandbox that denied the plugin its handshake.  `tofu init` still works, because it downloads rather than runs, so a configuration can be parsed and locked in an environment where it cannot be validated.

**`make env` says layer 1 has not been applied.**  Every target below `make plan` reads the cluster's own state.  Run `make apply`, or `make plan` for the ceilings alone.

**A node pool update is rejected while another is in flight.**  GKE serialises operations per pool.  Wait, or let the spend guard's next evaluation converge.  This is the one failure that is neither a fault nor a fix: it is GKE's concurrency model, and every tool that touches a node pool meets it.

**Agent pods churn and no agent connects.**  A deadline shorter than the wait.  The sibling measured 1,110 pods created and deleted in 24 minutes with no agent ever connecting, against a 30-second `slaveConnectTimeout`, followed by 483 `KubernetesProvisioningLimits ... went below zero` warnings after which no cap binds at all.  Both deadlines apply to the same wait, so raising one alone changes nothing; `3-smoke/check-pool-fit.py` asserts `slaveConnectTimeout` and `agent.waitForPodSec` against a 300-second floor.

**Every agent pod in `ImagePullBackOff`.**  Private nodes with no Cloud NAT.  See above; `make smoke` names it.

**HTTP load balancing is disabled.**  The generated GKE values use the Service load balancer and disable the shared chart's Ingress.  Run `make jenkins` to remove an Ingress left by older values.  The smoke check requires the add-on only when a Jenkins GCE Ingress exists.

**Small agents run, but the JAR tasks stay queued.** The Jenkinsfile holds a `cassandra-small` agent for the entire pipeline, then requests `cassandra-amd64-small` workers for its JAR tasks. If both labels share one template, overlapping pipelines can occupy every slot and wait forever for workers.

Concurrency follows the applied small-pool capacity. Each admitted pipeline gets a budget of one outer agent plus `small_workers_per_build` workers, default 3 for the current single-architecture JAR stage. With `S` small-node slots and global Jenkins cap `C`, the generated values use:

```text
budget       = min(S, C)
pipeline cap = floor(budget / (1 + small_workers_per_build))
worker cap   = budget - pipeline cap
```

Four slots admit one pipeline with three workers; eight admit two with six; ten admit two with eight; twelve admit three with nine. This is the largest pipeline count that retains the configured worker budget for every admitted build. It is a capacity policy, not a measured throughput optimum: later stages share the medium, large and report pools, and their concurrency depends on the selected profile.

The generated Helm values split the small template into:

- `agent-dind-pipeline`: only `cassandra-small`, capped at the derived pipeline count.
- `agent-dind-small`: the architecture-specific worker labels, capped at the remaining budget.

Both templates use exclusive label matching and stable IDs for the plugin's cap accounting. Extra pipelines wait before acquiring an outer agent. The declared small/medium/large pool ratios and the other pools' allocations stay unchanged. Scaling preserves a minimum of four small slots so one pipeline can run its three JAR workers, then checks the quota and address budgets. The pool can still scale to zero when idle. The Kubernetes plugin documents [label matching](https://plugins.jenkins.io/kubernetes/#using-a-label) and enforces [cloud and template caps](https://github.com/jenkinsci/kubernetes-plugin/blob/master/src/main/java/org/csanchez/jenkins/plugins/kubernetes/KubernetesProvisioningLimits.java).

A quota increase can admit more pipelines as the applied small pool grows. Keep an explicit `agent_pools.small.max_size` at least `small_workers_per_build + 1`. Change `small_workers_per_build` in `1-cluster/terraform.tfvars` if the build matrix needs a different worker budget. Run `make plan`, `make apply`, then `make jenkins` to update both layers. Raising only the node pool maximum leaves Jenkins's caps unchanged.

For the first migration from the shared template, stop the stalled builds and remove their idle small agents in Jenkins before `make jenkins`. Existing agents retain their old labels and template IDs. Start replacement builds after deployment; the new outer agents have names beginning `agent-dind-pipeline-`. A passing smoke check verifies capacity, not that an existing build has progressed.

**A PersistentVolumeClaim stays Pending with nothing reported.**  Its storage class does not exist.  `make storageclass` creates the pd-ssd class the controller's claim names, and `make platform` runs it first for that reason.

**The Jenkins controller's pod stays Pending.**  Its `resources.requests` exceed the node's allocatable.  `make quota` refuses that before the deploy, and `CONTROLLER_FIT_ARGS=--warn-only` does not cover it: `--warn-only` accepts a controller that will be slow, and there is nothing to accept about one that never starts.

**Agent pods report `NotTriggerScaleUp` with `Insufficient ephemeral-storage`.**  The autoscaler cannot fit the pod on a new node of its pool.  Each agent requests 50Gi of ephemeral storage, but the former 107-size COS boot disk reported only 48.03 GiB allocatable.  Agent disks now default to 200 GiB.  Run `make plan` and `make apply` to update existing pools; update any explicit `agent_pools.*.disk_gb` overrides too.  The controller boot disk and Jenkins home PVC have separate settings.

`3-smoke/check-pool-fit.py` now uses live node allocatable storage minus DaemonSet requests when a node is running.  For pools at zero, it estimates [GKE's system and eviction reservations](https://cloud.google.com/kubernetes-engine/docs/concepts/plan-node-sizes#local_ephemeral_storage_reservation), with a conservative 10% allowance for the COS filesystem layout.  That allowance is a planning margin, not a published GKE reservation.  Run `make smoke` after the pool update.  Raising pod startup timeouts does not fix insufficient storage.

**Helm cannot patch Roles or RoleBindings.**  The identity running `make platform` needs permission to manage the chart's Kubernetes RBAC objects.  `roles/container.admin` includes `container.roles.update` and `container.roleBindings.update`; it grants full GKE administration in the project.  An IAM administrator can grant it with:

```shell
gcloud projects add-iam-policy-binding <project> \
    --member='user:<email>' --role=roles/container.admin --condition=None
```

An existing cluster administrator can instead grant the deployer the Kubernetes `admin` role in Jenkins' namespace.  After permissions propagate, retry `make jenkins`.  The missing ConfigMap-reader RoleBinding can also prevent `config-reload-init` from starting Jenkins; inspect that container's logs if the pod reports `Init:Error`.  The later `init` container has not run while the first container is failing.

**`nil pointer evaluating interface {}.enabled`, naming the Jenkins chart.**  A `controller.ingress:` key with only comments under it is `ingress: null`, and Helm coalesces a null *over* the chart's default map, so the map vanishes.  Comment the whole key out, not just its contents.  `2-platform/jenkins-gke-overrides.yaml` says so where it would happen.

**`make spend` says UNKNOWN and the pools are braked.**  The guard working.  Read which of the four unknowns before anything else, fix that, and `make apply`; the function's source hash changes with the file, so an apply redeploys it.

## Decisions worth knowing before you apply

### State is not configured

The backend is unset, so state is a file in `1-cluster/`.  That is right for one person and wrong for a shared cluster: two applies would race with no locking.  `versions.tf` carries a commented `gcs` backend, which locks on the state object's own generation and so needs no second resource, unlike the sibling's S3 backend and its lockfile argument.

A tfstate holds the project and every resource identifier in clear, which is why the root `.gitignore` excludes it.  `.terraform.lock.hcl` *is* committed, because its checksums are what pin the providers; `.build/build-rat.xml` excludes it from the licence check instead, because OpenTofu rewrites it on every init and drops any header put in it.

### Versions are derived from Google, not written down

The Kubernetes version comes from `data.google_container_engine_versions`, as the release channel's default.  The node image comes from the control plane's version.  Nothing here pins either.

The cost is a plan that changes without this configuration changing.  There is a second cost that the sibling does not have: `min_master_version` is a *floor*.  Inside a release channel GKE will upgrade the control plane above it on its own schedule, so pinning `kubernetes_version` is weaker here than the sibling's `version` argument, and the only way to hold a version still is `release_channel = "UNSPECIFIED"`, which makes every upgrade yours to perform and yours to forget.

An upgrade rolls the node pools after it, draining nodes through the Eviction API, which `.jenkins/Jenkinsfile` turns into a failed branch and `failFast` turns into a failed run.  The pools are configured to surge rather than drain first, which bounds it and does not remove it.  Upgrade between builds.

### Four ceilings, and the lowest one wins

$$N_{\text{agents}} = \min(N_{\text{quotas}},\ N_{\text{pods}},\ N_{\text{nodes}},\ N_{\text{pools}})$$

| | ceiling | read by |
|---|---|---|
| $N_{\text{quotas}}$ | the lowest of every quota these pools spend from, less the controller's node | `1-cluster`, `make quota` |
| $N_{\text{pods}}$ | addresses in the pod range, divided by a node's slice of it | `1-cluster` |
| $N_{\text{nodes}}$ | addresses in the subnetwork's primary range | `1-cluster` |
| $N_{\text{pools}}$ | $\sum_{p} \text{max\_size}_p$, what `var.agent_pools` declares | `1-cluster`, `make quota` |

Layer 1 reserves one node for each enabled pool, except `small`, which reserves a pipeline slot plus `small_workers_per_build` worker slots. It distributes the remaining budget proportionally and caps each pool at its declared maximum. An impossible minimum fails the plan. `tofu output agent_node_ceiling` reports the ceilings, reserved minima and allocation factor.

Quota demand is computed separately for each applicable metric. The controller's actual charge is reserved first. The Jenkins agent ceiling accounts for each pool's bounded contribution, so an E2 quota does not cap N2 agents. Missing applicable quotas are reported as unverified; these checks do not reserve capacity against other workloads in the project.

$N_{\text{pods}}$ is the one to think about before creating the cluster, because it is the only ceiling here that cannot be raised afterwards.  A node's slice of the pod range holds twice `max_pods_per_node`, rounded up to a power of two: 110 pods takes 256 addresses and 60 takes 128, so halving `max_pods_per_node` doubles the nodes the same range holds.  An agent node runs one build pod and a few DaemonSets, so it needs nowhere near 110.  It is left at the default because it also moves the kubelet's memory reservation, which `3-smoke/check-pool-fit.py` models, and the fit check is what confirms the two agree.

The quotas are read live and deliberately not held in state.  A live read at deploy time is not a live read afterwards: `GKE_MAX_AGENT_NODES` freezes into Jenkins' `agent.containerCap` until the next `make jenkins`, and nothing reports the drift.

### The fifth ceiling is the controller, and it is the one that costs money to raise

The four above are free to raise.  The controller runs all 730 hours of a month whether a build runs or not, so raising it is a standing charge.

`controller-fit.py` sizes it and `make quota` runs it.  Two drivers, on different axes: memory follows the agent *count*, and CPU follows the pod *churn rate*.  So `idleMinutes` in `jenkins-deployment.yaml` is a controller-sizing knob, and it is the cheapest of the three ways past a controller that does not fit; the other two are lowering the pools' maxima and buying a bigger machine.

Neither `jenkins-deployment.yaml` nor `2-platform/jenkins-gke-overrides.yaml` commits a `controller.resources` block, and that is deliberate: the right figure is a function of the agent count, the agent count is what one project's quotas allow, and `jenkins-deployment.yaml` is read by both clouds, so a limit raised there demands a bigger node at every site.  `make quota` says when to add one and what to raise alongside it.

### Every node pool is one zone

A pool is one node pool per zone: `agents-large-a`, `agents-large-b`, `agents-large-c`.  Four sizes over three zones is twelve agent pools, and each one's maximum is that zone's share of the pool's total, divided exactly with the remainder going to the earliest zones.

GKE can do this in one pool.  `autoscaling.total_max_node_count` expresses a pool-wide ceiling directly and GKE spreads the nodes itself, so unlike the sibling this is a choice rather than a necessity: EKS needs one node group per zone because a managed node group given several subnets spreads its Auto Scaling group over all of them and then nothing can say where the next node will land.  It is kept because a zone that cannot supply capacity then fails only its own pool, and because `agents-large-a` says in its name where a node is, which is what an operator reading `kubectl get nodes` most often wants and cannot otherwise get.

What it costs: the shares are re-divided whenever `zone_count` changes, so adding a zone lowers every existing pool's maximum, and GKE rejects a maximum below the nodes a pool is running.  Change the zone count between builds.

The controller is one pool in one zone, because its 500Gi disk binds to the zone it was first attached in.

### Node pool names are an interface

`.build/run-ci` attributes a pool to an agent size by finding the size word delimited by `-` or `_`, and skips the controller by matching the literal `jenkins-controller`.  So the names are `jenkins-controller` and `agents-<size>-<zone letter>`, the `agent_pools` keys are validated as single lower-case words, and renaming either breaks an attribution silently rather than loudly.

### A public name, and what TLS costs here

Three values together: `enable_external_dns`, `jenkins_hostname`, `dns_managed_zone`.  Leave all three unset and Jenkins answers at its load balancer's address, which is what a throwaway cluster wants.

| Where | What |
|---|---|
| `1-cluster/tls.tf` | a DNS authorization, a managed certificate, and the record that proves the name is ours |
| external-dns, in layer 2 | the record that points the name at the load balancer |
| `Makefile` | checks the certificate is ACTIVE, and builds the values layer 2 deploys with |
| `2-platform/build-jenkins-values.py` | the values: the URL Jenkins reports, and the annotations |

**This is where GKE delivers less than the sibling, and it is not a detail.**  On EKS an ACM certificate attaches to a plain LoadBalancer Service through an annotation, so TLS works at layer 4 with no ingress controller and nothing else to create.  Google has no equivalent, and the path it does have is narrower than it first looks:

| | |
|---|---|
| a `Service` of type LoadBalancer | a passthrough Network Load Balancer.  It holds no certificate, and there is no GKE annotation equivalent to `aws-load-balancer-ssl-cert` |
| a GKE Ingress | an Application Load Balancer, and it **cannot use a Certificate Manager certificate at all**.  Google's Ingress documentation says to use the Gateway API for one.  An Ingress takes a `ManagedCertificate` or a pre-shared certificate instead |
| a Gateway | the only route to the map layer 1 creates, through `networking.gke.io/certmap` |

So layer 1 creates a genuine, issued certificate and the map to attach it with, and enables the Gateway API when a public name is set.  What it does not create is the Gateway and the HTTPRoute, because those are Kubernetes objects: layer 1 cannot create one without a kubernetes provider authenticating to a cluster it is midway through creating.  `2-platform/jenkins-gke-overrides.yaml` says where the annotation goes.

Until that Gateway exists, Jenkins answers on plain HTTP, which is what a cluster with no public name does anyway.  Saying that plainly is better than an annotation that looks like it works, and `make jenkins` prints it as the step remaining rather than silently generating values that do nothing.

The tempting alternative is a `ManagedCertificate` and an Ingress, which would work and would make layer 1's three certificate resources pointless.  It was not taken because the certificate would then be a Kubernetes object owned by whatever reinstalls Jenkins, and a certificate that is deleted and reissued on a redeploy is a certificate that is sometimes not there.

Plain HTTP stays open on port 80 in either case, and that is deliberate: `run-ci` writes `http://` into the build's source and appends `:<port>` only when the Service's first port is not 80, so moving to 443 alone would stop run-ci submitting builds rather than secure anything.

A DNS authorization rather than a compute managed certificate, because a compute managed certificate is validated by *serving* the name, which cannot happen until layer 2 has made the load balancer.  A DNS authorization is proved by a record layer 1 can write on its own, which keeps layer 1 able to finish without layer 2.  It also does not expire, where an ACM certificate is deleted after 72 hours pending validation.

### Identity is much smaller here

The sibling spends three files on IAM: a control plane role, a node role, four add-on roles bound through EKS Pod Identity, an IAM OIDC provider with a certificate thumbprint, and an IRSA trust policy for the autoscaler.  Of that, two things survive.

| Sibling | Here |
|---|---|
| control plane role | nothing.  GKE's control plane runs in a Google-managed project |
| node role | `google_service_account` plus `roles/container.nodeServiceAccount` |
| CNI role | nothing.  Pod addresses are alias IPs from the subnetwork, with no daemon and no credential |
| disk driver role | nothing.  A boolean in `addons_config` |
| kube-proxy, CoreDNS | nothing.  Control plane managed |
| OIDC provider, thumbprint, `tls` provider | `workload_identity_config` on the cluster |
| autoscaler role | nothing.  The autoscaler is part of the control plane |
| external-dns role | a service account and one Workload Identity binding |

The node service account is created rather than defaulted.  The Compute Engine default service account holds Editor on the whole project, and every pod on every node would inherit it through the metadata server; `workload_metadata_config { mode = "GKE_METADATA" }` is what blocks that, and creating a narrow identity as well means the block is not the only thing standing between a build's test code and the project.

One thing Workload Identity cannot do that the sibling's trust policy can: pin a binding to one cluster.  Every cluster in a project shares the pool `<project>.svc.id.goog`, so a second cluster with the same namespace and service account name impersonates the same identity.  A project per cluster is the answer, which is the recommendation anyway.

### No account identifiers are committed

No project id, network name, address range, billing account or email address.  The runbook this directory replaces leaked four such things including its author's home address.  The same rule covers per-project *figures*: a pool's `max_size`, a controller `resources` block, an `instanceCap`.

`.gke-jenkins-values.yaml` is gitignored for exactly this reason, and its own header says so.

### Two things are off, against what the console offers

`enable_managed_prometheus` is false.  It is a collector on every node, agents included, and the large agent already asks for 7 of its node's 8 vCPU; the runbook this replaces recorded pods failing to schedule on CPU.  `WORKLOADS` is likewise absent from `logging_components`: it ships the stdout of every container on every node, which here is a Cassandra test run whose output Jenkins already archives.

## Layout

The cloud-neutral arithmetic lives in `../shared/`, so this directory and `../eks/` share it rather than each holding a copy: Kubernetes quantity parsing, Helm's merge rule, the Jenkins controller sizing model, and the spend windows with their least-squares fit.  `../shared/README.md` says what is deliberately not shared, and why.  All three modules are used here unchanged.

```
gke/
  Makefile                       the join between the layers, and the only place they meet
  README.md                      this
  NOTES.md                       which figures are measured, and which are not
  vcpu-quota.py                  how many nodes the project's quotas allow
  controller-fit.py              whether the controller can hold that many agents
  spend-caps.py                  asks what this may spend, and writes spend-caps.auto.tfvars
  spend-guard.py                 compares spend against the caps, and brakes; runs as a Cloud Function
  *-test.sh                      offline regression suites, one per script
  1-cluster/
    versions.tf                  providers, the version floor, and the commented gcs backend
    variables.tf                 every input, with the consequence of changing it
    locals.tf                    the ceilings, and the pools scaled to the lowest of them
    data.tf                      what is read rather than created
    read-quotas.sh               Compute Engine quotas, which the provider has no data source for
    cluster.tf                   the cluster, its addons, and the managed autoscaler
    node-pools.tf                one pool for the controller, one per agent size per zone
    iam.tf                       the node identity, and external-dns
    tls.tf                       the certificate, and what it cannot yet be attached to
    spend-guard.tf               the function, its schedule, its secrets, and what watches it
    outputs.tf                   the whole interface to layers 2 and 3
    terraform.tfvars.example     copy to terraform.tfvars
  2-platform/
    helmfile.yaml                external-dns, and nothing else GKE already runs
    storageclass-pd-ssd.yaml     the class the controller's 500Gi claim names
    jenkins-gke-overrides.yaml   the GKE half of the Jenkins values
    build-jenkins-values.py      the project's half, generated because it names the project
  3-smoke/
    smoke-test.sh                assertions against a deployed cluster, and a real build under --build
    check-pool-fit.py            whether one agent fits one node of its pool
```

`make test` runs every `*-test.sh`, mocked provisioning tests, `tofu fmt -check` and `tofu validate`, needs no credentials and no cluster, and is what `.github/workflows/jenkins-check.yaml` runs on every change under `.jenkins/`.  Variable tests evaluate the production variable declarations with OpenTofu in a temporary directory without providers or operator tfvars.  Provisioning tests use provider schemas with mocked resources and data sources.  Cloud CLI calls in other suites are stubbed.

## Not goals

**Autopilot.**  It decides node shapes, and every ceiling here is computed from pools that were declared.  It also bills per pod resource request, which for a 300-cell parallel is a different cost model than this is written against.

**Self-managing the cluster autoscaler.**  It would restore the `cluster-autoscaler-status` configmap that `.build/run-ci` reads, which is a real loss to make good.  It is a not goal because the price is disabling GKE's own autoscaling and then owning an autoscaler's version, its IAM and its scale-down tuning, to recover one check that `make quota` and `make smoke` already cover between them.

**Node auto-provisioning.**  Same reason as Autopilot: it invents node pools with machine types nobody chose.

**Anything outside this directory.**  Four values in `../jenkins-deployment.yaml` decide whether this cluster works at all: `agent.waitForPodSec`, each template's `slaveConnectTimeout` and `instanceCap`, and `idleMinutes`.  They are already correct there, and they are shared with the sibling, so changing one is a change to both clouds.
