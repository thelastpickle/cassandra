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

# Verification and sizing evidence

Use [README.md](README.md) for deployment and troubleshooting. This file records the boundary between observed behavior and estimates.

## Deployment observations

Operator-supplied September 15, 2026 output from a Paris cluster showed a running Jenkins controller and small agent, followed by `29 passed, 0 failed` from smoke checks. Those checks predate the September 17 review fixes and do not verify this revision.

The original 107 GiB COS boot disk reported `106057252Ki` capacity and `51571464850` bytes allocatable ephemeral storage. This could not fit a 50 GiB agent request. After increasing agent disks to 200 GiB, the small node reported about 97 GiB available after DaemonSet requests; the conservative offline estimate was 86 GiB.

## Offline verification

The September 17 Linux CI run at `9d21de2cf0` passed the EKS test target and GKE provider validation, 25 variable tests and seven pool-sizing tests. The GKE provisioning suite then failed one of nine tests because mock build and runtime service accounts received identical computed names. Later GKE script suites did not run in that workflow.

The revised fixture assigns distinct resource names and preserves the account-separation assertion. New regression tests cover quota membership, pool floors, runtime REST calls, stale metrics, state retention, safe caps-file writes, context selection, NAT coverage and resource fit. Cloud responses are mocked, and provider-free HCL tests exercise arithmetic; neither establishes deployed behavior.

Local provider-backed tests stop before execution at the provider handshake (`Failed to read any lines from plugin's stdout`), including outside the sandbox. The changed provisioning and alert configuration therefore requires another Linux CI run.

## Model provenance

The agent proportions and controller load model come from EKS builds recorded in [the sibling's notes](../eks/NOTES.md). The controller model describes Jenkins remoting and pod churn, but has not been calibrated against sustained GKE load. The EKS cold-node observation is not a GKE startup measurement.

GKE memory and CPU reservations follow [Google's node sizing guidance](https://cloud.google.com/kubernetes-engine/docs/concepts/plan-node-sizes). The 255 MiB memory reservation applies only below 1 GiB; larger nodes use percentage tiers plus the eviction threshold. Live agent checks subtract DaemonSet requests from node allocatable values. Offline storage estimates include a conservative COS filesystem allowance.

Quota membership follows [Google's allocation quota documentation](https://cloud.google.com/compute/resource-usage). The scripts model flat CPU and disk-capacity quotas, not dimensioned CPU quotas or Hyperdisk performance quotas. They report missing applicable quotas. The spend model remains an estimate until a project-scoped billing export supplies enough settled, varied observations to fit it.

## Remaining deployment checks

- Invoke the deployed REST client with its service account and check secret access, Monitoring, node-pool updates and optional BigQuery access.
- Verify alert delivery before any heartbeat, on unknown spend or braking, and on recovery.
- Confirm GKE accepts a zero autoscaling maximum and that running builds remain unaffected. The guard falls back to one if zero is refused, which still permits capacity.
- Observe concurrent pool updates, cold scaling and controller load during a real build.
- Verify end-to-end egress and any custom Gateway/TLS configuration. NAT configuration checks and certificate creation alone do not establish either.
