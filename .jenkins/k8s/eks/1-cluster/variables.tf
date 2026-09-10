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

variable "cluster_name" {
  description = <<-EOT
    Name of the EKS cluster.  Also the value of the `k8s.io/cluster-autoscaler/<cluster>` ASG tag the
    autoscaler discovers node groups by, so changing it after the fact replaces both.

    The convention in .jenkins/k8s/README.md is a name identifiable to you, `<whoami>--cassandra-jenkins`.
    The default here is the bare `cassandra-jenkins` so that `tofu plan` runs with no tfvars file at all;
    set it in terraform.tfvars for anything that is not a throwaway.
  EOT
  type        = string
  default     = "cassandra-jenkins"

  validation {
    # EKS: 1-100 characters, alphanumeric, hyphen and underscore, starting alphanumeric.
    condition     = can(regex("^[0-9A-Za-z][A-Za-z0-9_-]{0,99}$", var.cluster_name))
    error_message = "cluster_name must start alphanumeric and contain only letters, digits, '-' and '_'."
  }
}

variable "region" {
  description = <<-EOT
    AWS region for the cluster and every node group.

    The GKE half of .jenkins/k8s/README.md asks for your closest low-carbon zone.  AWS publishes the
    same information per region under its carbon methodology pages rather than in the API, so the
    choice is yours to make; it is not derivable here.
  EOT
  type        = string
  default     = "eu-north-1"
}

variable "kubernetes_version" {
  description = <<-EOT
    Kubernetes minor version for the control plane, as `1.34`.  Leave null to take the newest version
    EKS reports in STANDARD_SUPPORT.

    The consequence of null: a later `tofu plan` will propose a control-plane upgrade on the day EKS
    promotes a newer version into standard support, with no change to this configuration.  The upgrade
    is visible in the plan rather than silent, but it is still a plan whose content moved on its own.
    Pin this variable if that is not acceptable.

    The consequence of a pin: EKS refuses to skip a minor version.  A pin left stale for two releases
    has to be stepped one minor at a time, applying each step, rather than jumped in one apply.
  EOT
  type        = string
  default     = null

  validation {
    condition     = var.kubernetes_version == null || can(regex("^1\\.[0-9]+$", var.kubernetes_version))
    error_message = "kubernetes_version must be a minor version such as \"1.34\", or null to derive it."
  }
}

variable "bootstrap_self_managed_addons" {
  description = <<-EOT
    Let EKS install vpc-cni, kube-proxy and CoreDNS itself at cluster creation.  False, and a new cluster
    should leave it false.

    What true costs: the three arrive as plain manifests rather than managed add-ons, so `aws eks
    describe-addon` cannot see them, nothing reports their version and nothing upgrades them.  addons.tf then
    creates the managed add-ons over those copies with resolve_conflicts_on_create = "NONE", which refuses any
    field the bootstrapped copy already holds; the vpc-cni address targets are always such a field, so the
    apply stops there.

    Set it true only for a cluster that was created with those copies already in place.  Changing this
    argument on a live cluster replaces the cluster, so a plan that proposes flipping it is proposing to
    destroy and rebuild the control plane, the node groups and the controller's volume.
  EOT
  type        = bool
  default     = false
}

variable "vpc_id" {
  description = <<-EOT
    VPC to place the cluster in.  Leave null to use the account's default VPC in var.region.

    A default VPC is the right answer for a throwaway CI cluster and the wrong answer for a long-lived
    one: its subnets are public, and the nodes get public addresses.
  EOT
  type        = string
  default     = null
}

variable "subnet_ids" {
  description = <<-EOT
    Subnets for the control plane's cross-account interfaces and for every node group.  Leave null to
    use every subnet of var.vpc_id that lies in the first var.availability_zone_count availability
    zones.
  EOT
  type        = list(string)
  default     = null

  validation {
    condition     = var.subnet_ids == null || length(var.subnet_ids) >= 2
    error_message = "EKS requires subnets in at least two availability zones, so pass at least two."
  }
}

variable "availability_zone_count" {
  description = <<-EOT
    How many availability zones to spread across when var.subnet_ids is null.

    Null, the default, means every zone the region offers, which is four in us-west-2.  Set a number to use
    fewer; two is the EKS minimum.  The zones are taken in sorted order, so the set a number selects is
    stable across plans.

    Zones buy addresses, and addresses are what the pools are now sized against.  Each zone of a default VPC
    holds one /20, which is 4091 usable addresses: two zones is 8182 and four is 16364.  var.agent_pools is
    scaled by the lowest of three ceilings and the subnets are one of them, so a region's third and fourth
    zone raise the node count this cluster may hold rather than sitting unused.  Build cassandra-eks-k8s #8
    spent every address two zones had, at 314 nodes.

    What it costs is cross-zone data transfer on every agent-to-controller connection, and it buys nothing in
    throughput per node: an agent pod is a single build on a single node, and the controller is a single
    instance in one zone whatever this is set to.  Every zone also adds one node group per agent size, so
    four zones is sixteen agent groups rather than eight; the pools' maxima are divided between them, not
    repeated in each.

    Change it between builds, not during one.  Each pool's max_size is divided by the zone count, so adding
    a zone drops every existing group's maximum, and a managed node group rejects a maximum below the nodes
    it is running.  Adding zones adds node groups and does not replace the existing ones, which keep the
    subnets they were created with.
  EOT
  type        = number
  default     = null

  validation {
    condition     = var.availability_zone_count == null || var.availability_zone_count >= 2
    error_message = "EKS requires at least two availability zones, so pass null for all of them or a number of at least two."
  }
}

variable "public_access_cidrs" {
  description = <<-EOT
    CIDR blocks allowed to reach the public Kubernetes API endpoint.  Leave null to accept the EKS
    default, which is everywhere.

    Narrow this to the addresses that actually run `kubectl` and `.build/run-ci`.  It is left null
    rather than defaulted to a literal because no address that belongs to a person or an office
    belongs in this repository.
  EOT
  type        = list(string)
  default     = null
}

variable "control_plane_log_types" {
  description = <<-EOT
    Control plane log streams to send to CloudWatch Logs.

    All five are on by default.  `authenticator` and `audit` are the two that answer "who did this to
    the cluster", which is the question asked after an unexplained change, and neither can be turned on
    retrospectively for an event that already happened.
  EOT
  type        = list(string)
  default     = ["api", "audit", "authenticator", "controllerManager", "scheduler"]

  validation {
    condition = length(setsubtract(var.control_plane_log_types,
    ["api", "audit", "authenticator", "controllerManager", "scheduler"])) == 0
    error_message = "Valid log types are api, audit, authenticator, controllerManager and scheduler."
  }
}

variable "log_retention_days" {
  description = <<-EOT
    Retention on the control plane's CloudWatch log group.

    The log group is created here rather than left to EKS.  EKS creates it on first write with no
    expiry, and audit logs from a busy control plane are the largest recurring cost of a CI cluster
    that is otherwise idle.
  EOT
  type        = number
  default     = 30

  validation {
    condition = contains([0, 1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731,
    1096, 1827, 2192, 2557, 2922, 3288, 3653], var.log_retention_days)
    error_message = "log_retention_days must be one of the retention periods CloudWatch Logs accepts (0 keeps forever)."
  }
}

variable "controller_pool" {
  description = <<-EOT
    The node group the Jenkins controller runs on.  Nothing else is pinned to it by default except the
    cluster autoscaler, CoreDNS and the EBS CSI controller, all of which would otherwise land on an
    agent node that scales away underneath them.

    ON_DEMAND: this node holds the build queue, the 500Gi jenkins_home and the autoscaler.  Losing it
    abandons every running build and strands their agent pods.  SPOT is accepted below, for a throwaway
    cluster whose builds nobody is waiting on.

    m7a.2xlarge is 8 vCPU / 32 GiB, matching the GKE e2-standard-8 in .jenkins/k8s/README.md.  The
    controller asks for 4 vCPU / 16G (decimal, so 14.9 GiB) in jenkins-deployment.yaml, and
    pre-ci.cassandra.apache.org runs it at 8 and 20G.
  EOT
  type = object({
    instance_types = optional(list(string), ["m7a.2xlarge"])
    capacity_type  = optional(string, "ON_DEMAND")
    disk_gib       = optional(number, 100)
    min_size       = optional(number, 1)
    max_size       = optional(number, 1)
    desired_size   = optional(number, 1)
  })
  default = {}

  validation {
    condition     = contains(["ON_DEMAND", "SPOT", "CAPACITY_BLOCK"], var.controller_pool.capacity_type)
    error_message = "capacity_type must be ON_DEMAND, SPOT or CAPACITY_BLOCK."
  }

  validation {
    condition = (var.controller_pool.min_size <= var.controller_pool.desired_size
    && var.controller_pool.desired_size <= var.controller_pool.max_size)
    error_message = "controller_pool needs min_size <= desired_size <= max_size."
  }

  validation {
    condition     = var.controller_pool.min_size >= 1
    error_message = "The controller pool cannot scale to zero: the Jenkins controller is a StatefulSet with a bound PVC."
  }
}

variable "agent_pools" {
  description = <<-EOT
    Agent node groups, keyed by the size word in the `cassandra.jenkins.agent.<size>` node label.  Each
    key becomes a node group named `agents-<key>` and a node label
    `cassandra.jenkins.agent.<key> = "true"`, which is what ties it to one agent podTemplate in
    jenkins-deployment.yaml.

    max_size mirrors each podTemplate's instanceCap, because the podAntiAffinity in every template puts
    one agent on a node.  An instanceCap above the pool's maximum does not idle: the surplus pods drive
    the autoscaler to its ceiling, expire after waitForPodSec and are requested again in a loop.
    `.build/run-ci` refuses to deploy that combination; see check_agent_capacity there.

    ON_DEMAND for all four.  A run spends one agent per cell, and the tests stage is a 314-cell parallel
    with failFast, so the run is only as durable as its least durable cell.  Reclaiming a spot node drains
    it through the Kubernetes Eviction API.  The pipeline's `finally { cleanAgent() }` then runs a shell
    step on an agent that has already gone, which throws AgentOfflineException; `retry` does not retry
    that, and failFast discards every other branch.  Build cassandra #3 lost 314 branches to
    five such evictions.  SPOT is still accepted below, for a pool whose loss costs one cell.

    The instance types come from the GKE machine types in .jenkins/k8s/README.md, matched on vCPU and
    memory:

      pool        GKE              vCPU / RAM     EKS
      controller  e2-standard-8    8 / 32 GiB     m7a.2xlarge
      small       e2-highcpu-8     8 /  8 GiB     c7a.2xlarge   (16 GiB, the smallest 8 vCPU c7a)
      medium      n2-highcpu-8     8 /  8 GiB     c7a.2xlarge
      large       n2-standard-8    8 / 32 GiB     m7a.2xlarge
      report      -                -              m7a.2xlarge

    The large pool is the one that has to be a 32 GiB instance and not a 16 GiB one.  agent-dind-large
    requests 16G for dind and 1G for jnlp; those are decimal G, so 17G is 15.83 GiB, which is more than
    a 16 GiB node has left after the kubelet's reservations.  Run 3-smoke/check-pool-fit.py before
    changing any of these.

    The report pool has no GKE counterpart: agent-dind-report was added after these four were matched, so
    a site deploying jenkins-deployment.yaml elsewhere points that template at a pool it already has.  It
    is a 32 GiB instance because generateTestReports runs several ant junitreport jvms at once and the
    template's dind container is given a 9G limit for them; it is 4 nodes because one build merges its
    reports once, at the end.

    disk_gib is 100 because each podTemplate budgets 80Gi of ephemeral storage, of the roughly 89Gi a
    100 GiB volume allocates.  The GKE side spells the same number as `--disk-size=107`, which is
    107 GB, that is 99.6 GiB.

    The max_size figures are measured, not matched to GKE, and against the demand of the pipeline profiles
    rather than one build's peak.  A profile is a list of test targets, each a fixed number of splits at a
    fixed agent size, so a profile's concurrent demand is arithmetic.  Counted from build cassandra-eks-k8s
    #11's archived stage logs, one per cell:

      profile                       small    medium     large   total cells
      skinny                            0       117       194           311
      pre-commit                       18       189       398           605
      post-commit less its upgrades     18       441       644         1103

    Against those, with the figures below scaled to this account's ceiling of 1122 agent nodes:

      pool    demand it must cover  declared max_size  scaled to this account
      small                     18                 10                      23
      medium                   441                190                     444
      large                    644                276                     646
      report            1 per build                  4                       9

    Three things fix those numbers.  The four must sum to the ceiling, because that sum is what reserves
    report a slot: an instanceCap is a ceiling and not a reservation, so only the other three caps, stopping
    at 1113, keep them off report's nine.  small is 18 because its demand is the build steps, stable at three
    cells each for artifacts, debian, redhat, fqltool-test, sstableloader-test and stress-test.  medium and
    large take what is left, in the ratio of the widest profile that fits at all.

    skinny and pre-commit are the typical profiles and both fit entirely, with 3.8x and 2.3x headroom on
    medium, so the ratio binds for neither.  It binds only above the ceiling, which is why it follows the
    widest profile that still fits.  post-commit cannot fit at any ratio: its four upgrade targets are 1200
    large cells alone.

    These are the declared figures, not what the node groups are created with.  var.size_pools_to_quotas
    scales all four by the lowest ceiling the account allows, so this table fixes the ratio and the absolute
    sizes follow the account.  `tofu output agent_node_ceiling` prints which ceiling bound; `make quota` adds
    whether the controller can hold the result.
  EOT
  type = map(object({
    instance_types = list(string)
    capacity_type  = optional(string, "ON_DEMAND")
    disk_gib       = optional(number, 100)
    min_size       = optional(number, 0)
    max_size       = number
    desired_size   = optional(number, 0)
  }))

  default = {
    small  = { instance_types = ["c7a.2xlarge"], max_size = 10 }
    medium = { instance_types = ["c7a.2xlarge"], max_size = 190 }
    large  = { instance_types = ["m7a.2xlarge"], max_size = 276 }
    report = { instance_types = ["m7a.2xlarge"], max_size = 4 }
  }

  validation {
    # `.build/run-ci` attributes an autoscaler node group to an agent size by looking for the size word
    # delimited by '-' or '_' in the group's name.  A key containing either delimiter would make
    # `agents-<key>` ambiguous, and a key that is a substring of another would match two groups.
    condition     = alltrue([for size in keys(var.agent_pools) : can(regex("^[a-z0-9]+$", size))])
    error_message = "agent_pools keys must be single lower-case words: '-' and '_' are how .build/run-ci splits a node group name."
  }

  validation {
    condition = alltrue([for pool in values(var.agent_pools) :
    pool.min_size <= pool.desired_size && pool.desired_size <= pool.max_size])
    error_message = "Each agent pool needs min_size <= desired_size <= max_size."
  }

  validation {
    condition     = alltrue([for pool in values(var.agent_pools) : contains(["ON_DEMAND", "SPOT"], pool.capacity_type)])
    error_message = "Agent pool capacity_type must be ON_DEMAND or SPOT."
  }

  validation {
    # locals.tf takes the largest of a pool's instance types to size it against the vCPU quota, and max() of
    # nothing is an error rather than a message about a pool.
    condition     = alltrue([for pool in values(var.agent_pools) : length(pool.instance_types) > 0])
    error_message = "Each agent pool needs at least one instance type."
  }
}

variable "size_pools_to_quotas" {
  description = <<-EOT
    Read the account's vCPU and EBS quotas at apply, and scale every agent pool's max_size by the lowest
    ceiling they and the subnets allow.

    The problem this solves is that the max_size figures in agent_pools are one account's answer.  Applied
    to a smaller account they produce node groups whose launches fail with `VcpuLimitExceeded`; applied
    after a quota increase they leave the extra capacity unused until somebody edits this file.  With this
    on, a quota increase needs no edit and a smaller account gets a smaller cluster that works.

    One scale factor is applied to every pool, so the ratios between the pools stay as written.  Above 1 it
    grows them, below 1 it shrinks them, and no pool ever falls below one node.  `tofu output
    agent_node_ceiling` prints the three ceilings, which one bound, and the factor.

    Turn it off when the credentials running the apply do not carry `servicequotas:GetServiceQuota`, and
    the declared max_size figures are then used as written.  The subnet address ceiling is still applied,
    because the subnets are already read for the node groups; that one can only reduce the pools, never
    grow them.

    Read the three ceilings with:

        aws service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A --query 'Quota.Value'
        aws service-quotas get-service-quota --service-code ebs --quota-code L-7A658B76 --query 'Quota.Value'
        aws service-quotas get-service-quota --service-code ebs --quota-code L-D18FCD1D --query 'Quota.Value'
  EOT
  type        = bool
  default     = true

  validation {
    # A quota that cannot hold the controller and one agent is not a cluster, and scaling to it would produce
    # node groups whose every launch fails.  Caught here rather than at apply, where the message would name a
    # node group instead of the quota.
    condition     = !var.size_pools_to_quotas || var.quota_headroom > 0
    error_message = "size_pools_to_quotas needs quota_headroom above 0."
  }
}

variable "control_plane_zone_count" {
  description = <<-EOT
    How many availability zones the cluster's own subnets span.  Two is the EKS minimum and the default.

    This is not var.availability_zone_count, and the two are separate because EKS treats them differently.
    The control plane's zone set is fixed at creation and cannot be changed afterwards:

        InvalidParameterException: Provided subnets belong to the AZs 'us-west-2a,us-west-2b,us-west-2c,
        us-west-2d'. But they should belong to the exact set of AZs 'us-west-2a,us-west-2b' in which subnets
        were provided during cluster creation.

    `UpdateClusterConfig` accepts more subnets inside the same zones and refuses a different set of zones.  A
    managed node group's subnets, on the other hand, do not have to be the cluster's at all: they have to be
    in the same VPC and able to reach the API endpoint, which every subnet of one VPC is.  So the node groups
    follow var.availability_zone_count and can gain a zone on a running cluster, and the control plane keeps
    the zones it was created with.

    **Changing this on an existing cluster needs a new cluster.**  Nothing here can detect that: OpenTofu
    compares this value against the state, not against what EKS will accept, so a plan will offer an
    in-place update that the apply then fails.  Raise var.availability_zone_count instead.
  EOT
  type        = number
  default     = 2

  validation {
    condition     = var.control_plane_zone_count >= 2
    error_message = "EKS requires the cluster's subnets to span at least two availability zones."
  }
}

variable "quota_headroom" {
  description = <<-EOT
    The share of each account quota that var.size_pools_to_quotas may claim for these pools.

    A quota is the account's whole limit for a region, not what is unused, and nothing in this directory can
    see what else spends it: a second cluster, an unrelated instance, a stray volume, or the controller's own
    500Gi `jenkins_home` claim, which layer 2 creates from a storage class named nowhere here.  Claiming the
    whole quota puts 510 root volumes of 100 GiB against a 50 TiB quota, which is 51000 GiB of 51200 and
    leaves two nodes of room for everything else in the region.

    0.9 keeps a tenth back, which on a 50 TiB quota is 5 TiB.  Raise it towards 1 for an account this cluster
    has to itself, and lower it for an account it shares.  It applies to the vCPU and EBS quotas, and not to
    the subnet address ceiling, which is measured from the subnets themselves and not from a limit.
  EOT
  type        = number
  default     = 0.9

  validation {
    condition     = var.quota_headroom > 0 && var.quota_headroom <= 1
    error_message = "quota_headroom is a share of a quota, so it must be above 0 and at most 1."
  }
}

variable "node_auto_repair" {
  description = <<-EOT
    Let EKS replace or reboot a node it finds unhealthy, and install the node monitoring agent add-on
    that widens what "unhealthy" covers.

    Without the agent, auto repair reacts only to the kubelet's `Ready` condition, a manually deleted
    node object, and an instance that fails to join.  With it, `ContainerRuntimeReady`, `KernelReady`,
    `NetworkingReady` and `StorageReady` also trigger a repair.  The agent is a DaemonSet, so it costs a
    small slice of every agent node; unlike CloudWatch Observability it is a slice, not a percentage.

    EKS stops repairing on its own once more than 20% of a node group of over five nodes is unhealthy,
    so this cannot turn a bad AMI into a replacement loop across a 306-node pool.
  EOT
  type        = bool
  default     = true
}

variable "enable_cloudwatch_observability" {
  description = <<-EOT
    Install the amazon-cloudwatch-observability add-on.  Off, against what the console offers.

    It is a DaemonSet on every node, agents included, and the large agent already requests 7 of the
    node's 8 vCPU.  The runbook this directory replaces recorded pods failing to schedule on CPU; an
    agent node that cannot fit the observability agent alongside its build is that same problem.

    Turn it on for the controller's node only by pinning it, not cluster-wide, if you want it at all.
  EOT
  type        = bool
  default     = false
}

variable "enable_external_dns" {
  description = <<-EOT
    Install the external-dns community add-on and give it a Route 53 role.  Off, against what the
    console offers.

    external-dns manages records in Route 53 hosted zones in this account.  It has nothing to manage
    unless the CI hostname is in such a zone, and pre-ci.cassandra.apache.org is not: that name is
    delegated in an ASF zone, and a record for it cannot be created from here at all.  See the DNS
    section of ../README.md.
  EOT
  type        = bool
  default     = false
}

variable "external_dns_hosted_zone_ids" {
  description = <<-EOT
    Further hosted zones external-dns may write to, beyond var.dns_hosted_zone_id.  AWS documents the
    add-on's managed policy as AmazonRoute53FullAccess, which is every zone in the account, and a CI
    cluster has no business holding that; this list and dns_hosted_zone_id are what it gets instead.

    Setting dns_hosted_zone_id is enough for the ordinary case.  This list is for a cluster that serves
    a name in one zone and must write records in another.
  EOT
  type        = list(string)
  default     = []
}

variable "jenkins_hostname" {
  description = <<-EOT
    Public name Jenkins serves on, such as `ci.example.org`.  Empty leaves the cluster reachable only
    at the load balancer's own name, which is what a throwaway cluster wants.

    Setting it does three things.  It requests an ACM certificate for the name, validated by records
    this configuration writes into var.dns_hosted_zone_id.  It puts the name and the certificate into
    the environment ../Makefile builds, from which layer 2 annotates the Jenkins Service, so the load
    balancer terminates TLS and external-dns creates the record.  And it makes the name the URL Jenkins
    reports as its own.

    The name must be inside the zone named by dns_hosted_zone_id, and that zone must be delegated from
    whoever holds the parent.  Delegation is the one part of this that cannot be scripted; see
    ../README.md.  Until it is done the certificate stays PENDING_VALIDATION, and `make jenkins` keeps
    Jenkins on plain HTTP rather than pointing the load balancer at a certificate that does not exist.
  EOT
  type        = string
  default     = ""

  validation {
    # A hostname, not a URL and not a wildcard.  ACM accepts a wildcard, and every other consumer of
    # this value does not: a Service annotation, a certificate for one name, and a Jenkins URL.
    condition     = var.jenkins_hostname == "" || can(regex("^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$", var.jenkins_hostname))
    error_message = "jenkins_hostname must be a bare lower-case hostname with at least one dot, and no scheme, port, path, or wildcard."
  }
}

variable "dns_hosted_zone_id" {
  description = <<-EOT
    Route 53 public hosted zone holding var.jenkins_hostname.  Required when that is set.

    The zone is not created here, and deliberately.  Creating it would make its identifier an output of
    the same configuration that needs it as an input, and the zone outlives any one cluster: it is
    account setup, like the account itself.  Create it once, and delegate it:

      aws route53 create-hosted-zone --name example.org --caller-reference example-org-1

    That prints the four nameservers to set at the registrar.
  EOT
  type        = string
  default     = ""

  validation {
    # The bare identifier, as `Z0123456789ABCDEFGHIJ`, and not the `/hostedzone/Z...` form the API also
    # accepts: this value is interpolated into the ARNs in iam-addons.tf and into the `export` lines of the
    # environment output, and a path or a quote in it would corrupt both.
    condition     = var.dns_hosted_zone_id == "" || can(regex("^[A-Z0-9]{8,32}$", var.dns_hosted_zone_id))
    error_message = "dns_hosted_zone_id must be a bare hosted zone identifier such as Z0123456789ABCDEFGHIJ, with no /hostedzone/ prefix."
  }
}

variable "spend_cap_daily_usd" {
  description = <<-EOT
    What this account may spend in a UTC day, in US dollars, before the agent pools are stopped.  Null is no
    daily cap.

    Set these with `make caps`, which asks for all three and writes spend-caps.auto.tfvars.  ../spend-caps.py
    prints the arithmetic that makes a number choosable: what the cluster costs standing still, what the
    pools cost an hour at full size, and how many hours of that each answer holds.

    What happens when it is met: ../spend-guard.py suspends the `Launch` process on every agent Auto Scaling
    group.  No new agent node starts, every running agent finishes untouched, and the autoscaler removes each
    node as it goes idle, so the pools drain to zero.  Jenkins keeps queueing tasks that cannot be given a
    node, which is a stopped cluster; `make spend` and 3-smoke/smoke-test.sh both name the brake, so it is
    not diagnosed as a fault in the autoscaler.

    The cap is on the whole account's bill in this region, not on what is tagged for this cluster.  That is
    how this directory already treats the vCPU quota, and it means anything else in the account spends from
    the same cap.  Run this cluster in an account of its own.

    All three caps are compared against an estimate, because AWS publishes no low-latency spend feed; see the
    docstring of ../spend-guard.py for how the estimate is made and what its error margin is.
  EOT
  type        = number
  default     = null

  validation {
    condition     = var.spend_cap_daily_usd == null || var.spend_cap_daily_usd > 0
    error_message = "spend_cap_daily_usd must be above 0, or null for no daily cap."
  }
}

variable "spend_cap_weekly_usd" {
  description = <<-EOT
    What this account may spend in a week, in US dollars, before the agent pools are stopped.  Null is no
    weekly cap.

    The week is ISO 8601's: it begins on Monday at 00:00 UTC.  AWS Budgets has no weekly period at all, so
    this window exists only in ../spend-guard.py and nothing in the AWS console reports it.

    See var.spend_cap_daily_usd for what being met does, and for what the figure is measured against.
  EOT
  type        = number
  default     = null

  validation {
    condition     = var.spend_cap_weekly_usd == null || var.spend_cap_weekly_usd > 0
    error_message = "spend_cap_weekly_usd must be above 0, or null for no weekly cap."
  }
}

variable "spend_cap_monthly_usd" {
  description = <<-EOT
    What this account may spend in a calendar month, in US dollars, before the agent pools are stopped.  Null
    is no monthly cap.

    This is the one to set against the floor rather than against a build.  The controller is one instance
    running all 730 hours of a month whether a build runs or not, the EKS control plane is charged by the
    hour beside it, and the controller's 500Gi volume and the load balancer are charged whatever happens.  A
    monthly cap under that floor is met on the first of the month with no build having run, and then the
    pools are braked for the rest of it.  `make caps` refuses such a cap; a figure set by hand here is not
    checked, because nothing in this configuration knows what an instance costs.
  EOT
  type        = number
  default     = null

  validation {
    condition     = var.spend_cap_monthly_usd == null || var.spend_cap_monthly_usd > 0
    error_message = "spend_cap_monthly_usd must be above 0, or null for no monthly cap."
  }
}

variable "spend_alert_email" {
  description = <<-EOT
    Address to email when a cap is met, when the pools are released again, or when the guard cannot read what
    has been spent.  Empty sends to nobody, and the same messages still reach the SNS topic and CloudWatch.

    An email subscription is pending until the address confirms it by following a link, which no apply can
    do.  Nothing fails while it is pending.
  EOT
  type        = string
  default     = ""

  validation {
    condition     = var.spend_alert_email == "" || can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", var.spend_alert_email))
    error_message = "spend_alert_email must be an email address, or empty."
  }
}

variable "spend_guard_interval_minutes" {
  description = <<-EOT
    How often the guard compares spend against the caps.

    This interval is what the overshoot is measured in.  The pools at full size are a few hundred dollars an
    hour, so a cap can be passed by up to an interval's worth of spend before anything acts, and every
    evaluation costs a fraction of a cent.  Five minutes is the default for that reason and not for AWS's.

    Two is the floor here rather than one, because `rate(1 minute)` is spelled in the singular and this is
    interpolated into a `rate(N minutes)` expression.
  EOT
  type        = number
  default     = 5

  validation {
    condition     = var.spend_guard_interval_minutes >= 2 && var.spend_guard_interval_minutes <= 60
    error_message = "spend_guard_interval_minutes must be between 2 and 60."
  }
}

variable "spend_price_per_vcpu_hour" {
  description = <<-EOT
    What a vCPU-hour costs, in US dollars, used only until the guard can fit the figure itself.

    ../spend-guard.py fits both this and var.spend_fixed_usd_per_day from Cost Explorer over the settled days
    of the last fortnight, so on a cluster whose settled days carry real load these two are unused.  They are
    what it falls back to on a new cluster, on an account where Cost Explorer is not enabled, and when the days
    it has cannot answer: an idle or braked fortnight differs by a few vCPU-hours, which fits a price near
    zero, and a price near zero estimates a full-size build at nothing at all.  `make spend` says which of the
    two is in force, and prints the figures a refused fit produced.

    It is not a list price, and treating it as one is what broke this once.  What the fit produces, and what
    this stands in for, is dollars of bill per vCPU-hour the metric reports on this account, so a Savings Plan,
    a discount held in the organisation's management account and any scaling between the metric and reality are
    all inside it.  The account this runs in bills $0.0010 a vCPU-hour where the on-demand list price is
    $0.058.

    0.06 is therefore a list-price snapshot for a brand new account and nothing more: an m7a.2xlarge is 8 vCPU
    at about $0.46 an hour in us-west-2.  It is deliberately not read from the Pricing API, which is a fifth
    AWS service to grant and a figure the fit replaces within three days.  From the third settled day the
    guard scales this figure so that its total over those days matches the bill, so a wrong one costs
    accuracy for two days rather than a braked cluster; `make spend` says when it has been scaled and how.
  EOT
  type        = number
  default     = 0.06

  validation {
    condition     = var.spend_price_per_vcpu_hour > 0
    error_message = "spend_price_per_vcpu_hour must be above 0."
  }
}

variable "spend_fixed_usd_per_day" {
  description = <<-EOT
    What this cluster costs a day with no instance running at all, in US dollars, used only until the guard
    can fit the figure itself.

    The EKS control plane at $0.10 an hour, the load balancer, the controller's 500Gi volume and the control
    plane's CloudWatch logs.  The controller's own instance is not in it: that one is vCPU like any other and
    is measured as vCPU.  See var.spend_price_per_vcpu_hour for when either figure is used.

    5 is a round number and not a measurement.  The account this was written against fitted $3.27 a day over
    12 idle days, so set this from what `make spend` reports for your own cluster rather than leaving the
    default: it is added to every day of every window, so 5 against a real 3.27 is $52 of phantom spend a
    month.
  EOT
  type        = number
  default     = 5

  validation {
    condition     = var.spend_fixed_usd_per_day >= 0
    error_message = "spend_fixed_usd_per_day cannot be negative."
  }
}

variable "spend_cost_metric" {
  description = <<-EOT
    Which Cost Explorer metric the caps are compared against.

    UnblendedCost is what the account is charged before credits are applied, and it is the default because it
    brakes earlier: an account running on credits still has a bill, and credits run out.  NetUnblendedCost
    nets the credits out, so on a sponsored account it reads near zero and no cap is ever met, which is right
    if the question is what is being paid and wrong if the question is what is being consumed.

    Change it deliberately, and know which question the caps then answer.
  EOT
  type        = string
  default     = "UnblendedCost"

  validation {
    condition = contains(["UnblendedCost", "NetUnblendedCost", "AmortizedCost", "NetAmortizedCost",
    "BlendedCost"], var.spend_cost_metric)
    error_message = "spend_cost_metric must be one of the cost metrics GetCostAndUsage publishes."
  }
}

variable "spend_cost_refresh_hours" {
  description = <<-EOT
    How often the guard calls Cost Explorer.

    Each call costs a cent, so at the five-minute evaluation interval an unthrottled guard would spend about
    $86 a month asking what it had spent.  Six hours is four calls a day, and the figures between them come
    from the estimate, which is what carries the current day anyway.  The kept figures are refreshed early whenever a
    settled day is missing from it, so a cluster that was switched off does not wait out the interval.
  EOT
  type        = number
  default     = 6

  validation {
    condition     = var.spend_cost_refresh_hours >= 1 && var.spend_cost_refresh_hours <= 24
    error_message = "spend_cost_refresh_hours must be between 1 and 24."
  }
}

variable "enable_spend_budgets" {
  description = <<-EOT
    Create an AWS Budget for the daily and monthly caps, alongside the guard.

    They enforce nothing.  A budget is where whoever pays the bill looks, and it is a second alert path that
    shares no code with the guard: AWS's own reading of AWS's own billing data, so the two disagreeing is
    itself worth knowing.

    Two budgets, and no more, because the first two in an account are free and each one after that is charged
    per day.  There is no weekly budget because AWS Budgets has no weekly period.
  EOT
  type        = bool
  default     = true
}

variable "tags" {
  description = "Tags applied to every resource created here, through the provider's default_tags."
  type        = map(string)
  default = {
    Project   = "apache-cassandra-ci"
    ManagedBy = "opentofu"
  }
}
