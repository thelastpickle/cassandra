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

locals {

  # ---------------------------------------------------------------------------------------------------
  # Kubernetes version
  # ---------------------------------------------------------------------------------------------------

  standard_versions = data.aws_eks_cluster_versions.standard.cluster_versions[*].cluster_version

  # Compare the minor as a number, never lexically: sort() is a string sort, so
  # `element(reverse(sort(["1.9", "1.28", "1.34"])), 0)` is "1.9".  Confirmed on OpenTofu 1.12.6.  Dormant
  # today only because EKS retired every 1.9-era version; it returns the day a "1.100" exists.
  newest_minor = max([for version in local.standard_versions : tonumber(split(".", version)[1])]...)

  newest_version = element([
    for version in local.standard_versions : version
    if tonumber(split(".", version)[1]) == local.newest_minor
  ], 0)

  kubernetes_version = coalesce(var.kubernetes_version, local.newest_version)

  # ---------------------------------------------------------------------------------------------------
  # Network
  # ---------------------------------------------------------------------------------------------------

  vpc_id = var.vpc_id != null ? var.vpc_id : one(data.aws_vpc.default[*].id)

  # sort() is safe here: availability zone names differ only in their trailing letter.  A null count means
  # every zone the region offers, which is the default; sorting first is what makes a number select the same
  # zones on every plan rather than whichever order the API answered in.
  availability_zones = slice(
    sort(data.aws_availability_zones.available.names),
    0,
    var.availability_zone_count == null
    ? length(data.aws_availability_zones.available.names)
    : min(var.availability_zone_count, length(data.aws_availability_zones.available.names)),
  )

  subnet_ids = var.subnet_ids != null ? var.subnet_ids : one(data.aws_subnets.selected[*].ids)

  # From the subnets themselves, so it holds whether they came from var.subnet_ids or the default VPC, and
  # whether a zone has one subnet or several.
  subnets_by_zone = {
    for zone in distinct([for subnet in data.aws_subnet.selected : subnet.availability_zone]) :
    zone => sort([for id, subnet in data.aws_subnet.selected : id if subnet.availability_zone == zone])
  }

  # The zones the node groups are built for, in a stable order.  Derived from the chosen subnets, not from
  # var.availability_zone_count or local.availability_zones: a caller passing var.subnet_ids bypassed both.
  node_group_zones = sort(keys(local.subnets_by_zone))

  zone_count = length(local.node_group_zones)

  # "us-west-2a" becomes "a", the suffix in `agents-large-a`.  A regex on the region prefix, not a fixed count
  # from the end, so a region naming its zones differently gives a wrong-looking name over a colliding one.
  zone_suffix = { for zone in local.node_group_zones : zone => replace(zone, "/^${var.region}-?/", "") }

  # One group in one zone, not one per zone: the controller's 500Gi volume binds to the zone it first started
  # in, so a group elsewhere would sit empty and still be simulated against by the autoscaler.
  controller_zone = local.node_group_zones[0]

  # Which zones the control plane's own subnets are in, and this is not the same question as which zones
  # the node groups are in.  EKS fixes the control plane's zone set at creation:
  #
  #   InvalidParameterException: Provided subnets belong to the AZs 'us-west-2a,us-west-2b,us-west-2c,
  #   us-west-2d'. But they should belong to the exact set of AZs 'us-west-2a,us-west-2b' in which subnets
  #   were provided during cluster creation.
  #
  # UpdateClusterConfig accepts more subnets inside the same zones and rejects a different set of zones, so
  # a cluster created across two zones can never be moved to four.  Passing every selected subnet here is
  # therefore what makes var.availability_zone_count unraisable on a live cluster, which is the whole point
  # of reading it from the region.
  #
  # A managed node group's subnets do not have to be the cluster's.  They have to be in the same VPC and
  # able to reach the API endpoint, which every subnet of one VPC is.  So the control plane takes the first
  # var.control_plane_zone_count zones and keeps them for the cluster's life, and the node groups take every
  # zone.  Sorted, so "the first two" is the same two on every plan and adding a third zone does not move
  # them.
  cluster_zones = slice(local.node_group_zones, 0, min(var.control_plane_zone_count, local.zone_count))

  cluster_subnet_ids = flatten([for zone in local.cluster_zones : local.subnets_by_zone[zone]])

  # ---------------------------------------------------------------------------------------------------
  # VPC CNI address targets
  # ---------------------------------------------------------------------------------------------------

  # The two values addons.tf sets on the vpc-cni add-on, hoisted here because the address ceiling below
  # is computed from them.  Read the comment above resource "vpc_cni" in addons.tf before changing either.
  vpc_cni_minimum_ip_target = 4
  vpc_cni_warm_ip_target    = 2

  # An agent node's pods that need an address of their own: the node itself, the agent pod, and
  # ebs-csi-node.  aws-node, kube-proxy, eks-pod-identity-agent and eks-node-monitoring-agent all use host
  # networking and need none.  Written down because the model below needs a number for "in use" and this
  # directory is what decides what runs on an agent node.
  addresses_in_use_per_node = 3

  # What one node actually holds.  ipamd's model is max(MINIMUM_IP_TARGET, in use + WARM_IP_TARGET), which
  # is 5 for the values above, and build cassandra-eks-k8s #9 measured 5.12 across 476 nodes: 48 of them
  # kept a second interface, whose own primary address is a sixth.  The + 1 is that interface, so this is
  # the measured figure rounded up and the ceiling below errs towards fewer nodes rather than towards a pool
  # that cannot fill.
  #
  # Both add-on targets are read, so raising either one lowers the ceiling as it should.
  addresses_per_node = 1 + max(
    local.vpc_cni_minimum_ip_target,
    local.addresses_in_use_per_node + local.vpc_cni_warm_ip_target,
  )

  # ---------------------------------------------------------------------------------------------------
  # The node ceiling the account allows, and the pools scaled to it
  # ---------------------------------------------------------------------------------------------------

  # Three ceilings are computed here and the lowest wins.  Two are account quotas and the third is the
  # subnets; with the pools' own max_size they are the four in "Four ceilings, and the lowest one wins" in
  # ../README.md.  All three are read at apply because a cluster stops at whichever ceiling nobody wrote
  # code for, and the subnets were once exactly that: a build stopped 166 nodes below its configured size
  # on the one ceiling nothing checked.
  #
  # The result is a single scale factor applied to every pool's declared max_size, which keeps the ratios
  # the operator wrote and needs no second set of numbers to maintain.  Above 1 it grows the pools into a
  # raised quota, which is the point: a quota increase then needs no edit here.  Below 1 it shrinks them,
  # so an apply against a smaller account produces a cluster that works rather than node groups whose
  # launches fail with VcpuLimitExceeded.

  declared_vcpus_per_node = { for size, pool in var.agent_pools : size => max([
    for type in pool.instance_types : data.aws_ec2_instance_type.declared[type].default_vcpus
  ]...) }

  # The controller's own share, subtracted before the agents are scaled.  It is one node and it is not
  # optional, so it comes off the top.
  controller_vcpus = var.controller_pool.max_size * max([
    for type in var.controller_pool.instance_types : data.aws_ec2_instance_type.declared[type].default_vcpus
  ]...)
  controller_gib       = var.controller_pool.max_size * var.controller_pool.disk_gib
  controller_addresses = var.controller_pool.max_size * local.addresses_per_node

  # What the pools ask for, in each of the three units.  The 1 floors keep the divisions below defined
  # for a configuration with no agent pools at all.
  declared_agent_vcpus = max(1, sum(concat([0], [for size, pool in var.agent_pools : pool.max_size * local.declared_vcpus_per_node[size]])))
  declared_agent_gib   = max(1, sum(concat([0], [for pool in values(var.agent_pools) : pool.max_size * pool.disk_gib])))
  declared_agent_nodes = max(1, sum(concat([0], [for pool in values(var.agent_pools) : pool.max_size])))

  # Addresses each zone's subnets can ever hold, which is deliberately not what is free right now.
  # available_ip_address_count read during a build would report a low figure and shrink the pools underneath
  # the nodes already using those addresses.  AWS reserves five addresses in every subnet.
  #
  # A subnet with no IPv4 range contributes nothing rather than failing the plan: an IPv6-only subnet
  # reports a null cidr_block, and a cluster given one has other problems to report first.
  zone_address_capacity = { for zone, ids in local.subnets_by_zone : zone => sum([
    for id in ids : data.aws_subnet.selected[id].cidr_block == null
    ? 0
    : floor(pow(2, 32 - tonumber(split("/", data.aws_subnet.selected[id].cidr_block)[1]))) - 5
  ]) }

  # Per zone and not summed, because agent_zone_shares divides every pool evenly between the zones.  A
  # summed figure would read as ample while the smallest zone's subnet ran out, which is the shape of
  # `InsufficientFreeAddressesInSubnet`: the failure is always in one subnet, never in the total.
  subnet_address_capacity = local.zone_count * min([for zone in local.node_group_zones : local.zone_address_capacity[zone]]...)

  # Storage is in TiB and the smaller of the two types is taken; see the data source for why both.
  quota_storage_gib = var.size_pools_to_quotas ? 1024 * min([
    for quota in data.aws_servicequotas_service_quota.ebs_storage_tib : quota.value
  ]...) : 0

  quota_vcpus = var.size_pools_to_quotas ? one(data.aws_servicequotas_service_quota.vcpu_on_demand_standard[*].value) : 0

  # A quota is the account's whole limit for the region and not what is unused, and nothing here can see what
  # else spends it: a second cluster, an unrelated instance, a stray volume.  var.quota_headroom is the share
  # of each quota these pools may claim, so the pools stop short of the limit rather than at it.  Without it
  # the storage ceiling puts 510 volumes of 100 GiB against a 50 TiB quota, which is 51000 GiB of 51200 and
  # leaves two nodes of room for everything else in the region.
  #
  # The controller's own 500Gi jenkins_home claim is one of the things the headroom covers.  It is created in
  # layer 2, from a storage class this directory does not name, so its type is not known here; the default
  # headroom on a 50 TiB quota is 5 TiB, which is ten times that claim.
  claimable_vcpus       = local.quota_vcpus * var.quota_headroom
  claimable_storage_gib = local.quota_storage_gib * var.quota_headroom

  pool_scale_by_vcpu      = (local.claimable_vcpus - local.controller_vcpus) / local.declared_agent_vcpus
  pool_scale_by_storage   = (local.claimable_storage_gib - local.controller_gib) / local.declared_agent_gib
  pool_scale_by_addresses = (local.subnet_address_capacity - local.controller_addresses) / (local.addresses_per_node * local.declared_agent_nodes)

  # Addresses are checked whether or not the quotas are read, because they cost nothing to read: the
  # subnets are already looked up for the node groups.
  pool_scale = var.size_pools_to_quotas ? min(
    local.pool_scale_by_vcpu,
    local.pool_scale_by_storage,
    local.pool_scale_by_addresses,
  ) : min(1, local.pool_scale_by_addresses)

  # Which of them bound, for the agent_node_ceiling output.  Named here rather than derived there by
  # comparing floats: with the quotas unread, pool_scale is min(1, addresses) and neither quota comparison
  # would match, so an equality test would report the wrong ceiling in the one mode the variable exists for.
  pool_scale_bound_by = (!var.size_pools_to_quotas
    ? (local.pool_scale_by_addresses < 1 ? "subnet-addresses" : "declared")
    : (local.pool_scale == local.pool_scale_by_vcpu
      ? "vcpu-quota"
  : (local.pool_scale == local.pool_scale_by_storage ? "ebs-quota" : "subnet-addresses")))

  # The floor is one node for a pool that asked for any, so that scaling down never deletes a pool outright:
  # a pool at one node still runs its steps, slowly, where a pool at zero makes every task for that label
  # queue forever.  A pool declared at max_size 0 was disabled on purpose and stays disabled, which is why
  # the floor is conditional rather than a plain max(1, ...).
  effective_pool_max = { for size, pool in var.agent_pools : size =>
    pool.max_size == 0 ? 0 : max(1, floor(pool.max_size * local.pool_scale))
  }

  # ---------------------------------------------------------------------------------------------------
  # Node groups
  # ---------------------------------------------------------------------------------------------------

  # `.build/run-ci` matches this literal to skip the controller's group when reading pool ceilings from the
  # autoscaler's status configmap; the autoscaler names the group `eks-jenkins-controller-<uuid>`, so the
  # substring survives.  Renamed, the group reads as an agent pool matching no size, reported as
  # unattributed on every deploy.
  controller_node_group_name = "jenkins-controller"

  controller_labels = {
    "cassandra.jenkins.controller" = "true"
  }

  agent_labels = { for size in keys(var.agent_pools) : size => {
    "cassandra.jenkins.agent"         = "true"
    "cassandra.jenkins.agent.${size}" = "true"
  } }

  # One node group per agent size per zone: `agents-large-a`, `agents-large-b`.  run-ci finds the size word
  # delimited by '-' or '_' in `eks-agents-large-a-<uuid>`, so the zone suffix is safe; what it cannot survive
  # is the size becoming a suffix or running together with another word.
  #
  # One zone each, because a managed node group given several subnets spreads its ASG over all of them and
  # then neither it nor the autoscaler can say where the next node lands.  One zone per group makes the
  # simulation of a group at zero exact, keeps a pod with a zone-bound volume off the wrong zone, and stops
  # one zone's capacity starving the pool.
  #
  # `max_size` is divided between the zones, not repeated in each: run-ci sums the maxima it attributes to a
  # size, so a pool of 160 written as four groups of 160 would report 640 and accept an unreachable
  # instanceCap.  The division is exact, remainder to the earliest zones.  The cost is that a pool can no
  # longer reach its ceiling out of one zone alone.
  #
  # What is divided is local.effective_pool_max, not the declared max_size; see the ceiling block above.
  # min_size and desired_size are clamped to it: EKS rejects a group whose minimum exceeds its maximum, and
  # a scaled-down pool would otherwise cross its own floor.
  agent_zone_shares = { for size, pool in var.agent_pools : size => [
    for index, zone in local.node_group_zones : {
      zone         = zone
      min_size     = floor(min(pool.min_size, local.effective_pool_max[size]) / local.zone_count) + (index < min(pool.min_size, local.effective_pool_max[size]) % local.zone_count ? 1 : 0)
      max_size     = floor(local.effective_pool_max[size] / local.zone_count) + (index < local.effective_pool_max[size] % local.zone_count ? 1 : 0)
      desired_size = floor(min(pool.desired_size, local.effective_pool_max[size]) / local.zone_count) + (index < min(pool.desired_size, local.effective_pool_max[size]) % local.zone_count ? 1 : 0)
    }
  ] }

  # Keyed by node group name.  A zone whose share of max_size is zero gets no group: EKS rejects max_size 0,
  # and raising it to 1 would break the sum above.  Only reached when a pool's max_size is below the zone
  # count, and the shares still add up, the dropped zones contributing nothing.
  agent_node_group_specs = merge([
    for size, shares in local.agent_zone_shares : {
      for share in shares : "agents-${size}-${local.zone_suffix[share.zone]}" => {
        size              = size
        availability_zone = share.zone
        subnet_ids        = local.subnets_by_zone[share.zone]
        instance_types    = var.agent_pools[size].instance_types
        capacity_type     = var.agent_pools[size].capacity_type
        disk_gib          = var.agent_pools[size].disk_gib
        labels            = local.agent_labels[size]
        min_size          = share.min_size
        max_size          = share.max_size
        desired_size      = share.desired_size
      } if share.max_size >= 1
    }
  ]...)

  # Which groups belong to each size, for the outputs layer 3 reads.  A pool is one row there however many
  # zones it spans: the instance type and the disk are the pool's, as is check-pool-fit.py's question.
  agent_node_group_names = { for size in keys(var.agent_pools) : size => sort([
    for name, spec in local.agent_node_group_specs : name if spec.size == size
  ]) }

  # Every node group, keyed by node group name.  Drives the autoscaler's ASG tags.  Values are variables
  # and subnet zones only, so these keys are known at plan time.
  node_group_specs = merge(
    {
      (local.controller_node_group_name) = {
        labels   = local.controller_labels
        disk_gib = var.controller_pool.disk_gib
      }
    },
    { for name, spec in local.agent_node_group_specs : name => {
      labels   = spec.labels
      disk_gib = spec.disk_gib
    } },
  )

  # ---------------------------------------------------------------------------------------------------
  # Cluster autoscaler ASG tags
  # ---------------------------------------------------------------------------------------------------

  # What the autoscaler reads off an ASG at zero instances.  EKS managed node groups appear to publish the
  # discovery tags themselves, and the autoscaler can ask eks:DescribeNodegroup for a group's labels, which
  # is why the runbook this replaces scaled from zero untagged.  Both are narrower than they look:
  #
  #  - DescribeNodegroup is called only for a managed group created at zero that never had a node.  After one
  #    scale-up and scale-down the tags are the only record left, which is every agent pool's resting state.
  #  - ephemeral storage is in the launch template, not the instance type, so it cannot be inferred at all.
  #    Every agent podTemplate requests 50Gi.
  #
  # So the tags are redundant for the first scale-up and required for every one after it.
  autoscaler_asg_tags = merge([
    for name, spec in local.node_group_specs : merge(
      {
        "${name}|k8s.io/cluster-autoscaler/enabled" = {
          group = name
          key   = "k8s.io/cluster-autoscaler/enabled"
          value = "true"
        }
        # The second discovery tag.  The chart passes both to --node-group-auto-discovery and an ASG must
        # carry all of them, which keeps two clusters in one account from scaling each other's node groups.
        "${name}|k8s.io/cluster-autoscaler/${var.cluster_name}" = {
          group = name
          key   = "k8s.io/cluster-autoscaler/${var.cluster_name}"
          value = "owned"
        }
        # floor(disk * 0.9) - 2 GiB: the kubelet's default hard eviction threshold holds back 10%, and the
        # AL2023 image plus the agent images take a further couple.  So a 100 GiB volume offers about 88Gi
        # against each podTemplate's 80Gi limit.  Over-reported, the autoscaler starts a node the pod is then
        # evicted from; under-reported, the pool never scales up.
        "${name}|k8s.io/cluster-autoscaler/node-template/resources/ephemeral-storage" = {
          group = name
          key   = "k8s.io/cluster-autoscaler/node-template/resources/ephemeral-storage"
          value = "${floor(spec.disk_gib * 0.9) - 2}Gi"
        }
      },
      # One tag per Kubernetes label, so a pending pod's nodeSelector can be matched against a pool with no
      # nodes to read the label off.
      { for label, value in spec.labels : "${name}|k8s.io/cluster-autoscaler/node-template/label/${label}" => {
        group = name
        key   = "k8s.io/cluster-autoscaler/node-template/label/${label}"
        value = value
      } },
    )
  ]...)

  # ---------------------------------------------------------------------------------------------------
  # Add-ons
  # ---------------------------------------------------------------------------------------------------

  # Every add-on this configuration installs, for the version lookup in data.tf.  The ordering between
  # them is expressed with depends_on in addons.tf, not here.
  addon_names = concat(
    [
      "eks-pod-identity-agent",
      "vpc-cni",
      "kube-proxy",
      "coredns",
      "aws-ebs-csi-driver",
    ],
    var.node_auto_repair ? ["eks-node-monitoring-agent"] : [],
    var.enable_cloudwatch_observability ? ["amazon-cloudwatch-observability"] : [],
    var.enable_external_dns ? ["external-dns"] : [],
  )

  # ---------------------------------------------------------------------------------------------------
  # DNS and TLS
  # ---------------------------------------------------------------------------------------------------

  # Gates tls.tf and the DNS half of the environment output.
  serves_public_name = var.jenkins_hostname != ""

  # Every zone external-dns may write to.  var.dns_hosted_zone_id is folded in rather than listed twice: it
  # holds the name external-dns publishes, so naming it in both variables is a second place to forget.
  external_dns_zone_ids = distinct(concat(
    var.external_dns_hosted_zone_ids,
    var.dns_hosted_zone_id == "" ? [] : [var.dns_hosted_zone_id],
  ))

  # The one validation record ACM asks for, named so tls.tf reads it three times without the unwrapping.
  # `try`, because with no certificate both unwrapping steps are applied to a null.
  certificate_validation_option = try(
    tolist(one(aws_acm_certificate.jenkins[*].domain_validation_options))[0],
    null
  )

  # ---------------------------------------------------------------------------------------------------
  # IAM
  # ---------------------------------------------------------------------------------------------------

  iam_policy_arn_prefix = "arn:${data.aws_partition.current.partition}:iam::aws:policy"

  # The issuer without its scheme, which is the form an IAM condition key takes.
  oidc_issuer_host = replace(aws_eks_cluster.this.identity[0].oidc[0].issuer, "https://", "")

  # Fixed, not left to the chart's fullname template (`<release>-aws-cluster-autoscaler`): the name is in the
  # IRSA trust policy's `sub` condition, so a release renamed in layer 2 would silently lose the role.
  # 2-platform sets the same literal in rbac.serviceAccount.name.
  cluster_autoscaler_namespace       = "kube-system"
  cluster_autoscaler_service_account = "cluster-autoscaler"
}
