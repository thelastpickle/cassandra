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

  standard_versions = data.aws_eks_cluster_versions.standard.cluster_versions[*].cluster_version

  newest_minor = max([for version in local.standard_versions : tonumber(split(".", version)[1])]...)

  newest_version = element([
    for version in local.standard_versions : version
    if tonumber(split(".", version)[1]) == local.newest_minor
  ], 0)

  kubernetes_version = coalesce(var.kubernetes_version, local.newest_version)

  vpc_id = var.vpc_id != null ? var.vpc_id : one(data.aws_vpc.default[*].id)

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

  cluster_zones = slice(local.node_group_zones, 0, min(var.control_plane_zone_count, local.zone_count))

  cluster_subnet_ids = flatten([for zone in local.cluster_zones : local.subnets_by_zone[zone]])

  # The two values addons.tf sets on the vpc-cni add-on, hoisted here because the address ceiling below
  # is computed from them.  Read the comment above resource "vpc_cni" in addons.tf before changing either.
  vpc_cni_minimum_ip_target = 4
  vpc_cni_warm_ip_target    = 2

  addresses_in_use_per_node = 3

  addresses_per_node = 1 + max(
    local.vpc_cni_minimum_ip_target,
    local.addresses_in_use_per_node + local.vpc_cni_warm_ip_target,
  )

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

  zone_address_capacity = { for zone, ids in local.subnets_by_zone : zone => sum([
    for id in ids : data.aws_subnet.selected[id].cidr_block == null
    ? 0
    : floor(pow(2, 32 - tonumber(split("/", data.aws_subnet.selected[id].cidr_block)[1]))) - 5
  ]) }

  subnet_address_capacity = local.zone_count * min([for zone in local.node_group_zones : local.zone_address_capacity[zone]]...)

  # Storage is in TiB and the smaller of the two types is taken; see the data source for why both.
  quota_storage_gib = var.size_pools_to_quotas ? 1024 * min([
    for quota in data.aws_servicequotas_service_quota.ebs_storage_tib : quota.value
  ]...) : 0

  quota_vcpus = var.size_pools_to_quotas ? one(data.aws_servicequotas_service_quota.vcpu_on_demand_standard[*].value) : 0

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

  pool_scale_bound_by = (!var.size_pools_to_quotas
    ? (local.pool_scale_by_addresses < 1 ? "subnet-addresses" : "declared")
    : (local.pool_scale == local.pool_scale_by_vcpu
      ? "vcpu-quota"
  : (local.pool_scale == local.pool_scale_by_storage ? "ebs-quota" : "subnet-addresses")))

  controller_node_group_name = "jenkins-controller"

  controller_labels = {
    "cassandra.jenkins.controller" = "true"
  }

  agent_labels = { for size in keys(var.agent_pools) : size => {
    "cassandra.jenkins.agent"         = "true"
    "cassandra.jenkins.agent.${size}" = "true"
  } }

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

  iam_policy_arn_prefix = "arn:${data.aws_partition.current.partition}:iam::aws:policy"

  # The issuer without its scheme, which is the form an IAM condition key takes.
  oidc_issuer_host = replace(aws_eks_cluster.this.identity[0].oidc[0].issuer, "https://", "")

  cluster_autoscaler_namespace       = "kube-system"
  cluster_autoscaler_service_account = "cluster-autoscaler"
}
