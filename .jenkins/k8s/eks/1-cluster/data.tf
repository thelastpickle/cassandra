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

# Everything in this file is read from AWS rather than written down.  Nothing here should ever be
# replaced by a literal: an account identifier, a subnet identifier or a version number transcribed
# into a repository is a fact that was true once, on one person's machine.

# The account this is being applied to, and its partition.  Used to build managed-policy ARNs, which
# differ between aws, aws-cn and aws-us-gov.
data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}

# The newest Kubernetes version EKS will create a cluster at.
#
# STANDARD_SUPPORT excludes both EXTENDED_SUPPORT, which costs more per hour and is a version EKS
# already wants you off, and UNSUPPORTED, which cannot be created.
data "aws_eks_cluster_versions" "standard" {
  cluster_type   = "eks"
  version_status = "STANDARD_SUPPORT"
}

# The default VPC, when var.vpc_id is null.
data "aws_vpc" "default" {
  count = var.vpc_id == null ? 1 : 0

  default = true
}

# opt-in-not-required excludes Local Zones and Wavelength Zones.  EKS cannot place a control plane
# interface in either, and a subnet lookup that returns one produces a cluster that fails to create
# with a message about the subnet rather than about the zone.
data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

# Subnets, when var.subnet_ids is null.
#
# The availability-zone filter is what var.availability_zone_count acts through.  Without it this returns
# every subnet in the VPC whatever that variable says, so the count would silently do nothing and the node
# groups would span every zone in the region: more cross-zone traffic between agents and the controller, and
# no way to say which zone the next agent node lands in.
data "aws_subnets" "selected" {
  count = var.subnet_ids == null ? 1 : 0

  filter {
    name   = "vpc-id"
    values = [local.vpc_id]
  }

  filter {
    name   = "availability-zone"
    values = local.availability_zones
  }
}

# Which availability zone each chosen subnet is in.
#
# Node groups are one zone each, so a subnet's zone is what decides which group it belongs to.  Neither
# aws_subnets nor a var.subnet_ids list reports it, and the order of a subnet list says nothing about
# zones, so each subnet is read for itself.
data "aws_subnet" "selected" {
  for_each = toset(local.subnet_ids)

  id = each.key
}

# The add-on version to install, resolved per add-on against the cluster's own Kubernetes version.
#
# `kubernetes_version = aws_eks_cluster.this.version`, not local.kubernetes_version: reading it back
# off the cluster resource is what stops the control plane and its add-ons drifting apart.  Were this
# keyed on the local, a pin changed here and not yet applied would resolve add-on versions for a
# control plane that does not exist yet.
data "aws_eks_addon_version" "this" {
  for_each = toset(local.addon_names)

  addon_name         = each.key
  kubernetes_version = aws_eks_cluster.this.version
  most_recent        = true
}

# The cluster's OIDC issuer certificate, for the thumbprint the IAM OIDC provider wants.  See the
# comment on aws_iam_openid_connect_provider in iam-autoscaler.tf.
data "tls_certificate" "oidc" {
  url = aws_eks_cluster.this.identity[0].oidc[0].issuer
}

# How many vCPUs a node of each declared instance type has, for the vCPU half of the node ceiling in
# locals.tf.  Read rather than written down: an instance type changed in var.agent_pools would otherwise
# leave a transcribed 8 behind, and the ceiling would be computed for the type that used to be there.
#
# This one is not gated on var.size_pools_to_quotas, so `ec2:DescribeInstanceTypes` is now needed by every
# plan in this directory.  It is in every read-only EC2 policy, and unlike the quota reads below there is no
# arithmetic to fall back to without it.
data "aws_ec2_instance_type" "declared" {
  for_each = toset(concat(
    var.controller_pool.instance_types,
    flatten([for pool in values(var.agent_pools) : pool.instance_types]),
  ))

  instance_type = each.key
}

# The two account quotas that decide how many nodes may exist at once.  Both are read only when
# var.size_pools_to_quotas is true, because the reads need permissions not every operator has:
# `servicequotas:GetServiceQuota` for the value, and `servicequotas:GetAWSDefaultServiceQuota`, which the
# data source calls for the `default_value` and `adjustable` attributes it publishes whether or not this
# configuration reads them.  Granting only the first still fails the plan, with an AccessDeniedException that
# names neither this file nor the variable that would have turned the read off.
#
# L-1216C47A is "Running On-Demand Standard instances", counted in vCPUs, and it covers the c and m
# families both agent pools use.
data "aws_servicequotas_service_quota" "vcpu_on_demand_standard" {
  count = var.size_pools_to_quotas ? 1 : 0

  service_code = "ec2"
  quota_code   = "L-1216C47A"
}

# Storage, in TiB, for the root volume every node carries.  Both types are read because which one applies
# is not pinned here: node-groups.tf sets disk_size and no launch template, so the volume type comes from
# the EKS AMI's own default.  locals.tf takes the smaller of the two, which is right whichever it is.
data "aws_servicequotas_service_quota" "ebs_storage_tib" {
  for_each = var.size_pools_to_quotas ? {
    gp3 = "L-7A658B76"
    gp2 = "L-D18FCD1D"
  } : {}

  service_code = "ebs"
  quota_code   = each.value
}
