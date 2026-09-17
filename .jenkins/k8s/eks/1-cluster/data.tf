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

# The account this is being applied to, and its partition.  Used to build managed-policy ARNs, which
# differ between aws, aws-cn and aws-us-gov.
data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}

data "aws_eks_cluster_versions" "standard" {
  cluster_type   = "eks"
  version_status = "STANDARD_SUPPORT"
}

# The default VPC, when var.vpc_id is null.
data "aws_vpc" "default" {
  count = var.vpc_id == null ? 1 : 0

  default = true
}

data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

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

data "aws_subnet" "selected" {
  for_each = toset(local.subnet_ids)

  id = each.key
}

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

data "aws_ec2_instance_type" "declared" {
  for_each = toset(concat(
    var.controller_pool.instance_types,
    flatten([for pool in values(var.agent_pools) : pool.instance_types]),
  ))

  instance_type = each.key
}

data "aws_servicequotas_service_quota" "vcpu_on_demand_standard" {
  count = var.size_pools_to_quotas ? 1 : 0

  service_code = "ec2"
  quota_code   = "L-1216C47A"
}

data "aws_servicequotas_service_quota" "ebs_storage_tib" {
  for_each = var.size_pools_to_quotas ? {
    gp3 = "L-7A658B76"
    gp2 = "L-D18FCD1D"
  } : {}

  service_code = "ebs"
  quota_code   = each.value
}
