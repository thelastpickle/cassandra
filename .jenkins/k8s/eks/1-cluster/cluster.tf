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

# Created before the cluster, and depended on below.  EKS creates this log group itself on the first
# write, with no expiry, and a retention set afterwards does not apply to what is already in it.
resource "aws_cloudwatch_log_group" "cluster" {
  name              = "/aws/eks/${var.cluster_name}/cluster"
  retention_in_days = var.log_retention_days
}

resource "aws_eks_cluster" "this" {
  name     = var.cluster_name
  role_arn = aws_iam_role.cluster.arn
  version  = local.kubernetes_version

  enabled_cluster_log_types = var.control_plane_log_types

  bootstrap_self_managed_addons = var.bootstrap_self_managed_addons

  access_config {
    authentication_mode = "API"

    # Whoever applies this gets cluster-admin.  Anyone else needs an aws_eks_access_entry, which is not
    # written here because it would name a principal in one account; see ../README.md.
    bootstrap_cluster_creator_admin_permissions = true
  }

  upgrade_policy {
    support_type = "STANDARD"
  }

  zonal_shift_config {
    enabled = false
  }

  vpc_config {
    subnet_ids = local.cluster_subnet_ids

    # Both endpoints.  Private so that nodes and the autoscaler reach the API server without leaving the
    # VPC; public because `.build/run-ci` runs on a laptop.
    endpoint_private_access = true
    endpoint_public_access  = true
    public_access_cidrs     = var.public_access_cidrs
  }

  depends_on = [
    aws_cloudwatch_log_group.cluster,
    aws_iam_role_policy_attachment.cluster,
  ]

  lifecycle {
    precondition {
      condition     = length(local.cluster_zones) >= 2
      error_message = "EKS needs subnets in at least two availability zones. Fewer than two were resolved: check var.vpc_id, var.subnet_ids, var.availability_zone_count and var.control_plane_zone_count against the region."
    }

  }
}
