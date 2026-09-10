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

  # False, so EKS installs no add-on of its own.  Left true, it creates vpc-cni, kube-proxy and CoreDNS as
  # plain manifests at cluster creation: copies that `aws eks describe-addon` cannot see, whose version
  # nothing reports and nothing upgrades.  addons.tf then creates the managed add-ons over them, and
  # resolve_conflicts_on_create = "NONE" refuses a field the bootstrapped copy already holds, which the
  # vpc-cni configuration_values there always is.
  #
  # A cluster created before this was set needs var.bootstrap_self_managed_addons = true, because the
  # argument replaces the cluster: see the variable.
  bootstrap_self_managed_addons = var.bootstrap_self_managed_addons

  access_config {
    # API, not API_AND_CONFIG_MAP.  Access entries are IAM resources that a plan can show; the aws-auth
    # configmap is a hand-edited YAML blob in kube-system that nothing tracks and one bad edit locks
    # everyone out of.
    authentication_mode = "API"

    # Whoever applies this gets cluster-admin.  Anyone else needs an aws_eks_access_entry, which is not
    # written here because it would name a principal in one account; see ../README.md.
    bootstrap_cluster_creator_admin_permissions = true
  }

  upgrade_policy {
    # STANDARD: EKS upgrades the control plane itself at the end of standard support rather than moving
    # it to paid extended support.  A CI cluster is the wrong place to pay for a version nobody chose to
    # stay on, and var.kubernetes_version left null keeps the config in step with that.
    support_type = "STANDARD"
  }

  zonal_shift_config {
    # Off, and explicit.  A zonal shift triggered by the Application Recovery Controller suspends node
    # auto repair for the whole cluster, so this and var.node_auto_repair are coupled.  A single-region
    # CI cluster with one controller instance has nothing to shift away from.
    enabled = false
  }

  vpc_config {
    # The control plane's own zones, not every zone the node groups use.  EKS fixes this zone set at
    # creation and rejects an update that changes it, so passing everything here would make
    # var.availability_zone_count unraisable for the cluster's life.  See local.cluster_zones.
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
