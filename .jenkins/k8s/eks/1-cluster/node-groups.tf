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

# Managed node groups: one for the controller, and one per agent size per availability zone.
#
# Every group is restricted to a single zone.  See local.agent_zone_shares in locals.tf for why, and for
# why each zone gets a share of the pool's max_size rather than a copy of it.
#
# No launch template and no `release_version`.  Omitting the version is what makes EKS choose the newest
# AMI it publishes for the cluster's Kubernetes version, and omitting the launch template is what lets
# `disk_size` and `instance_types` stay arguments here instead of a second resource to keep in step.
# The cost of that choice: no user data, so anything that has to run before the kubelet does needs a
# launch template adding.  Nothing here needs one.
#
# No taints on the agent pools either.  A taint would keep everything but agents off those nodes, which
# is the intent, but the agent podTemplates in jenkins-deployment.yaml carry no tolerations and that
# file is out of scope; a tainted pool would simply never run a build.  The pinning is done from the
# other side instead: CoreDNS, the EBS CSI controller and the autoscaler all select the controller node.
#
# ami_type is x86_64 throughout, and each group asserts its own instance types against it below.
# var.agent_pools and var.controller_pool take any type, and an AL2023_x86_64_STANDARD group of m7g nodes
# fails at the node group with a message about the AMI rather than at the plan with one about the pool.
# A precondition and not a `check` block, because a warning here is an apply that stops half way; the
# architectures come from data.aws_ec2_instance_type, which is already read for the vCPU ceiling.
#
# The agent images in jenkins-deployment.yaml are amd64, so arm64 is not a matter of changing this literal.

# ---------------------------------------------------------------------------------------------------
# Controller
# ---------------------------------------------------------------------------------------------------

# One group, in one zone, and not one per zone.  The controller is a single pod with a 500Gi volume bound
# to the zone it first started in, so groups in the other zones could never hold it.  The zone is the
# first of local.node_group_zones, which is sorted, so it does not move under a plan.
resource "aws_eks_node_group" "controller" {
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = local.controller_node_group_name
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = local.subnets_by_zone[local.controller_zone]

  ami_type       = "AL2023_x86_64_STANDARD"
  capacity_type  = var.controller_pool.capacity_type
  instance_types = var.controller_pool.instance_types
  disk_size      = var.controller_pool.disk_gib

  labels = local.controller_labels

  scaling_config {
    min_size     = var.controller_pool.min_size
    max_size     = var.controller_pool.max_size
    desired_size = var.controller_pool.desired_size
  }

  update_config {
    # One at a time.  With min_size 1 this means EKS replaces the node by draining the only one there
    # is, so a Kubernetes upgrade stops Jenkins for as long as the StatefulSet takes to reattach its
    # volume in the new node's zone.  That is the honest behaviour of a single-controller cluster.
    max_unavailable = 1
  }

  node_repair_config {
    enabled = var.node_auto_repair
  }

  depends_on = [
    aws_iam_role_policy_attachment.node,
    # The CNI has to be running before a node can report Ready, and kube-proxy before a pod on it can
    # reach a Service.  Both are add-ons here rather than bootstrapped manifests, so the dependency is
    # explicit; without it the group is created against a cluster with no CNI and the nodes sit NotReady
    # until vpc-cni happens to arrive.
    aws_eks_addon.vpc_cni,
    aws_eks_addon.kube_proxy,
  ]

  lifecycle {
    # The autoscaler owns the ASG's capacity from the moment it starts.  Without this, every plan after
    # a scale-up proposes putting desired_size back to what is written above, and applying it deletes
    # nodes with builds on them.
    ignore_changes = [scaling_config[0].desired_size]

    precondition {
      condition = alltrue([
        for type in var.controller_pool.instance_types :
        contains(data.aws_ec2_instance_type.declared[type].supported_architectures, "x86_64")
      ])
      error_message = "Every type in controller_pool.instance_types must be x86_64: this group is AL2023_x86_64_STANDARD."
    }
  }
}

# ---------------------------------------------------------------------------------------------------
# Agents
# ---------------------------------------------------------------------------------------------------

# Keyed by node group name, `agents-<size>-<zone letter>`, so a zone added or removed moves one group
# rather than renumbering the rest.
resource "aws_eks_node_group" "agents" {
  for_each = local.agent_node_group_specs

  cluster_name    = aws_eks_cluster.this.name
  node_group_name = each.key
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = each.value.subnet_ids

  ami_type       = "AL2023_x86_64_STANDARD"
  capacity_type  = each.value.capacity_type
  instance_types = each.value.instance_types
  disk_size      = each.value.disk_gib

  labels = each.value.labels

  scaling_config {
    min_size     = each.value.min_size
    max_size     = each.value.max_size
    desired_size = each.value.desired_size
  }

  update_config {
    # A percentage, not a count: these groups run from 0 to a share of 160 nodes, and a fixed count is
    # either pointlessly slow at the top of that range or a mass eviction at the bottom.
    max_unavailable_percentage = 25
  }

  node_repair_config {
    enabled = var.node_auto_repair
  }

  depends_on = [
    aws_iam_role_policy_attachment.node,
    aws_eks_addon.vpc_cni,
    aws_eks_addon.kube_proxy,
  ]

  lifecycle {
    ignore_changes = [scaling_config[0].desired_size]

    precondition {
      condition = alltrue([
        for type in each.value.instance_types :
        contains(data.aws_ec2_instance_type.declared[type].supported_architectures, "x86_64")
      ])
      error_message = "Every type in agent_pools[*].instance_types must be x86_64: these groups are AL2023_x86_64_STANDARD, and the agent images in jenkins-deployment.yaml are amd64."
    }
  }
}

# ---------------------------------------------------------------------------------------------------
# Autoscaler tags on the underlying Auto Scaling groups
# ---------------------------------------------------------------------------------------------------

locals {
  # Node group name to the name of the ASG EKS created for it.  A managed node group has exactly one,
  # but the API returns a list, so it is flattened rather than indexed.
  node_group_asg_names = merge(
    {
      (local.controller_node_group_name) = flatten([
        for resource in aws_eks_node_group.controller.resources : resource.autoscaling_groups[*].name
      ])[0]
    },
    { for name, group in aws_eks_node_group.agents : name => flatten([
      for resource in group.resources : resource.autoscaling_groups[*].name
    ])[0] },
  )
}

# One resource per tag, because the ASG is not managed here: EKS created it, and `tags` on
# aws_eks_node_group tags the node group. The cluster autoscaler reads tags on the Auto Scaling group
# itself, and aws_autoscaling_group_tag is the documented way to set one on an ASG that another service
# owns.  See the comment on local.autoscaler_asg_tags in locals.tf for what each tag is for.
resource "aws_autoscaling_group_tag" "autoscaler" {
  for_each = local.autoscaler_asg_tags

  autoscaling_group_name = local.node_group_asg_names[each.value.group]

  tag {
    key   = each.value.key
    value = each.value.value

    # These are hints for the autoscaler's simulation of a node that does not exist yet.  Copying them
    # onto the instances would put `k8s.io/cluster-autoscaler/node-template/...` keys on every EC2
    # instance, where nothing reads them.
    propagate_at_launch = false
  }
}
