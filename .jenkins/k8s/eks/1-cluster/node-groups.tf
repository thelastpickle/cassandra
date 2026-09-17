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
    max_unavailable = 1
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
        for type in var.controller_pool.instance_types :
        contains(data.aws_ec2_instance_type.declared[type].supported_architectures, "x86_64")
      ])
      error_message = "Every type in controller_pool.instance_types must be x86_64: this group is AL2023_x86_64_STANDARD."
    }
  }
}

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

resource "aws_autoscaling_group_tag" "autoscaler" {
  for_each = local.autoscaler_asg_tags

  autoscaling_group_name = local.node_group_asg_names[each.value.group]

  tag {
    key   = each.value.key
    value = each.value.value

    propagate_at_launch = false
  }
}
