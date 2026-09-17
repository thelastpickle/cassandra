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

resource "aws_eks_addon" "pod_identity_agent" {
  cluster_name  = aws_eks_cluster.this.name
  addon_name    = "eks-pod-identity-agent"
  addon_version = data.aws_eks_addon_version.this["eks-pod-identity-agent"].version

  resolve_conflicts_on_create = "NONE"
  resolve_conflicts_on_update = "OVERWRITE"
}

resource "aws_eks_addon" "vpc_cni" {
  cluster_name  = aws_eks_cluster.this.name
  addon_name    = "vpc-cni"
  addon_version = data.aws_eks_addon_version.this["vpc-cni"].version

  resolve_conflicts_on_create = "NONE"
  resolve_conflicts_on_update = "OVERWRITE"

  # Not service_account_role_arn, which is the IRSA argument and needs the OIDC provider.  This is the
  # role in iam-addons.tf reaching exactly kube-system/aws-node.
  pod_identity_association {
    role_arn        = aws_iam_role.vpc_cni.arn
    service_account = "aws-node"
  }

  configuration_values = jsonencode({
    env = {
      MINIMUM_IP_TARGET = tostring(local.vpc_cni_minimum_ip_target)
      WARM_IP_TARGET    = tostring(local.vpc_cni_warm_ip_target)
    }
  })

  depends_on = [
    aws_eks_addon.pod_identity_agent,
    aws_iam_role_policy_attachment.vpc_cni,
  ]
}

resource "aws_eks_addon" "kube_proxy" {
  cluster_name  = aws_eks_cluster.this.name
  addon_name    = "kube-proxy"
  addon_version = data.aws_eks_addon_version.this["kube-proxy"].version

  resolve_conflicts_on_create = "NONE"
  resolve_conflicts_on_update = "OVERWRITE"

  depends_on = [aws_eks_addon.pod_identity_agent]
}

# Widens what node auto repair can see.  A DaemonSet, so it is created before the node groups; see
# var.node_auto_repair for what it adds over the kubelet's Ready condition alone.
resource "aws_eks_addon" "node_monitoring_agent" {
  count = var.node_auto_repair ? 1 : 0

  cluster_name  = aws_eks_cluster.this.name
  addon_name    = "eks-node-monitoring-agent"
  addon_version = data.aws_eks_addon_version.this["eks-node-monitoring-agent"].version

  resolve_conflicts_on_create = "NONE"
  resolve_conflicts_on_update = "OVERWRITE"

  depends_on = [aws_eks_addon.pod_identity_agent]
}

resource "aws_eks_addon" "coredns" {
  cluster_name  = aws_eks_cluster.this.name
  addon_name    = "coredns"
  addon_version = data.aws_eks_addon_version.this["coredns"].version

  resolve_conflicts_on_create = "NONE"
  resolve_conflicts_on_update = "OVERWRITE"

  configuration_values = jsonencode({
    nodeSelector = local.controller_labels
    replicaCount = var.controller_pool.min_size
  })

  depends_on = [aws_eks_node_group.controller]
}

resource "aws_eks_addon" "ebs_csi" {
  cluster_name  = aws_eks_cluster.this.name
  addon_name    = "aws-ebs-csi-driver"
  addon_version = data.aws_eks_addon_version.this["aws-ebs-csi-driver"].version

  resolve_conflicts_on_create = "NONE"
  resolve_conflicts_on_update = "OVERWRITE"

  pod_identity_association {
    role_arn        = aws_iam_role.ebs_csi.arn
    service_account = "ebs-csi-controller-sa"
  }

  configuration_values = jsonencode({
    controller = {
      nodeSelector = local.controller_labels
      replicaCount = var.controller_pool.min_size
    }
  })

  depends_on = [
    aws_eks_addon.pod_identity_agent,
    aws_eks_node_group.controller,
    aws_iam_role_policy_attachment.ebs_csi,
  ]
}

resource "aws_eks_addon" "cloudwatch_observability" {
  count = var.enable_cloudwatch_observability ? 1 : 0

  cluster_name  = aws_eks_cluster.this.name
  addon_name    = "amazon-cloudwatch-observability"
  addon_version = data.aws_eks_addon_version.this["amazon-cloudwatch-observability"].version

  resolve_conflicts_on_create = "NONE"
  resolve_conflicts_on_update = "OVERWRITE"

  pod_identity_association {
    role_arn        = one(aws_iam_role.cloudwatch_observability[*].arn)
    service_account = "cloudwatch-agent"
  }

  depends_on = [
    aws_eks_addon.pod_identity_agent,
    aws_eks_node_group.controller,
    aws_iam_role_policy_attachment.cloudwatch_observability,
  ]
}

resource "aws_eks_addon" "external_dns" {
  count = var.enable_external_dns ? 1 : 0

  cluster_name  = aws_eks_cluster.this.name
  addon_name    = "external-dns"
  addon_version = data.aws_eks_addon_version.this["external-dns"].version

  resolve_conflicts_on_create = "NONE"
  resolve_conflicts_on_update = "OVERWRITE"

  pod_identity_association {
    role_arn        = one(aws_iam_role.external_dns[*].arn)
    service_account = "external-dns"
  }

  configuration_values = jsonencode({
    # Pinned to the controller node, for the same reason the autoscaler and the EBS CSI controller are:
    # the agent pools scale to zero, and this is a single replica holding a lease on every record it owns.
    nodeSelector = local.controller_labels

    domainFilters = compact([one(data.aws_route53_zone.jenkins[*].name)])

    policy     = "sync"
    txtOwnerId = var.cluster_name
  })

  depends_on = [
    aws_eks_addon.pod_identity_agent,
    aws_eks_node_group.controller,
    aws_iam_role_policy.external_dns,
  ]
}
