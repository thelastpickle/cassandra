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

# The cluster autoscaler's role, assumed through IRSA and not Pod Identity.
#
# This follows the autoscaler maintainers' documented path rather than this repository's preference.
# cluster-autoscaler/cloudprovider/aws/README.md in kubernetes/autoscaler recommends IAM Roles for
# Service Accounts, links a worked OIDC federation example, and does not mention EKS Pod Identity at
# all.  Pod Identity very likely works: the autoscaler reads its credentials through the standard SDK
# chain, which the Pod Identity agent serves.  Nobody has said so in writing.
#
# If it turns out to work, this whole file collapses to the same shape as iam-addons.tf, and both the
# aws_iam_openid_connect_provider below and the tls provider in versions.tf are deleted with it.

# EKS publishes an OIDC issuer per cluster, but does not register it with IAM.  Nothing federates until
# this exists.
resource "aws_iam_openid_connect_provider" "cluster" {
  url = aws_eks_cluster.this.identity[0].oidc[0].issuer

  client_id_list = ["sts.amazonaws.com"]

  # IAM has verified EKS issuer certificates against its own trust store since mid-2023, so this value
  # is no longer checked.  The argument is still required, and the certificate is still the only correct
  # thing to put in it.
  thumbprint_list = [data.tls_certificate.oidc.certificates[0].sha1_fingerprint]
}

data "aws_iam_policy_document" "cluster_autoscaler_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.cluster.arn]
    }

    # Both conditions matter.  Without `sub` any service account in the cluster can assume this role;
    # without `aud` a token minted for a different audience is accepted.
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_issuer_host}:sub"
      values   = ["system:serviceaccount:${local.cluster_autoscaler_namespace}:${local.cluster_autoscaler_service_account}"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_issuer_host}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

# The "Full Cluster Autoscaler Features" policy from the autoscaler's own AWS README, which is what ASG
# auto-discovery and dynamic EC2 list generation need.
data "aws_iam_policy_document" "cluster_autoscaler" {
  statement {
    sid    = "Discover"
    effect = "Allow"
    actions = [
      "autoscaling:DescribeAutoScalingGroups",
      "autoscaling:DescribeAutoScalingInstances",
      "autoscaling:DescribeLaunchConfigurations",
      "autoscaling:DescribeScalingActivities",
      "ec2:DescribeImages",
      "ec2:DescribeInstanceTypes",
      "ec2:DescribeLaunchTemplateVersions",
      "ec2:GetInstanceTypesFromInstanceRequirements",
      # How the autoscaler learns a managed node group's labels while the group has never had a node.
      # The ASG tags in locals.tf cover the case this does not, which is every scale-up after the first.
      "eks:DescribeNodegroup",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "Scale"
    effect = "Allow"
    actions = [
      "autoscaling:SetDesiredCapacity",
      "autoscaling:TerminateInstanceInAutoScalingGroup",
    ]

    # The autoscaler's README recommends narrowing exactly this statement, either by ASG ARN or by tag.
    # By tag, because the ASG names EKS generates carry a uuid that is not known until apply.
    resources = [
      "arn:${data.aws_partition.current.partition}:autoscaling:${var.region}:${data.aws_caller_identity.current.account_id}:autoScalingGroup:*:autoScalingGroupName/*"
    ]

    condition {
      test     = "StringEquals"
      variable = "autoscaling:ResourceTag/k8s.io/cluster-autoscaler/${var.cluster_name}"
      values   = ["owned"]
    }
  }
}

resource "aws_iam_role" "cluster_autoscaler" {
  name               = "${var.cluster_name}-cluster-autoscaler"
  description        = "Cluster autoscaler role for ${var.cluster_name}, assumed by ${local.cluster_autoscaler_namespace}/${local.cluster_autoscaler_service_account} through IRSA"
  assume_role_policy = data.aws_iam_policy_document.cluster_autoscaler_assume.json
}

resource "aws_iam_role_policy" "cluster_autoscaler" {
  name   = "autoscaling"
  role   = aws_iam_role.cluster_autoscaler.id
  policy = data.aws_iam_policy_document.cluster_autoscaler.json
}
