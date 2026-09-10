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

# Add-on roles, assumed through Pod Identity.
#
# Pod Identity rather than IRSA for these: the association is an argument on aws_eks_addon, so the role,
# the add-on and the binding between them are one resource graph, and there is no OIDC provider or
# certificate thumbprint in the middle of it.  The cluster autoscaler is the exception, and iam-autoscaler.tf
# says why.

# ---------------------------------------------------------------------------------------------------
# vpc-cni
# ---------------------------------------------------------------------------------------------------

resource "aws_iam_role" "vpc_cni" {
  name               = "${var.cluster_name}-vpc-cni"
  description        = "Amazon VPC CNI add-on role for ${var.cluster_name}, assumed by kube-system/aws-node"
  assume_role_policy = data.aws_iam_policy_document.pods_eks_assume.json
}

resource "aws_iam_role_policy_attachment" "vpc_cni" {
  role       = aws_iam_role.vpc_cni.name
  policy_arn = "${local.iam_policy_arn_prefix}/AmazonEKS_CNI_Policy"
}

# ---------------------------------------------------------------------------------------------------
# aws-ebs-csi-driver
# ---------------------------------------------------------------------------------------------------

# Needed, not optional.  jenkins-deployment.yaml asks for a 500Gi PersistentVolumeClaim, and since
# Kubernetes 1.23 the in-tree EBS provisioner behind the `gp2` StorageClass is a shim onto this driver.
# Without the add-on the claim stays Pending, the controller StatefulSet never starts, and the only
# symptom is a pod that will not schedule.
resource "aws_iam_role" "ebs_csi" {
  name               = "${var.cluster_name}-ebs-csi"
  description        = "EBS CSI driver add-on role for ${var.cluster_name}, assumed by kube-system/ebs-csi-controller-sa"
  assume_role_policy = data.aws_iam_policy_document.pods_eks_assume.json
}

# Under service-role/, unlike every other managed policy this configuration attaches.  AWS files a
# managed policy in one of three paths, and which one is a fact about that policy rather than about its
# name.  Confirm any policy added here before trusting the path:
#
#   aws iam list-policies --scope AWS --query "Policies[?PolicyName=='<name>'].Arn" --output text
#
# The wrong path fails at apply with NoSuchEntity, "does not exist or is not attachable".
resource "aws_iam_role_policy_attachment" "ebs_csi" {
  role       = aws_iam_role.ebs_csi.name
  policy_arn = "${local.iam_policy_arn_prefix}/service-role/AmazonEBSCSIDriverPolicy"
}

# ---------------------------------------------------------------------------------------------------
# amazon-cloudwatch-observability
# ---------------------------------------------------------------------------------------------------

resource "aws_iam_role" "cloudwatch_observability" {
  count = var.enable_cloudwatch_observability ? 1 : 0

  name               = "${var.cluster_name}-cloudwatch-observability"
  description        = "CloudWatch Observability add-on role for ${var.cluster_name}"
  assume_role_policy = data.aws_iam_policy_document.pods_eks_assume.json
}

resource "aws_iam_role_policy_attachment" "cloudwatch_observability" {
  for_each = var.enable_cloudwatch_observability ? toset([
    "${local.iam_policy_arn_prefix}/CloudWatchAgentServerPolicy",
    "${local.iam_policy_arn_prefix}/AWSXrayWriteOnlyAccess",
  ]) : toset([])

  role       = one(aws_iam_role.cloudwatch_observability[*].name)
  policy_arn = each.value
}

# ---------------------------------------------------------------------------------------------------
# external-dns
# ---------------------------------------------------------------------------------------------------

# AWS documents this add-on's managed policy as AmazonRoute53FullAccess, which is every record in every
# zone in the account.  The same page documents the three actions it actually needs, so those are what
# it gets, scoped to local.external_dns_zone_ids: var.dns_hosted_zone_id and var.external_dns_hosted_zone_ids.
data "aws_iam_policy_document" "external_dns" {
  count = var.enable_external_dns ? 1 : 0

  statement {
    sid     = "ChangeRecordsInNamedZones"
    effect  = "Allow"
    actions = ["route53:ChangeResourceRecordSets"]
    resources = [
      for zone_id in local.external_dns_zone_ids :
      "arn:${data.aws_partition.current.partition}:route53:::hostedzone/${zone_id}"
    ]
  }

  statement {
    sid    = "ListZonesAndRecords"
    effect = "Allow"
    actions = [
      "route53:ListHostedZones",
      "route53:ListResourceRecordSets",
      "route53:ListTagsForResource",
    ]
    # These three take no resource-level condition; Route 53 rejects the policy if one is given.
    resources = ["*"]
  }
}

resource "aws_iam_role" "external_dns" {
  count = var.enable_external_dns ? 1 : 0

  name               = "${var.cluster_name}-external-dns"
  description        = "external-dns add-on role for ${var.cluster_name}, assumed by external-dns/external-dns"
  assume_role_policy = data.aws_iam_policy_document.pods_eks_assume.json

  lifecycle {
    precondition {
      condition     = length(local.external_dns_zone_ids) > 0
      error_message = "enable_external_dns needs at least one hosted zone, in dns_hosted_zone_id or external_dns_hosted_zone_ids: a role that can write to no zone is a role that does nothing, and the alternative is account-wide Route 53 write access."
    }
  }
}

resource "aws_iam_role_policy" "external_dns" {
  count = var.enable_external_dns ? 1 : 0

  name   = "route53-records"
  role   = one(aws_iam_role.external_dns[*].id)
  policy = one(data.aws_iam_policy_document.external_dns[*].json)
}
