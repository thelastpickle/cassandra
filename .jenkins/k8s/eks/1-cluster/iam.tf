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

# The roles the EKS console offers to build for you as "Create recommended role".
#
# That button has no API.  Every role it creates has to be written out, and not writing them out is the
# single largest reason the console runbook this directory replaces cannot be replayed: the roles it
# produced exist in one account, under names nothing recorded, holding policies nobody chose.

# ---------------------------------------------------------------------------------------------------
# Trust policies
# ---------------------------------------------------------------------------------------------------

data "aws_iam_policy_document" "eks_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["eks.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "ec2_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

# Pod Identity's trust policy.  sts:TagSession is not optional: the Pod Identity agent tags the session
# with the cluster, namespace and service account, and a role that refuses the tags cannot be assumed at
# all.  This is the difference between a Pod Identity trust policy and an ordinary service one, and it
# fails with an AccessDenied that names neither tags nor the pod.
#
# Both conditions narrow it to this cluster.  Unconditioned, `pods.eks.amazonaws.com` is every cluster in the
# account: a Pod Identity association made in a second cluster, which this directory's own naming
# deliberately allows, would assume these roles.  aws:SourceArn is the cluster, aws:SourceAccount is the
# account, and AWS documents both for exactly this.
data "aws_iam_policy_document" "pods_eks_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole", "sts:TagSession"]

    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_eks_cluster.this.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

# ---------------------------------------------------------------------------------------------------
# Cluster role
# ---------------------------------------------------------------------------------------------------

resource "aws_iam_role" "cluster" {
  name               = "${var.cluster_name}-cluster"
  description        = "EKS control plane role for the ${var.cluster_name} Jenkins cluster"
  assume_role_policy = data.aws_iam_policy_document.eks_assume.json
}

resource "aws_iam_role_policy_attachment" "cluster" {
  for_each = toset([
    "${local.iam_policy_arn_prefix}/AmazonEKSClusterPolicy",
    # Lets the control plane manage the branch network interfaces security groups for pods uses.  Not
    # used by this configuration, but attaching it is what the console does, and adding it later is a
    # cluster-wide change made under pressure.
    "${local.iam_policy_arn_prefix}/AmazonEKSVPCResourceController",
  ])

  role       = aws_iam_role.cluster.name
  policy_arn = each.value
}

# ---------------------------------------------------------------------------------------------------
# Node role
# ---------------------------------------------------------------------------------------------------

# One role for all four node groups.  Nothing an agent runs uses AWS credentials, so there is nothing to
# separate: the agent containers talk to a docker daemon in their own pod and to the Jenkins controller.
resource "aws_iam_role" "node" {
  name               = "${var.cluster_name}-node"
  description        = "EKS worker node role for the ${var.cluster_name} Jenkins cluster"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

resource "aws_iam_role_policy_attachment" "node" {
  for_each = toset([
    "${local.iam_policy_arn_prefix}/AmazonEKSWorkerNodePolicy",
    # Pull rights on ECR.  The agent images come from apache.jfrog.io, not ECR, but the add-on images do.
    # AmazonEC2ContainerRegistryPullOnly is the tighter modern equivalent if every add-on version in use
    # tolerates it.
    "${local.iam_policy_arn_prefix}/AmazonEC2ContainerRegistryReadOnly",
  ])

  role       = aws_iam_role.node.name
  policy_arn = each.value
}

# AmazonEKS_CNI_Policy is deliberately not attached here.  The console's recommended node role carries
# it, which gives every pod on the node the CNI's rights through the instance profile.  It is attached
# to the vpc-cni add-on's own role in iam-addons.tf instead, and reaches only the aws-node service
# account.
