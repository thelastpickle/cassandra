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

# EKS add-ons, in a deliberate order.
#
# Two orderings are expressed here, and both of them are the fix for a symptom the console runbook this
# directory replaces recorded as normal:
#
#  1. eks-pod-identity-agent before any add-on that carries a pod_identity_association.  The association
#     is what the agent serves; created against a cluster with no agent, the add-on's pods start, find no
#     credentials endpoint, and fail on an AWS call rather than on anything that names Pod Identity.
#     The agent itself runs with host networking, so it does not need the CNI it makes credentials
#     available to.
#
#  2. Everything with a Deployment after aws_eks_node_group.controller.  CoreDNS and the EBS CSI
#     controller have to be scheduled somewhere; created before any node exists they report DEGRADED
#     until compute arrives.  The runbook noted add-ons sitting degraded and waited it out.  It is
#     avoidable, and an add-on that is DEGRADED for a reason nobody chose is indistinguishable from one
#     that is DEGRADED for a reason somebody should look at.
#
# Every version comes from data.aws_eks_addon_version, keyed on the cluster's own Kubernetes version.
# There is no add-on version written down in this directory.
#
# resolve_conflicts_on_create is NONE throughout, and rests on var.bootstrap_self_managed_addons being
# false: with no self-managed copy on the cluster there is nothing for a create to conflict with, and NONE
# then means any conflict is a surprise worth stopping for.  On a cluster where that variable is true, the
# vpc-cni create below conflicts on its own configuration_values and the apply stops there.
#
# resolve_conflicts_on_update is OVERWRITE, which says that configuration_values below is the source of
# truth and a change made with kubectl is meant to be lost at the next apply.

# ---------------------------------------------------------------------------------------------------
# Before the nodes: Pod Identity, CNI, kube-proxy, node monitoring
# ---------------------------------------------------------------------------------------------------

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

  # Two address settings, and they are what decides how many agent nodes this cluster can hold.
  #
  # Both are unset by default, and WARM_ENI_TARGET=1 governs when they are: every node then keeps one
  # whole spare network interface attached.  On an instance whose interfaces carry 15 addresses each, a
  # node that needs 3 addresses holds 30.  A pool of one agent pod per node needs the node's own address,
  # the agent's, and ebs-csi-node's; aws-node, kube-proxy, eks-pod-identity-agent and
  # eks-node-monitoring-agent all use host networking and need none.
  #
  # Measured on build cassandra-eks-k8s #8, before this block was applied: 314 nodes, 552
  # interfaces, 8182 addresses, 26.1 addresses a node, and both subnets at zero free.  All four node
  # groups sat in `OutOfResource placeholder-cannot-be-fulfilled` backoff with 451 pods Pending, and the
  # scaling activities named it exactly: `InsufficientFreeAddressesInSubnet`.  The pools were configured
  # for 480 nodes and the account allowed 622 on vCPU and 512 on gp3 storage, so nothing else was close.
  # This ceiling is the one that binds, and it is invisible in every place that reports the others.
  #
  #   N_max = free addresses across the chosen subnets / addresses a node holds
  #
  # A node holds max(MINIMUM_IP_TARGET, in-use + WARM_IP_TARGET), so 4 here.  Measured on #9, with this
  # block applied: 476 nodes holding 2437 addresses, which is 5.12 a node, against the 26.1
  # above.  So the gain is a factor of five, not the order of magnitude the 30-address arithmetic
  # suggests, and the same two subnets hold about 1600 nodes rather than 313.  Both keys are checked
  # against the add-on's JSON schema at apply, where each takes a string of digits; see the note above
  # resource "coredns" for how to print that schema.
  #
  # Why these and not ENABLE_PREFIX_DELEGATION, which is the answer AWS documents first.  Prefix
  # delegation raises the pods per node well above the default, and 3-smoke/check-pool-fit.py models the
  # kubelet's memory reservation from the default maxPods; turning it on without changing that model makes
  # the estimated mode wrong in the direction that reads as "fits".  These two keys do not touch maxPods.
  # The EKS bootstrap computes maxPods from the instance type's interface and address limits, which these
  # leave alone, so the fit model stays true.
  #
  # The cost is EC2 API calls.  A node that runs out of warm addresses calls AssignPrivateIpAddresses
  # before a pod can start, so MINIMUM_IP_TARGET is set above what an agent node actually needs: the node
  # boots with its addresses already in hand and the agent pod waits for nothing.  Raise MINIMUM_IP_TARGET
  # rather than WARM_IP_TARGET if a pod template is ever added that needs more than one address.
  #
  # It takes effect at once, not as the pools cycle.  An apply that changes these values rolls the aws-node
  # DaemonSet, ipamd restarts with the new targets, and it releases the surplus.  Measured 25 minutes after
  # the roll: of 475 instances, the 92 launched before it held 5.58 addresses a node and the 383 launched
  # after held exactly 5.00.  Those 92 had held about 26 each an hour earlier.  Reclaim is not quite
  # complete: 48 of the 92 kept a second interface, whose own primary address is the sixth they hold.  To
  # read what one node holds:
  #
  #   kubectl -n kube-system exec <aws-node pod> -c aws-node -- \
  #       curl -s localhost:61678/metrics | grep '^awscni_\(total\|assigned\)_ip_addresses'
  #
  # Both numbers live in locals.tf, because the node ceiling there is computed from them: a node that holds
  # more addresses fits fewer times into the subnets.  Changing one here alone would leave that arithmetic
  # describing the previous value.
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

# ---------------------------------------------------------------------------------------------------
# After the controller node group: CoreDNS, EBS CSI
# ---------------------------------------------------------------------------------------------------

# Pinned to the controller node: left alone CoreDNS lands wherever there is room, which here means an agent
# node, and every agent pool scales to zero between builds and would take cluster DNS with it.
#
# replicaCount follows the controller pool's minimum, because the EKS CoreDNS Deployment carries a pod
# anti-affinity on its own label at hostname granularity: two replicas need two nodes, and pinned to a
# one-node pool one of them stays Pending for the cluster's life.  The cost of one replica is that replacing
# the controller node stops cluster DNS until the new pod starts, and that outage is already happening for
# the single StatefulSet with its bound volume.
#
# configuration_values is checked against the add-on's JSON schema at apply, not plan, so a key misspelled
# here fails an apply that has already created a cluster.  Check it first with `aws eks
# describe-addon-configuration --addon-name coredns --kubernetes-version <version> --query
# configurationSchema`; ../README.md repeats the command.
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

# The controller half is pinned for the same reason as CoreDNS, and with more at stake: it is what
# attaches and detaches the controller's 500Gi volume.  On an agent node that scales away mid-build the
# next attach has nothing to run on.  The node half is a DaemonSet and stays everywhere.
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

# ---------------------------------------------------------------------------------------------------
# Optional, both off by default
# ---------------------------------------------------------------------------------------------------

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

    # The zone this cluster serves, when it serves one.  Empty when var.jenkins_hostname is unset, which
    # external-dns reads as "no filter"; the IAM policy in iam-addons.tf is the limit that matters, and
    # this is the cheaper second one.
    domainFilters = compact([one(data.aws_route53_zone.jenkins[*].name)])

    # `sync`, not the default `upsert-only`.  This cluster is rebuilt, and upsert-only leaves the old record
    # pointing at a load balancer that no longer exists: the name resolves and nothing answers.
    #
    # sync deletes only what this cluster owns, ownership being a TXT record beside each address record keyed
    # by txtOwnerId.  So two clusters in one zone are safe as long as their names differ.
    policy     = "sync"
    txtOwnerId = var.cluster_name
  })

  depends_on = [
    aws_eks_addon.pod_identity_agent,
    aws_eks_node_group.controller,
    aws_iam_role_policy.external_dns,
  ]
}
