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

# The whole of the interface to layers 2 and 3.  Data flows one way: ../Makefile reads these with
# `tofu output` and puts them in the environment, and nothing in 2-platform or 3-smoke reads AWS state
# or a tfstate file for itself.
#
# Scalars are shaped for `tofu output -raw`.  The two structured outputs are shaped for
# `tofu output -json`, which is how 3-smoke/check-pool-fit.py is given the pools to check.

output "cluster_name" {
  description = "EKS cluster name. Also the kubectl context name the Makefile writes, and the string 3-smoke/smoke-test.sh asserts the current context contains."
  value       = aws_eks_cluster.this.name
}

output "region" {
  description = "Region the cluster is in, for `aws eks update-kubeconfig` and for the EC2 instance-type lookups in 3-smoke."
  value       = var.region
}

output "cluster_endpoint" {
  description = "Kubernetes API server endpoint."
  value       = aws_eks_cluster.this.endpoint
}

output "kubernetes_version" {
  description = <<-EOT
    The control plane's Kubernetes version, as EKS reports it rather than as this configuration asked
    for it.  2-platform reads it to choose a matching cluster autoscaler image: the autoscaler is
    versioned against the Kubernetes minor it schedules for, and a mismatch shows up as pods the
    autoscaler declines to act on rather than as an error.
  EOT
  value       = aws_eks_cluster.this.version
}

output "oidc_provider_arn" {
  description = "ARN of the IAM OIDC provider registered for this cluster. Only the cluster autoscaler federates through it; see iam-autoscaler.tf."
  value       = aws_iam_openid_connect_provider.cluster.arn
}

output "cluster_autoscaler_role_arn" {
  description = "Role the cluster autoscaler assumes. 2-platform annotates its service account with this."
  value       = aws_iam_role.cluster_autoscaler.arn
}

output "cluster_autoscaler_namespace" {
  description = "Namespace the autoscaler's service account must be in for the IRSA trust policy to match."
  value       = local.cluster_autoscaler_namespace
}

output "cluster_autoscaler_service_account" {
  description = "Service account name the autoscaler's IRSA trust policy is written against. 2-platform must set exactly this in rbac.serviceAccount.name."
  value       = local.cluster_autoscaler_service_account
}

output "controller_node_label" {
  description = "The label a node in the controller pool carries, as `key=value`. What 2-platform pins the autoscaler with, and what 3-smoke/smoke-test.sh looks for on a Ready node."
  value       = join("=", [keys(local.controller_labels)[0], values(local.controller_labels)[0]])
}

output "addon_names" {
  description = "Every add-on installed, for 3-smoke/smoke-test.sh to assert ACTIVE on. Follows var.node_auto_repair and the two enable_ variables, so the smoke test does not need to know which are on."
  value       = local.addon_names
}

output "controller_node_group" {
  description = "The controller node group, keyed the same way as agent_node_groups so 3-smoke can iterate over both. One zone, unlike the agent pools; see node-groups.tf."
  value = {
    node_group_name   = aws_eks_node_group.controller.node_group_name
    asg_name          = local.node_group_asg_names[local.controller_node_group_name]
    availability_zone = local.controller_zone
    instance_types    = var.controller_pool.instance_types
    capacity_type     = var.controller_pool.capacity_type
    disk_gib          = var.controller_pool.disk_gib
    min_size          = var.controller_pool.min_size
    max_size          = var.controller_pool.max_size
    labels            = local.controller_labels
  }
}

output "jenkins_hostname" {
  description = "The public name Jenkins serves on, or empty when the cluster is reachable only at its load balancer's own name."
  value       = var.jenkins_hostname
}

output "tls_certificate_arn" {
  description = <<-EOT
    ACM certificate for var.jenkins_hostname, or empty when there is none.

    `join("")` over a list of at most one, rather than `one()`, because `one()` of an empty list is null
    and this value is interpolated into the environment output below.  A string template rejects a null.
  EOT
  value       = join("", aws_acm_certificate.jenkins[*].arn)
}

output "dns_nameservers" {
  description = <<-EOT
    The nameservers of the hosted zone named by var.dns_hosted_zone_id.

    Set these four at the registrar that holds the domain.  Until that is done nothing in the zone
    resolves for anyone else, the ACM certificate stays PENDING_VALIDATION, and Jenkins stays on plain
    HTTP.  It is the one step in this directory that cannot be scripted; see ../README.md.
  EOT
  value       = try(one(data.aws_route53_zone.jenkins[*].name_servers), [])
}

output "environment" {
  description = <<-EOT
    Every scalar above as shell `export` lines, which is the form ../Makefile puts them in the
    environment with:

      tofu -chdir=1-cluster output -raw environment > .eks-env

    Built here rather than assembled in the Makefile from eight `tofu output -raw` calls.  One call
    cannot report half the environment, and the shell variable names are part of the contract with
    ../2-platform and ../3-smoke, so they belong next to the values they carry.
  EOT
  value = join("\n", [
    "export EKS_CLUSTER_NAME='${aws_eks_cluster.this.name}'",
    "export AWS_REGION='${var.region}'",
    "export EKS_KUBERNETES_VERSION='${aws_eks_cluster.this.version}'",
    # The endpoint is deliberately absent, and stays in the cluster_endpoint output above instead.  No
    # reader needs it; kubeconfig carries it.  A string template rejects a null, and the endpoint is null
    # in state whenever a create failed while waiting for the control plane, which is the one moment
    # `make env` has to keep working.  See "When something stops part way" in ../README.md.
    "export EKS_CONTROLLER_NODE_LABEL='${join("=", [keys(local.controller_labels)[0], values(local.controller_labels)[0]])}'",
    "export EKS_CLUSTER_AUTOSCALER_ROLE_ARN='${aws_iam_role.cluster_autoscaler.arn}'",
    "export EKS_CLUSTER_AUTOSCALER_NAMESPACE='${local.cluster_autoscaler_namespace}'",
    "export EKS_CLUSTER_AUTOSCALER_SERVICE_ACCOUNT='${local.cluster_autoscaler_service_account}'",
    # Space separated, so that a `for` loop in 3-smoke/smoke-test.sh needs no JSON parser.
    "export EKS_ADDON_NAMES='${join(" ", local.addon_names)}'",
    # Every agent group, one per size per zone, so that 3-smoke/smoke-test.sh asserts the autoscaler
    # discovered each zone's group rather than only one of them per size.
    "export EKS_AGENT_NODE_GROUP_NAMES='${join(" ", keys(local.agent_node_group_specs))}'",
    # The controller's own node, which spends from the same account vCPU quota as an agent's and is a node
    # the autoscaler counts under --max-nodes-total.  ../vcpu-quota.py needs both to turn that quota into a
    # node ceiling; the agent pools reach it through agent_node_groups, and the controller has no such
    # output because it is not a pool.  Space separated, so no JSON parser is needed to read it.
    "export EKS_CONTROLLER_INSTANCE_TYPES='${join(" ", var.controller_pool.instance_types)}'",
    "export EKS_CONTROLLER_MAX_SIZE='${var.controller_pool.max_size}'",
    # All three are the empty string on a cluster with no public name, which is what ../Makefile tests to
    # decide whether to configure the name and TLS at all.  They are exported empty rather than omitted:
    # an unset variable and an empty one differ under `set -o nounset`, and 3-smoke uses it.
    "export EKS_JENKINS_HOSTNAME='${var.jenkins_hostname}'",
    "export EKS_TLS_CERTIFICATE_ARN='${join("", aws_acm_certificate.jenkins[*].arn)}'",
    "export EKS_DNS_HOSTED_ZONE_ID='${var.dns_hosted_zone_id}'",
    # The spend guard, under the same names its Lambda is given, so `make spend` and the Lambda read one set
    # of names.  All empty on a cluster with no cap set, which is what ../Makefile tests to decide whether
    # `make spend` has anything to report; see spend-guard.tf.
    "export EKS_SPEND_CAPS_PARAMETER='${local.spend_guard_enabled ? local.spend_caps_parameter : ""}'",
    "export EKS_SPEND_STATE_PARAMETER='${local.spend_guard_enabled ? local.spend_state_parameter : ""}'",
    "export EKS_SPEND_AGENT_ASG_NAMES='${local.spend_guard_enabled ? join(" ", local.spend_agent_asg_names) : ""}'",
    "export EKS_SPEND_ALERT_TOPIC_ARN='${join("", aws_sns_topic.spend_alerts[*].arn)}'",
    "export EKS_SPEND_METRIC_NAMESPACE='${local.spend_metric_namespace}'",
    "export EKS_SPEND_COST_METRIC='${var.spend_cost_metric}'",
    "export EKS_SPEND_PRICE_PER_VCPU_HOUR='${var.spend_price_per_vcpu_hour}'",
    "export EKS_SPEND_FIXED_USD_PER_DAY='${var.spend_fixed_usd_per_day}'",
    "export EKS_SPEND_CE_REFRESH_HOURS='${var.spend_cost_refresh_hours}'",
    "export EKS_SPEND_GUARD_FUNCTION='${join("", aws_lambda_function.spend_guard[*].function_name)}'",
    "export EKS_SPEND_GUARD_INTERVAL_MINUTES='${var.spend_guard_interval_minutes}'",
    "export EKS_SPEND_STALLED_ALARM='${join("", aws_cloudwatch_metric_alarm.spend_guard_stalled[*].alarm_name)}'",
  ])
}

output "spend_guard" {
  description = <<-EOT
    The cap on what this cluster may spend, and the pieces that act on it.  Null when no cap is set, which is
    what `make caps` writes when the operator answers `none` to all three.

    `agent_asg_names` is what the brake acts on: `Launch` suspended on each of those groups is a stopped
    cluster, and it is the only thing the guard ever changes.  Read the brake's state, which lives on the
    groups themselves rather than in any second place:

      aws autoscaling describe-auto-scaling-groups --auto-scaling-group-names <name> \
          --query 'AutoScalingGroups[].SuspendedProcesses'
  EOT
  value = local.spend_guard_enabled ? {
    caps               = local.spend_caps
    function_name      = one(aws_lambda_function.spend_guard[*].function_name)
    interval_minutes   = var.spend_guard_interval_minutes
    caps_parameter     = local.spend_caps_parameter
    state_parameter    = local.spend_state_parameter
    alert_topic_arn    = one(aws_sns_topic.spend_alerts[*].arn)
    alert_email        = var.spend_alert_email
    metric_namespace   = local.spend_metric_namespace
    stalled_alarm_name = one(aws_cloudwatch_metric_alarm.spend_guard_stalled[*].alarm_name)
    budget_names       = sort([for budget in aws_budgets_budget.spend : budget.name])
    agent_asg_names    = local.spend_agent_asg_names
  } : null
}

output "agent_node_groups" {
  description = <<-EOT
    Every agent pool, keyed by the size word in its `cassandra.jenkins.agent.<size>` label.

    One row per pool, not per node group: a pool is now several groups, one per availability zone, and
    the instance type, the disk and the label they share are what makes the pool a pool.  The sizes are
    the pool's totals, which is what the per-zone shares add up to.

    3-smoke/check-pool-fit.py joins this to jenkins-deployment.yaml on `node_selector_label`: that is the
    label an agent podTemplate's nodeSelector names, and the only thing tying a podTemplate to a pool.

    `max_size` is what the node groups were actually created with, which is the declared size scaled to the
    ceiling the account allows; `declared_max_size` is what var.agent_pools asked for.  The two differ
    whenever `tofu output agent_node_ceiling` reports a factor other than 1, and it is `max_size` that any
    instanceCap has to match.
  EOT
  value = { for size, pool in var.agent_pools : size => {
    node_group_names = local.agent_node_group_names[size]
    asg_names = [
      for name in local.agent_node_group_names[size] : local.node_group_asg_names[name]
    ]
    availability_zones = [
      for name in local.agent_node_group_names[size] : local.agent_node_group_specs[name].availability_zone
    ]
    instance_types      = pool.instance_types
    capacity_type       = pool.capacity_type
    disk_gib            = pool.disk_gib
    min_size            = min(pool.min_size, local.effective_pool_max[size])
    max_size            = local.effective_pool_max[size]
    declared_max_size   = pool.max_size
    node_selector_label = "cassandra.jenkins.agent.${size}"
  } }
}

output "agent_node_ceiling" {
  description = <<-EOT
    The three ceilings on how many nodes may exist at once, which one bound, and the factor every pool's
    max_size was scaled by.  See var.size_pools_to_quotas, and "Four ceilings, and the lowest one wins" in
    ../README.md.

    `nodes` on each row is that ceiling expressed in whole agent nodes of the declared mix, so the three are
    comparable at a glance.  A factor of 1 or more means the declared sizes fit; below 1 the pools were cut
    to what the account holds.
  EOT
  value = {
    scale       = local.pool_scale
    bound_by    = local.pool_scale_bound_by
    read_quotas = var.size_pools_to_quotas
    # Null rather than a negative node count when the quotas were not read.  With var.size_pools_to_quotas
    # false the two quota values are 0, and reporting `(0 - controller) / declared` as a ceiling would be a
    # figure that looks measured and is not.
    vcpu = var.size_pools_to_quotas ? {
      quota = local.quota_vcpus
      nodes = floor(local.declared_agent_nodes * local.pool_scale_by_vcpu)
    } : null
    storage = var.size_pools_to_quotas ? {
      quota_gib = local.quota_storage_gib
      nodes     = floor(local.declared_agent_nodes * local.pool_scale_by_storage)
    } : null
    addresses = {
      subnet_capacity    = local.subnet_address_capacity
      addresses_per_node = local.addresses_per_node
      nodes              = floor(local.declared_agent_nodes * local.pool_scale_by_addresses)
    }
    declared_agent_nodes = local.declared_agent_nodes
    # concat, for the same reason the locals are floored: a configuration with no agent pools at all must
    # still produce this output rather than fail on an empty list.
    effective_agent_nodes = sum(concat([0], values(local.effective_pool_max)))
  }
}
