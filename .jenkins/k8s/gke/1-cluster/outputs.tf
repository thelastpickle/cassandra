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

output "cluster_name" {
  value       = google_container_cluster.this.name
  description = "GKE cluster name.  Part of the kubectl context `gcloud container clusters get-credentials` writes, and the string 3-smoke/smoke-test.sh asserts the current context contains."
}

output "project" {
  value       = var.project
  description = "Project the cluster is in.  Also the billing, quota and IAM boundary, so it is the scope every spend cap applies to."
}

output "region" {
  value       = var.region
  description = "Region the cluster is in, for `gcloud container clusters get-credentials --region` and for the machine-type lookups in 3-smoke."
}

output "cluster_endpoint" {
  value       = google_container_cluster.this.endpoint
  description = "Kubernetes API server address."
}

output "kubernetes_version" {
  value       = google_container_cluster.this.master_version
  description = "The control plane's version as GKE reports it, which is a full `1.34.1-gke.1000` rather than the minor this configuration asked for."
}

output "node_service_account" {
  value       = google_service_account.node.email
  description = "The identity every node pool runs as.  Holds only what a kubelet needs; no build uses it."
}

output "workload_identity_pool" {
  value       = local.workload_identity_pool
  description = "The Workload Identity pool a Kubernetes service account is bound through, as `<project>.svc.id.goog`.  Note it is per project and not per cluster, so two clusters in one project share it; see iam.tf."
}

output "external_dns_service_account" {
  value       = join("", google_service_account.external_dns[*].email)
  description = "The Google service account external-dns impersonates, or empty when var.enable_external_dns is false."
}

output "controller_node_label" {
  value       = join("=", [keys(local.controller_labels)[0], values(local.controller_labels)[0]])
  description = "The label a node in the controller pool carries, as `key=value`.  What 2-platform pins external-dns with, and what 3-smoke/smoke-test.sh looks for on a Ready node."
}

output "max_pods_per_node" {
  value       = var.max_pods_per_node
  description = "Pods a node may run.  3-smoke/check-pool-fit.py needs it to model the kubelet's reservation, which on EKS is derived from a network interface count and here is simply this figure."
}

output "agent_node_pool_names" {
  value       = sort(keys(local.agent_pool_specs))
  description = "Every agent node pool, one per size per zone, so 3-smoke asserts each zone's pool exists and is autoscaled rather than only one per size."
}

output "jenkins_hostname" {
  value       = var.jenkins_hostname
  description = "The public name Jenkins serves on, or empty when the cluster is reachable only at its load balancer's own address."
}

output "tls_certificate_map" {
  value       = join("", google_certificate_manager_certificate_map.jenkins[*].name)
  description = "The Certificate Manager map holding the certificate for var.jenkins_hostname, or empty when there is none."
}

output "tls_certificate_name" {
  value       = join("", google_certificate_manager_certificate.jenkins[*].name)
  description = "The certificate itself, which is what `gcloud certificate-manager certificates describe` takes and what 3-smoke asserts is ACTIVE.  Empty when there is no public name."
}

output "dns_managed_zone" {
  value       = var.dns_managed_zone
  description = "The Cloud DNS zone holding the public name, or empty.  3-smoke reads the address record out of it, which is how external-dns's one job is asserted rather than a resolver's."
}

output "dns_nameservers" {
  value       = try(one(data.google_dns_managed_zone.jenkins[*].name_servers), [])
  description = "The nameservers of the zone named by var.dns_managed_zone. Set these at the registrar that holds the domain."
}

output "controller_node_pool" {
  description = "The controller node pool, keyed the same way as agent_node_pools so 3-smoke can read both.  One zone, unlike the agent pools; see node-pools.tf."

  value = {
    node_pool_name      = google_container_node_pool.controller.name
    instance_group_urls = google_container_node_pool.controller.managed_instance_group_urls
    zone                = local.controller_zone
    machine_types       = var.controller_pool.machine_types
    spot                = var.controller_pool.spot
    disk_gb             = var.controller_pool.disk_gb
    disk_type           = var.controller_pool.disk_type
    max_pods_per_node   = var.max_pods_per_node
    min_size            = var.controller_pool.min_size
    max_size            = var.controller_pool.max_size
    labels              = local.controller_labels
  }
}

output "agent_node_pools" {
  description = "Every agent pool, keyed by the size word in its `cassandra.jenkins.agent.<size>` label."

  value = { for size, pool in var.agent_pools : size => {
    node_pool_names = local.agent_pool_names[size]

    instance_group_urls = flatten([
      for name in local.agent_pool_names[size] : google_container_node_pool.agents[name].managed_instance_group_urls
    ])

    zones                   = [for name in local.agent_pool_names[size] : local.agent_pool_specs[name].zone]
    machine_types           = pool.machine_types
    spot                    = pool.spot
    disk_gb                 = pool.disk_gb
    disk_type               = pool.disk_type
    max_pods_per_node       = var.max_pods_per_node
    min_size                = min(pool.min_size, local.effective_pool_max[size])
    max_size                = local.effective_pool_max[size]
    declared_max_size       = pool.max_size
    small_workers_per_build = size == "small" ? var.small_workers_per_build : null

    node_selector_label = "cassandra.jenkins.agent.${size}"
  } }
}

output "agent_node_ceiling" {
  precondition {
    condition     = local.pool_limits_fit
    error_message = "The minimum agent pool sizes exceed the quota or address budget. The small pool needs small_workers_per_build + 1 slots for a pipeline and its workers; raise the available capacity or reduce other pools."
  }

  description = "Quota and address ceilings, proportional scale, and the final allocation after reserving minimum pool sizes."

  value = {
    minimum_pool_max = local.minimum_pool_max
    allocation_scale = local.allocation_scale
    scale            = local.pool_scale
    bound_by         = local.pool_scale_bound_by
    read_quotas      = var.size_pools_to_quotas

    quotas = { for metric, scale in local.quota_scale_by_metric : metric => {
      limit     = local.quota_limits[metric]
      usage     = lookup(local.quota_usage, metric, null)
      claimable = local.quota_limits[metric] * var.quota_headroom
      demand    = local.agent_demand[metric] + local.controller_demand[metric]
      nodes     = floor(local.declared_agent_nodes * scale)
    } }

    pod_range = {
      assumed           = local.pod_range_assumed
      addresses         = local.pod_range_addresses
      per_node          = local.pod_addresses_per_node
      max_pods_per_node = var.max_pods_per_node
      nodes             = local.pod_range_node_ceiling
    }

    node_range = {
      addresses = local.node_range_addresses
      nodes     = local.node_range_node_ceiling
    }

    declared_agent_nodes  = local.declared_agent_nodes
    effective_agent_nodes = local.effective_agent_nodes
  }
}

output "environment" {
  description = "Shell exports consumed by the platform and smoke-test scripts."

  value = join("\n", [
    "export GKE_CLUSTER_NAME='${google_container_cluster.this.name}'",
    "export GOOGLE_PROJECT='${var.project}'",
    "export GOOGLE_REGION='${var.region}'",
    # The word gcloud uses for the flag that takes either a region or a zone.  Exported as well as
    # GOOGLE_REGION so that a command in a script reads as the command an operator would type.
    "export GKE_LOCATION='${var.region}'",
    "export GKE_KUBERNETES_VERSION='${google_container_cluster.this.master_version}'",
    "export GKE_CONTROLLER_NODE_LABEL='${join("=", [keys(local.controller_labels)[0], values(local.controller_labels)[0]])}'",
    # Every agent pool, one per size per zone, space separated so a `for` loop in 3-smoke needs no JSON parser.
    "export GKE_AGENT_NODE_POOL_NAMES='${join(" ", sort(keys(local.agent_pool_specs)))}'",
    "export GKE_CONTROLLER_MACHINE_TYPES='${join(" ", var.controller_pool.machine_types)}'",
    "export GKE_CONTROLLER_MAX_SIZE='${var.controller_pool.max_size}'",
    "export GKE_MAX_PODS_PER_NODE='${var.max_pods_per_node}'",
    "export GKE_JENKINS_HOSTNAME='${var.jenkins_hostname}'",
    "export GKE_TLS_CERTIFICATE_MAP='${join("", google_certificate_manager_certificate_map.jenkins[*].name)}'",
    "export GKE_TLS_CERTIFICATE_NAME='${join("", google_certificate_manager_certificate.jenkins[*].name)}'",
    "export GKE_DNS_MANAGED_ZONE='${var.dns_managed_zone}'",
    "export GKE_EXTERNAL_DNS_SERVICE_ACCOUNT='${join("", google_service_account.external_dns[*].email)}'",
    # The spend variables are under the same names the Cloud Function is given, so `make spend` and the
    # deployed guard read one set of names.  All empty on a cluster with no cap set.
    "export GKE_SPEND_CAPS_SECRET='${local.spend_guard_enabled ? local.spend_caps_secret : ""}'",
    "export GKE_SPEND_STATE_SECRET='${local.spend_guard_enabled ? local.spend_state_secret : ""}'",
    "export GKE_SPEND_STATE_BOOTSTRAP_VERSION='${local.spend_guard_enabled ? one(google_secret_manager_secret_version.spend_state[*].version) : ""}'",
    "export GKE_SPEND_AGENT_POOL_NAMES='${local.spend_guard_enabled ? join(" ", local.spend_agent_pool_names) : ""}'",
    "export GKE_SPEND_ALERT_TOPIC='${join("", google_pubsub_topic.spend_alerts[*].name)}'",
    "export GKE_SPEND_METRIC_PREFIX='${local.spend_metric_prefix}'",
    "export GKE_SPEND_BILLING_TABLE='${var.spend_billing_table}'",
    "export GKE_SPEND_PRICE_PER_VCPU_HOUR='${var.spend_price_per_vcpu_hour}'",
    "export GKE_SPEND_FIXED_USD_PER_DAY='${var.spend_fixed_usd_per_day}'",
    "export GKE_SPEND_COST_REFRESH_HOURS='${var.spend_cost_refresh_hours}'",
    "export GKE_SPEND_GUARD_FUNCTION='${join("", google_cloudfunctions2_function.spend_guard[*].name)}'",
    "export GKE_SPEND_GUARD_SCHEDULER_JOB='${join("", google_cloud_scheduler_job.spend_guard[*].name)}'",
    "export GKE_SPEND_GUARD_SCHEDULER_REGION='${join("", google_cloud_scheduler_job.spend_guard[*].region)}'",
    "export GKE_SPEND_GUARD_INTERVAL_MINUTES='${var.spend_guard_interval_minutes}'",
    "export GKE_SPEND_STALLED_ALERT_POLICY='${join("", google_monitoring_alert_policy.spend_guard_stalled[*].display_name)}'",
  ])
}

output "spend_guard" {
  description = "Spend caps and guard resources. Null when all caps are unset."

  value = local.spend_guard_enabled ? {
    caps               = local.spend_caps
    function_name      = one(google_cloudfunctions2_function.spend_guard[*].name)
    scheduler_job      = one(google_cloud_scheduler_job.spend_guard[*].name)
    scheduler_region   = one(google_cloud_scheduler_job.spend_guard[*].region)
    interval_minutes   = var.spend_guard_interval_minutes
    caps_secret        = local.spend_caps_secret
    state_secret       = local.spend_state_secret
    alert_topic        = one(google_pubsub_topic.spend_alerts[*].name)
    alert_email        = var.spend_alert_email
    metric_prefix      = local.spend_metric_prefix
    stalled_alert      = one(google_monitoring_alert_policy.spend_guard_stalled[*].display_name)
    budget_name        = join("", google_billing_budget.spend[*].display_name)
    billing_table      = var.spend_billing_table
    agent_pool_names   = local.spend_agent_pool_names
    agent_pool_maximum = local.spend_agent_pool_maxima
  } : null
}
