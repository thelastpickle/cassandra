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

locals {
  channel_default_version = try(
    data.google_container_engine_versions.this.release_channel_default_version[var.release_channel],
    data.google_container_engine_versions.this.latest_master_version,
  )

  kubernetes_version = coalesce(var.kubernetes_version, local.channel_default_version)
}

locals {
  # Sorted before slicing, so that a zone count selects the same zones on every plan.  sort() is safe here
  # because zone names differ only in their trailing letter.
  available_zones = sort(data.google_compute_zones.available.names)

  node_zones = slice(
    local.available_zones,
    0,
    var.zone_count == null
    ? length(local.available_zones)
    : min(var.zone_count, length(local.available_zones)),
  )

  zone_count = length(local.node_zones)

  zone_suffix = { for zone in local.node_zones : zone => replace(zone, "/^${var.region}-?/", "") }

  controller_zone = local.node_zones[0]
}

locals {
  # Every machine type named anywhere in this configuration.
  declared_machine_types = distinct(concat(
    var.controller_pool.machine_types,
    flatten([for pool in values(var.agent_pools) : pool.machine_types]),
  ))

  machine_vcpus = {
    for type in local.declared_machine_types :
    type => tonumber(element(split("-", type), length(split("-", type)) - 1))
  }

  # The series selects the regional CPU quota.
  machine_series = {
    for type in local.declared_machine_types : type => element(split("-", type), 0)
  }
}

locals {
  quota_limits = var.size_pools_to_quotas ? {
    for key, value in one(data.external.quotas[*].result) :
    replace(key, "-", "_") => tonumber(value)
    if !startswith(key, "usage_") && value != ""
  } : {}

  quota_usage = var.size_pools_to_quotas ? {
    for key, value in one(data.external.quotas[*].result) :
    replace(trimprefix(key, "usage_"), "-", "_") => tonumber(value)
    if startswith(key, "usage_") && value != ""
  } : {}
}

locals {
  quota_pools = merge(
    {
      "controller" = {
        is_agent  = false
        max_size  = var.controller_pool.max_size
        vcpus     = max([for type in var.controller_pool.machine_types : local.machine_vcpus[type]]...)
        series    = distinct([for type in var.controller_pool.machine_types : local.machine_series[type]])
        spot      = var.controller_pool.spot
        disk_gb   = var.controller_pool.disk_gb
        disk_type = var.controller_pool.disk_type
      }
    },
    { for size, pool in var.agent_pools : size => {
      is_agent = true
      max_size = pool.max_size
      # The largest of a pool's types, because a pool given several may create any of them and only the
      # largest cannot under-count.
      vcpus     = max([for type in pool.machine_types : local.machine_vcpus[type]]...)
      series    = distinct([for type in pool.machine_types : local.machine_series[type]])
      spot      = pool.spot
      disk_gb   = pool.disk_gb
      disk_type = pool.disk_type
    } },
  )

  # Only the metrics this project actually reports a limit for.  A metric nobody reports is not a ceiling.
  binding_metrics = distinct([
    for entry in flatten([for name, units in local.pool_quota_units : keys(units)]) :
    entry if contains(keys(local.quota_limits), entry)
  ])

  # Demand per metric, split so the controller's share can be taken off the top.
  controller_demand = {
    for metric in local.binding_metrics :
    metric => sum(concat([0], [
      for name, units in local.pool_quota_units :
      lookup(units, metric, 0) * local.quota_pools[name].max_size
      if !local.quota_pools[name].is_agent
    ]))
  }

  agent_demand = {
    for metric in local.binding_metrics :
    metric => sum(concat([0], [
      for name, units in local.pool_quota_units :
      lookup(units, metric, 0) * local.quota_pools[name].max_size
      if local.quota_pools[name].is_agent
    ]))
  }

  quota_scale_by_metric = {
    for metric in local.binding_metrics :
    metric => (local.quota_limits[metric] * var.quota_headroom - local.controller_demand[metric]) / local.agent_demand[metric]
    if local.agent_demand[metric] > 0
  }
}

locals {
  pod_addresses_per_node = max(16, pow(2, ceil(log(2 * var.max_pods_per_node, 2))))

  pod_range_cidr = var.pod_range_name == null ? null : one([
    for range in data.google_compute_subnetwork.selected.secondary_ip_range :
    range.ip_cidr_range if range.range_name == var.pod_range_name
  ])

  pod_range_assumed = local.pod_range_cidr == null

  pod_range_addresses = local.pod_range_assumed ? pow(2, 32 - 14) : pow(2, 32 - tonumber(split("/", local.pod_range_cidr)[1]))

  # Nodes the pod range holds, less the controller's own node.
  pod_range_node_ceiling = floor(local.pod_range_addresses / local.pod_addresses_per_node) - var.controller_pool.max_size

  node_range_addresses = pow(2, 32 - tonumber(split("/", data.google_compute_subnetwork.selected.ip_cidr_range)[1])) - 4

  node_range_node_ceiling = local.node_range_addresses - var.controller_pool.max_size
}

locals {
  # What the pools ask for, floored at 1 so the divisions stay defined for a configuration with no agent
  # pools at all.
  declared_agent_nodes = max(1, sum(concat([0], [for pool in values(var.agent_pools) : pool.max_size])))

  address_scale = min(
    local.pod_range_node_ceiling / local.declared_agent_nodes,
    local.node_range_node_ceiling / local.declared_agent_nodes,
  )

  pool_scale = var.size_pools_to_quotas ? min(concat(
    values(local.quota_scale_by_metric),
    [local.address_scale],
  )...) : min(1, local.address_scale)

  binding_quota_metrics = sort([
    for metric, scale in local.quota_scale_by_metric : metric if scale == local.pool_scale
  ])

  pool_scale_bound_by = (!var.size_pools_to_quotas
    ? (local.address_scale < 1 ? "addresses" : "declared")
    : (local.pool_scale == local.address_scale
      ? "addresses"
  : (length(local.binding_quota_metrics) > 0 ? join(", ", local.binding_quota_metrics) : "declared")))
}

locals {
  controller_pool_name = "jenkins-controller"

  controller_labels = {
    "cassandra.jenkins.controller" = "true"
  }

  agent_labels = { for size in keys(var.agent_pools) : size => {
    "cassandra.jenkins.agent"         = "true"
    "cassandra.jenkins.agent.${size}" = "true"
  } }

  agent_zone_shares = { for size, pool in var.agent_pools : size => [
    for index, zone in local.node_zones : {
      zone = zone
      min_size = floor(min(pool.min_size, local.effective_pool_max[size]) / local.zone_count) + (
        index < min(pool.min_size, local.effective_pool_max[size]) % local.zone_count ? 1 : 0
      )
      max_size = floor(local.effective_pool_max[size] / local.zone_count) + (
        index < local.effective_pool_max[size] % local.zone_count ? 1 : 0
      )
    }
  ] }

  agent_pool_specs = merge([
    for size, shares in local.agent_zone_shares : {
      for share in shares : "agents-${size}-${local.zone_suffix[share.zone]}" => {
        size          = size
        zone          = share.zone
        machine_types = var.agent_pools[size].machine_types
        spot          = var.agent_pools[size].spot
        disk_gb       = var.agent_pools[size].disk_gb
        disk_type     = var.agent_pools[size].disk_type
        labels        = local.agent_labels[size]
        min_size      = share.min_size
        max_size      = share.max_size
      } if share.max_size >= 1
    }
  ]...)

  # Which pools belong to each size, for the outputs layer 3 reads.  A pool is one row there however many
  # zones it spans.
  agent_pool_names = { for size in keys(var.agent_pools) : size => sort([
    for name, spec in local.agent_pool_specs : name if spec.size == size
  ]) }
}

locals {
  # Workload Identity's pool, which is fixed by the project and is the whole of what replaced the sibling's
  # IAM OIDC provider, its thumbprint and the `tls` provider that read it.
  workload_identity_pool = "${var.project}.svc.id.goog"

  external_dns_namespace       = "external-dns"
  external_dns_service_account = "external-dns"

  serves_public_name = var.jenkins_hostname != ""

  external_dns_zones = distinct(concat(
    var.external_dns_zones,
    var.dns_managed_zone == "" ? [] : [var.dns_managed_zone],
  ))
}
