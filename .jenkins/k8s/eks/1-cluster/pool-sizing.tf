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
  minimum_pool_max = { for size, pool in var.agent_pools : size =>
    pool.max_size == 0 ? 0 : (size == "small" ? var.small_workers_per_build + 1 : 1)
  }
  residual_pool_max    = { for size, pool in var.agent_pools : size => max(0, pool.max_size - local.minimum_pool_max[size]) }
  minimum_agent_nodes  = sum(concat([0], values(local.minimum_pool_max)))
  residual_agent_nodes = max(1, sum(concat([0], values(local.residual_pool_max))))
  minimum_agent_vcpus  = sum(concat([0], [for size, maximum in local.minimum_pool_max : maximum * local.declared_vcpus_per_node[size]]))
  minimum_agent_gib    = sum(concat([0], [for size, maximum in local.minimum_pool_max : maximum * var.agent_pools[size].disk_gib]))
  residual_agent_vcpus = max(1, sum(concat([0], [for size, maximum in local.residual_pool_max : maximum * local.declared_vcpus_per_node[size]])))
  residual_agent_gib   = max(1, sum(concat([0], [for size, maximum in local.residual_pool_max : maximum * var.agent_pools[size].disk_gib])))
  allocation_scale = max(0, min(concat(
    [local.pool_scale],
    var.size_pools_to_quotas ? [
      (local.claimable_vcpus - local.controller_vcpus - local.minimum_agent_vcpus) / local.residual_agent_vcpus,
      (local.claimable_storage_gib - local.controller_gib - local.minimum_agent_gib) / local.residual_agent_gib,
    ] : [],
    [(local.zone_count * min(values(local.zone_address_capacity)...) - local.controller_addresses - local.minimum_agent_nodes * local.addresses_per_node) / (local.residual_agent_nodes * local.addresses_per_node)],
  )...))
  # Reserve the floors before allocating the remaining quota proportionally.
  effective_pool_max = { for size, pool in var.agent_pools : size =>
    local.minimum_pool_max[size] + floor(local.residual_pool_max[size] * local.allocation_scale)
  }

  agent_zone_shares = { for size, pool in var.agent_pools : size => [
    for index, zone in local.node_group_zones : {
      zone         = zone
      min_size     = floor(min(pool.min_size, local.effective_pool_max[size]) / local.zone_count) + (index < min(pool.min_size, local.effective_pool_max[size]) % local.zone_count ? 1 : 0)
      max_size     = floor(local.effective_pool_max[size] / local.zone_count) + (index < local.effective_pool_max[size] % local.zone_count ? 1 : 0)
      desired_size = floor(min(pool.desired_size, local.effective_pool_max[size]) / local.zone_count) + (index < min(pool.desired_size, local.effective_pool_max[size]) % local.zone_count ? 1 : 0)
    }
  ] }

  effective_vcpus = local.controller_vcpus + sum(concat([0], [
    for size, maximum in local.effective_pool_max : maximum * local.declared_vcpus_per_node[size]
  ]))
  effective_storage_gib = local.controller_gib + sum(concat([0], [
    for size, maximum in local.effective_pool_max : maximum * var.agent_pools[size].disk_gib
  ]))

  # Zone remainders can put more nodes in the first subnet than an aggregate budget suggests.
  pool_limits_fit = (
    (!var.size_pools_to_quotas || (
      local.effective_vcpus <= local.claimable_vcpus &&
      local.effective_storage_gib <= local.claimable_storage_gib
      )) && alltrue([
      for index, zone in local.node_group_zones :
      (zone == local.controller_zone ? local.controller_addresses : 0) + local.addresses_per_node * sum(concat([0], [
        for shares in values(local.agent_zone_shares) : shares[index].max_size
      ])) <= local.zone_address_capacity[zone]
    ])
  )
}
