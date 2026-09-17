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
  minimum_quota_demand = { for metric in local.binding_metrics : metric => sum(concat([0], [
    for size, maximum in local.minimum_pool_max : lookup(local.pool_quota_units[size], metric, 0) * maximum
  ])) }
  residual_quota_demand = { for metric in local.binding_metrics : metric => sum(concat([0], [
    for size, maximum in local.residual_pool_max : lookup(local.pool_quota_units[size], metric, 0) * maximum
  ])) }
  allocation_scale = max(0, min(concat(
    [local.pool_scale, (min(local.pod_range_node_ceiling, local.node_range_node_ceiling) - local.minimum_agent_nodes) / local.residual_agent_nodes],
    var.size_pools_to_quotas ? [for metric in local.binding_metrics :
      (local.quota_limits[metric] * var.quota_headroom - local.controller_demand[metric] - local.minimum_quota_demand[metric]) / local.residual_quota_demand[metric]
      if local.residual_quota_demand[metric] > 0
    ] : [],
  )...))
  # Reserve the floors before allocating the remaining quota proportionally.
  effective_pool_max = { for size, pool in var.agent_pools : size =>
    local.minimum_pool_max[size] + floor(local.residual_pool_max[size] * local.allocation_scale)
  }

  effective_agent_nodes = sum(concat([0], values(local.effective_pool_max)))

  effective_quota_demand = { for metric in local.binding_metrics : metric =>
    local.controller_demand[metric] + sum(concat([0], [
      for size, maximum in local.effective_pool_max :
      lookup(local.pool_quota_units[size], metric, 0) * maximum
    ]))
  }

  # Minimum pool sizes must not silently exceed the budget used for proportional scaling.
  pool_limits_fit = (
    local.effective_agent_nodes <= min(local.pod_range_node_ceiling, local.node_range_node_ceiling) &&
    (!var.size_pools_to_quotas || alltrue([
      for metric, demand in local.effective_quota_demand :
      demand <= local.quota_limits[metric] * var.quota_headroom
    ]))
  )
}
