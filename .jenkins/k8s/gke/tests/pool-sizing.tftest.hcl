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

variables { project = "test-project" }
run "scaled_small_pool_keeps_a_worker_slot" {
  command = plan
  assert {
    condition     = local.effective_pool_max.small >= 4
    error_message = "One outer pipeline needs three small workers for its JAR stage."
  }
  assert {
    condition     = local.effective_pool_max.large == 50 && local.effective_pool_max.medium == 34 && local.effective_pool_max.report == 1
    error_message = "The other pools must retain their proportional allocations."
  }
  assert {
    condition     = local.pool_limits_fit && local.effective_agent_nodes == 90
    error_message = "The corrected allocation must fit the supplied quota and address budgets."
  }
}
run "declared_pool_is_unchanged_when_it_fits" {
  command = plan
  variables {
    test_scale = 1
    test_quota = 5000
  }
  assert {
    condition     = local.effective_pool_max.small == var.agent_pools.small.max_size
    error_message = "A fitting pool must retain its declared maximum."
  }
}
run "disabled_small_pool_stays_disabled" {
  command = plan
  variables {
    agent_pools = { small = { machine_types = ["e2-highcpu-8"], max_size = 0 } }
  }
  assert {
    condition     = local.effective_pool_max.small == 0
    error_message = "Explicitly disabled pools must stay disabled."
  }
}

run "minimum_pools_cannot_exceed_cpu_budget" {
  command = plan
  variables {
    test_scale     = 0
    test_quota     = 40
    quota_headroom = 1
  }
  assert {
    condition     = !local.pool_limits_fit
    error_message = "Seven agent slots and a controller cannot fit a 40-vCPU budget at 8 vCPU each."
  }
}

run "minimum_pools_cannot_exceed_address_budget" {
  command = plan
  variables {
    test_scale     = 0
    test_addresses = 6
  }
  assert {
    condition     = !local.pool_limits_fit
    error_message = "The minimum agent slots cannot fit an address range that holds only six nodes."
  }
}

run "minimum_pools_fit_exactly_at_budget" {
  command = plan
  variables {
    test_scale     = 0
    test_quota     = 64
    test_addresses = 7
    quota_headroom = 1
  }
  assert {
    condition     = local.pool_limits_fit && local.effective_agent_nodes == 7
    error_message = "Seven agent slots and a controller fit at 64 vCPU."
  }
}

run "larger_worker_budget_keeps_the_full_jar_stage" {
  command = plan
  variables { small_workers_per_build = 6 }
  assert {
    condition     = local.effective_pool_max.small == 7 && local.pool_limits_fit
    error_message = "Six small workers and one outer pipeline need seven slots after quota scaling."
  }
}

run "minimum_reservation_leaves_a_feasible_cpu_allocation" {
  command = plan
  variables {
    test_scale     = 792 / 3840
    test_quota     = 800
    quota_headroom = 1
  }
  assert {
    condition     = local.pool_limits_fit && local.effective_agent_nodes >= 96 && local.effective_pool_max.small >= 4
    error_message = "The minimum floors must fit inside 800 vCPUs without rejecting a feasible allocation."
  }
}
