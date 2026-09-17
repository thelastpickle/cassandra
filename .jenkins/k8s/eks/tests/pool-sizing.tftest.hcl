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

run "scaled_small_pool_keeps_three_workers" {
  command = plan
  assert {
    condition     = local.effective_pool_max.small == 4
    error_message = "One outer pipeline needs three small workers for its JAR stage."
  }
  assert {
    condition     = local.effective_pool_max.large == 28 && local.effective_pool_max.medium == 19 && local.effective_pool_max.report == 1
    error_message = "Other pools must retain their proportional allocations."
  }
}

run "unscaled_pools_keep_declared_sizes" {
  command = plan
  variables { test_scale = 1 }
  assert {
    condition     = local.effective_pool_max.small == 10 && local.pool_limits_fit
    error_message = "A fitting pool retains its declared size."
  }
}
run "larger_worker_budget" {
  command = plan
  variables { small_workers_per_build = 6 }
  assert {
    condition     = local.effective_pool_max.small == 7 && local.pool_limits_fit
    error_message = "A six-worker budget needs seven small slots."
  }
}
run "disabled_small_pool_stays_disabled" {
  command = plan
  variables { agent_pools = { small = { instance_types = ["c7a.2xlarge"], max_size = 0 } } }
  assert {
    condition     = local.effective_pool_max.small == 0
    error_message = "An explicitly disabled small pool must stay disabled."
  }
}
run "minimum_pools_cannot_exceed_cpu_budget" {
  command = plan
  variables {
    test_scale = 0
    test_vcpus = 63
  }
  assert {
    condition     = !local.pool_limits_fit
    error_message = "Seven agents and a controller require 64 vCPU."
  }
}
run "minimum_pools_cannot_exceed_storage_budget" {
  command = plan
  variables {
    test_scale       = 0
    test_storage_gib = 799
  }
  assert {
    condition     = !local.pool_limits_fit
    error_message = "Seven agents and a controller require 800 GiB of boot disks."
  }
}
run "zone_remainders_cannot_exceed_one_subnet" {
  command = plan
  variables {
    test_scale     = 0
    test_addresses = { a = 35, b = 100 }
  }
  assert {
    condition     = !local.pool_limits_fit
    error_message = "The first zone needs 36 addresses even when the aggregate capacity is ample."
  }
}
run "minimum_pools_fit_at_the_budget" {
  command = plan
  variables {
    test_scale       = 0
    test_vcpus       = 64
    test_storage_gib = 800
    test_addresses   = { a = 36, b = 18 }
  }
  assert {
    condition     = local.pool_limits_fit
    error_message = "The exact CPU, storage and per-zone address budgets must fit."
  }
}
run "unread_quotas_do_not_block_pool_sizing" {
  command = plan
  variables {
    size_pools_to_quotas = false
    test_vcpus           = 0
    test_storage_gib     = 0
  }
  assert {
    condition     = local.pool_limits_fit
    error_message = "Disabling quota reads must not compare allocations against zero."
  }
}
run "addresses_still_bind_without_quota_reads" {
  command = plan
  variables {
    size_pools_to_quotas = false
    test_scale           = 0
    test_addresses       = { a = 35, b = 100 }
  }
  assert {
    condition     = !local.pool_limits_fit
    error_message = "Disabling quota reads must not disable subnet limits."
  }
}
run "two_slots_cannot_hold_three_workers" {
  command = plan
  variables { agent_pools = { small = { instance_types = ["c7a.2xlarge"], max_size = 2 } } }
  expect_failures = [var.agent_pools]
}
run "two_slots_can_hold_one_worker" {
  command = plan
  variables {
    small_workers_per_build = 1
    agent_pools             = { small = { instance_types = ["c7a.2xlarge"], max_size = 2 } }
  }
  assert {
    condition     = local.effective_pool_max.small == 2
    error_message = "A one-worker budget needs two small slots."
  }
}
run "zero_worker_budget_is_rejected" {
  command = plan
  variables { small_workers_per_build = 0 }
  expect_failures = [var.small_workers_per_build]
}
run "fractional_worker_budget_is_rejected" {
  command = plan
  variables { small_workers_per_build = 1.5 }
  expect_failures = [var.small_workers_per_build]
}

run "minimum_reservation_leaves_a_feasible_cpu_allocation" {
  command = plan
  variables {
    test_scale = 792 / 3840
    test_vcpus = 800
  }
  assert {
    condition     = local.pool_limits_fit && local.effective_vcpus >= 768 && local.effective_pool_max.small >= 4
    error_message = "Minimum floors must not overrun the available CPU budget."
  }
}
run "controller_addresses_belong_only_to_its_zone" {
  command = plan
  variables {
    size_pools_to_quotas = false
    test_scale           = 1
    test_addresses       = { a = 251, b = 123 }
    agent_pools = {
      small  = { instance_types = ["m7a.2xlarge"], max_size = 4 }
      medium = { instance_types = ["m7a.2xlarge"], max_size = 16 }
      large  = { instance_types = ["m7a.2xlarge"], max_size = 18 }
      report = { instance_types = ["m7a.2xlarge"], max_size = 2 }
    }
  }
  assert {
    condition     = local.pool_limits_fit && local.effective_pool_max.large == 18
    error_message = "The controller consumes addresses only in the first zone."
  }
}
