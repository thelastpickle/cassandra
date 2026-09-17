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

variables {
  project = "test-project"
}

run "one_small_slot_is_rejected" {
  command = plan
  variables {
    agent_pools = { small = { machine_types = ["e2-highcpu-8"], max_size = 1 } }
  }
  expect_failures = [var.agent_pools]
}

run "agent_disks_cover_the_pod_storage_limits" {
  command = plan

  # Each committed agent template allows 80Gi. Apply GKE's published boot-disk
  # reservation and a conservative 10% allowance for the COS filesystem layout.
  assert {
    condition = alltrue([for pool in values(var.agent_pools) :
      0.81 * pool.disk_gb - min(0.5 * pool.disk_gb, 6 + 0.35 * pool.disk_gb, 100) >= 80
    ])
    error_message = "Default agent boot disks must leave room for the pod templates' 80Gi storage limits."
  }
}

run "reported_cluster_name_rejected_with_spend_cap" {
  command = plan
  variables {
    cluster_name        = "mck--cassandra-jenkins"
    spend_cap_daily_usd = 100
  }
  expect_failures = [var.cluster_name]
}

run "spend_accounts_at_30_characters" {
  command = plan
  variables {
    cluster_name        = "abcdefghijklmnopqr"
    spend_cap_daily_usd = 100
  }
}

run "spend_accounts_over_30_characters" {
  command = plan
  variables {
    cluster_name        = "abcdefghijklmnopqrs"
    spend_cap_daily_usd = 100
  }
  expect_failures = [var.cluster_name]
}

run "weekly_cap_also_limits_name" {
  command = plan
  variables {
    cluster_name         = "abcdefghijklmnopqrs"
    spend_cap_weekly_usd = 100
  }
  expect_failures = [var.cluster_name]
}

run "monthly_cap_also_limits_name" {
  command = plan
  variables {
    cluster_name          = "abcdefghijklmnopqrs"
    spend_cap_monthly_usd = 100
  }
  expect_failures = [var.cluster_name]
}

run "external_dns_account_at_30_characters" {
  command = plan
  variables {
    cluster_name        = "abcdefghijklmnopq"
    enable_external_dns = true
  }
}

run "external_dns_account_over_30_characters" {
  command = plan
  variables {
    cluster_name        = "abcdefghijklmnopqr"
    enable_external_dns = true
  }
  expect_failures = [var.cluster_name]
}

run "node_account_at_30_characters_without_optional_accounts" {
  command = plan
  variables {
    cluster_name = "abcdefghijklmnopqrstuvwxy"
  }
}

run "node_account_over_30_characters" {
  command = plan
  variables {
    cluster_name = "abcdefghijklmnopqrstuvwxyz"
  }
  expect_failures = [var.cluster_name]
}

run "node_account_at_six_characters" {
  command = plan
  variables {
    cluster_name = "a"
  }
}

run "empty_cluster_name_rejected" {
  command = plan
  variables {
    cluster_name = ""
  }
  expect_failures = [var.cluster_name]
}

run "default_logging_uses_apiserver" {
  command = plan
  assert {
    condition     = contains(var.logging_components, "APISERVER")
    error_message = "Default control-plane logging must include APISERVER."
  }
}

run "apiserver_logging_accepted" {
  command = plan
  variables {
    logging_components = ["SYSTEM_COMPONENTS", "APISERVER"]
  }
}

run "api_server_logging_rejected" {
  command = plan
  variables {
    logging_components = ["SYSTEM_COMPONENTS", "API_SERVER"]
  }
  expect_failures = [var.logging_components]
}

run "project_managed_log_retention" {
  command = plan
  variables {
    log_retention_days = null
  }
}

run "invalid_log_retention_rejected" {
  command = plan
  variables {
    log_retention_days = 0
  }
  expect_failures = [var.log_retention_days]
}

run "separate_scheduler_region_accepted" {
  command = plan
  variables {
    region                 = "europe-north1"
    spend_scheduler_region = "europe-west1"
  }
}

run "scheduler_zone_rejected" {
  command = plan
  variables {
    spend_scheduler_region = "europe-west1-b"
  }
  expect_failures = [var.spend_scheduler_region]
}

run "empty_scheduler_region_rejected" {
  command = plan
  variables {
    spend_scheduler_region = ""
  }
  expect_failures = [var.spend_scheduler_region]
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
run "two_slots_cannot_hold_a_pipeline_and_three_workers" {
  command = plan
  variables {
    agent_pools = { small = { machine_types = ["e2-highcpu-8"], max_size = 2 } }
  }
  expect_failures = [var.agent_pools]
}
run "two_slots_can_hold_one_pipeline_and_its_worker" {
  command = plan
  variables {
    small_workers_per_build = 1
    agent_pools             = { small = { machine_types = ["e2-highcpu-8"], max_size = 2 } }
  }
}

run "reject_dimensioned_cpu_quota" {
  command = plan
  variables {
    agent_pools = { large = { machine_types = ["n4-standard-8"], max_size = 10 } }
  }
  expect_failures = [var.agent_pools]
}
