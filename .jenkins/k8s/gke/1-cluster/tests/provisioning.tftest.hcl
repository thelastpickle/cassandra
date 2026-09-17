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

mock_provider "google" {
  mock_data "google_project" {
    defaults = { number = "123456789" }
  }
  mock_data "google_compute_zones" {
    defaults = { names = ["us-central1-a", "us-central1-b", "us-central1-c"] }
  }
  mock_data "google_compute_network" {
    defaults = { self_link = "https://www.googleapis.com/compute/v1/projects/test-project/global/networks/default" }
  }
  mock_data "google_compute_subnetwork" {
    defaults = {
      self_link          = "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/subnetworks/default"
      ip_cidr_range      = "10.0.0.0/20"
      secondary_ip_range = []
    }
  }
  mock_resource "google_service_account" {
    defaults = { email = "test-account@test-project.iam.gserviceaccount.com" }
  }
  mock_resource "google_storage_bucket_object" {
    defaults = { generation = 1 }
  }
}

mock_provider "archive" {
  mock_data "archive_file" {
    defaults = {
      output_base64sha256 = "3OgNQvWH04oeow7vp5OKmCV1/OMO0nyMBp5wh9ckdik="
    }
  }
}
mock_provider "external" {
  mock_data "external" {
    defaults = { result = { CPUS = "800", N2_CPUS = "800", CPUS_ALL_REGIONS = "32", SSD_TOTAL_GB = "100000" } }
  }
}

override_resource {
  target = google_service_account.spend_build
  values = {
    name  = "projects/test-project/serviceAccounts/spend_build@test-project.iam.gserviceaccount.com"
    email = "spend_build@test-project.iam.gserviceaccount.com"
  }
}

override_resource {
  target = google_service_account.spend_guard
  values = {
    name  = "projects/test-project/serviceAccounts/spend_guard@test-project.iam.gserviceaccount.com"
    email = "spend_guard@test-project.iam.gserviceaccount.com"
  }
}

override_resource {
  target = google_service_account.spend_scheduler
  values = {
    name  = "projects/test-project/serviceAccounts/spend_scheduler@test-project.iam.gserviceaccount.com"
    email = "spend_scheduler@test-project.iam.gserviceaccount.com"
  }
}

variables {
  project               = "test-project"
  region                = "us-central1"
  cluster_name          = "test-jenkins"
  kubernetes_version    = "1.34"
  size_pools_to_quotas  = false
  spend_cap_daily_usd   = 100
  spend_cap_weekly_usd  = null
  spend_cap_monthly_usd = null
  spend_alert_email     = ""
  billing_account       = ""
  jenkins_hostname      = ""
  enable_external_dns   = false
  log_retention_days    = 30
}

run "cluster_satisfies_system_binding_policy" {
  command = plan
  assert {
    condition = (
      google_container_cluster.this.rbac_binding_config[0].enable_insecure_binding_system_authenticated == false &&
      google_container_cluster.this.rbac_binding_config[0].enable_insecure_binding_system_unauthenticated == false
    )
    error_message = "The cluster must prohibit non-default bindings to the broad system identities."
  }
}

run "heartbeat_descriptor_exists_before_first_evaluation" {
  command = plan
  assert {
    condition = (
      google_monitoring_metric_descriptor.spend_guard_evaluation[0].type == "custom.googleapis.com/cassandra_jenkins/spend_guard/evaluation_ok" &&
      google_monitoring_metric_descriptor.spend_guard_evaluation[0].metric_kind == "GAUGE" &&
      google_monitoring_metric_descriptor.spend_guard_evaluation[0].value_type == "DOUBLE" &&
      one(google_monitoring_metric_descriptor.spend_guard_evaluation[0].labels).key == "cluster"
    )
    error_message = "The heartbeat metric must exist at provision time and match the guard's time-series payload."
  }
}

run "no_spend_cap_creates_no_heartbeat_descriptor" {
  command = plan
  variables {
    spend_cap_daily_usd = null
  }
  assert {
    condition     = length(google_monitoring_metric_descriptor.spend_guard_evaluation) == 0
    error_message = "A cluster without spend caps must not create spend-guard monitoring resources."
  }
  assert {
    condition = (
      length(google_service_account.spend_build) == 0 &&
      length(google_project_iam_member.spend_build) == 0
    )
    error_message = "A cluster without spend caps must not create a build account or grant build permissions."
  }
}

run "project_managed_retention_keeps_cluster_logging" {
  command = plan
  variables {
    log_retention_days = null
  }
  assert {
    condition = (
      length(google_logging_project_bucket_config.default) == 0 &&
      contains(google_container_cluster.this.logging_config[0].enable_components, "APISERVER")
    )
    error_message = "Project-managed retention must omit bucket management while retaining cluster log collection."
  }
}

run "explicit_retention_is_managed" {
  command = plan
  variables {
    log_retention_days = 14
  }
  assert {
    condition     = google_logging_project_bucket_config.default[0].retention_days == 14
    error_message = "An explicit retention period must configure the log bucket."
  }
}

run "scheduler_defaults_to_cluster_region" {
  command = plan
  assert {
    condition     = google_cloud_scheduler_job.spend_guard[0].region == "us-central1"
    error_message = "Without an override, the schedule must remain in the cluster region."
  }
}

# These applies use mocked providers and write only the test's temporary state.
run "source_uploaded_before_function_build" {
  command = apply
  assert {
    condition = (
      google_cloudfunctions2_function.spend_guard[0].build_config[0].service_account == google_service_account.spend_build[0].name &&
      google_cloudfunctions2_function.spend_guard[0].build_config[0].service_account != google_service_account.spend_guard[0].name
    )
    error_message = "The function must use an explicit build account separate from its runtime account."
  }
  assert {
    condition = (
      toset([for grant in google_project_iam_member.spend_build : grant.role]) == toset([
        "roles/logging.logWriter", "roles/artifactregistry.writer", "roles/storage.objectViewer"
      ]) &&
      alltrue([for grant in google_project_iam_member.spend_build :
        grant.project == "test-project" && grant.member == "serviceAccount:${google_service_account.spend_build[0].email}"
      ])
    )
    error_message = "The build account must be able to read source objects, write images and write logs in the function's project."
  }
  assert {
    condition = (
      google_storage_bucket_object.spend_guard[0].generation == 1 &&
      google_cloudfunctions2_function.spend_guard[0].build_config[0].source[0].storage_source[0].generation == 1
    )
    error_message = "The function build must select the generation of the uploaded source object."
  }
}

run "bucket_replacement_uploads_source_again" {
  command = apply
  plan_options {
    replace = [google_storage_bucket.spend_guard[0]]
  }
  override_resource {
    target = google_storage_bucket_object.spend_guard
    values = { generation = 2 }
  }
  assert {
    condition = (
      google_storage_bucket_object.spend_guard[0].generation == 2 &&
      google_cloudfunctions2_function.spend_guard[0].build_config[0].source[0].storage_source[0].generation == 2
    )
    error_message = "Replacing the bucket under the same name must upload the source again and update the function's source generation."
  }
}

run "scheduler_can_invoke_guard_in_another_region" {
  command = apply
  variables {
    region                 = "europe-north1"
    spend_scheduler_region = "europe-west1"
  }
  override_data {
    target = data.google_compute_zones.available
    values = { names = ["europe-north1-a", "europe-north1-b", "europe-north1-c"] }
  }
  override_data {
    target = data.google_compute_subnetwork.selected
    values = {
      self_link          = "https://www.googleapis.com/compute/v1/projects/test-project/regions/europe-north1/subnetworks/default"
      ip_cidr_range      = "10.0.0.0/20"
      secondary_ip_range = []
    }
  }
  assert {
    condition = (
      google_cloud_scheduler_job.spend_guard[0].region == "europe-west1" &&
      google_container_cluster.this.location == "europe-north1" &&
      google_cloudfunctions2_function.spend_guard[0].location == "europe-north1" &&
      google_cloud_run_service_iam_member.spend_guard_invoker[0].location == "europe-north1"
    )
    error_message = "The scheduler region override must preserve the cluster, function and invoker binding locations."
  }
  assert {
    condition = (
      google_cloud_scheduler_job.spend_guard[0].http_target[0].uri == google_cloudfunctions2_function.spend_guard[0].service_config[0].uri &&
      google_cloud_scheduler_job.spend_guard[0].http_target[0].oidc_token[0].audience == google_cloudfunctions2_function.spend_guard[0].service_config[0].uri &&
      google_cloud_scheduler_job.spend_guard[0].http_target[0].oidc_token[0].service_account_email == google_service_account.spend_scheduler[0].email
    )
    error_message = "A schedule in another region must still authenticate to the guard's URL as the invoker account."
  }
  assert {
    condition = (
      output.spend_guard.scheduler_region == "europe-west1" &&
      contains(split("\n", output.environment), "export GKE_SPEND_GUARD_SCHEDULER_REGION='europe-west1'") &&
      contains(split("\n", output.environment), "export GKE_LOCATION='europe-north1'") &&
      strcontains(google_monitoring_alert_policy.spend_guard_stalled[0].documentation[0].content,
      "gcloud scheduler jobs describe test-jenkins-spend-guard --location europe-west1")
    )
    error_message = "Operator outputs must distinguish the schedule's region from the cluster's location."
  }
}

run "alerts_cover_initial_silence_and_cap_events" {
  command = plan
  variables { spend_alert_email = "ci@example.org" }
  assert {
    condition = (
      startswith(google_monitoring_alert_policy.spend_guard_stalled[0].conditions[0].condition_prometheus_query_language[0].query, "absent_over_time(") &&
      length(google_monitoring_alert_policy.spend_guard_action[0].notification_channels) == 1 &&
      contains(google_monitoring_alert_policy.spend_guard_action[0].alert_strategy[0].notification_prompts, "CLOSED") &&
      strcontains(google_monitoring_alert_policy.spend_guard_action[0].conditions[0].condition_prometheus_query_language[0].query, "/cap_exceeded") &&
      length(google_monitoring_alert_policy.spend_guard_stalled[0].notification_channels) == 1 &&
      google_secret_manager_secret_iam_member.spend_state_write[0].role == "roles/secretmanager.secretVersionManager"
    )
    error_message = "Initial silence, cap events and state retention must have active wiring."
  }
}

run "global_cpu_quota_bounds_the_actual_plan" {
  command = plan
  variables {
    size_pools_to_quotas = true
    quota_headroom       = 1
    agent_pools          = { large = { machine_types = ["n2-standard-8"], max_size = 100, spot = false } }
  }
  assert {
    condition     = local.pool_limits_fit && local.effective_agent_nodes <= 3 && local.effective_quota_demand.CPUS_ALL_REGIONS <= 32
    error_message = "Regional capacity cannot bypass a smaller global CPU quota."
  }
}
