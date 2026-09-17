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
  spend_caps = {
    daily   = var.spend_cap_daily_usd
    weekly  = var.spend_cap_weekly_usd
    monthly = var.spend_cap_monthly_usd
  }

  spend_guard_enabled = length([for cap in values(local.spend_caps) : cap if cap != null]) > 0

  spend_guard_name = "${var.cluster_name}-spend-guard"

  # Cluster-scoped, so two clusters in one project do not share caps or kept figures.  A project per cluster is
  # the recommendation anyway, and this costs one interpolation.
  spend_caps_secret  = "${var.cluster_name}-spend-caps"
  spend_state_secret = "${var.cluster_name}-spend-state"

  spend_metric_prefix = "custom.googleapis.com/cassandra_jenkins/spend_guard"

  # The pools the brake acts on: every agent pool, and deliberately never the controller's.  That one holds the
  # build queue and jenkins_home.
  spend_agent_pool_names = sort(keys(local.agent_pool_specs))

  spend_agent_pool_maxima = { for name, spec in local.agent_pool_specs : name => spec.max_size }

  spend_billing_dataset = var.spend_billing_table == "" ? null : {
    project = split(".", replace(var.spend_billing_table, ":", "."))[0]
    dataset = split(".", replace(var.spend_billing_table, ":", "."))[1]
  }
}

# Two numbers rather than a configuration fault, so this warns and does not refuse: a wider window with a
# smaller cap makes the narrower cap unreachable, which is worth saying and is not worth stopping an apply for.
check "spend_caps_are_ordered" {
  assert {
    condition     = var.spend_cap_weekly_usd == null || var.spend_cap_daily_usd == null || var.spend_cap_weekly_usd >= var.spend_cap_daily_usd
    error_message = "The weekly cap is below the daily one, so the daily cap can never be met: a day is inside a week."
  }

  assert {
    condition     = var.spend_cap_monthly_usd == null || var.spend_cap_weekly_usd == null || var.spend_cap_monthly_usd >= var.spend_cap_weekly_usd
    error_message = "The monthly cap is below the weekly one, so the weekly cap can never be met: a week is inside a month, give or take its edges."
  }
}

resource "google_project_service" "spend_guard" {
  for_each = local.spend_guard_enabled ? toset([
    "cloudfunctions.googleapis.com",
    "cloudbuild.googleapis.com",
    "run.googleapis.com",
    "cloudscheduler.googleapis.com",
    "secretmanager.googleapis.com",
    "pubsub.googleapis.com",
    "artifactregistry.googleapis.com",
  ]) : toset([])

  project = var.project
  service = each.key

  disable_on_destroy = false
}

# One secret holding all three caps as JSON, not three secrets, for the sibling's reason: three reads can see
# two values from before an edit and one from after.
resource "google_secret_manager_secret" "spend_caps" {
  count = local.spend_guard_enabled ? 1 : 0

  secret_id = local.spend_caps_secret

  replication {
    auto {}
  }

  labels = var.labels

  depends_on = [google_project_service.spend_guard]
}

resource "google_secret_manager_secret_version" "spend_caps" {
  count = local.spend_guard_enabled ? 1 : 0

  secret      = one(google_secret_manager_secret.spend_caps[*].id)
  secret_data = jsonencode(local.spend_caps)
}

resource "google_secret_manager_secret" "spend_state" {
  count = local.spend_guard_enabled ? 1 : 0

  secret_id = local.spend_state_secret

  replication {
    auto {}
  }

  labels = var.labels

  depends_on = [google_project_service.spend_guard]
}

resource "google_secret_manager_secret_version" "spend_state" {
  count = local.spend_guard_enabled ? 1 : 0

  secret = one(google_secret_manager_secret.spend_state[*].id)

  secret_data = jsonencode({
    version         = 1
    cost_as_of      = null
    sample_interval = null
    cost_by_day     = {}
    pool_maxima     = {}
  })

  lifecycle {
    # Without this, every plan after an evaluation proposes replacing the guard's kept figures with an empty
    # document, which throws away the days a fit is made from.
    ignore_changes = [secret_data]
  }
}

resource "google_pubsub_topic" "spend_alerts" {
  count = local.spend_guard_enabled ? 1 : 0

  name   = "${var.cluster_name}-spend-alerts"
  labels = var.labels

  depends_on = [google_project_service.spend_guard]
}

resource "google_monitoring_notification_channel" "spend_email" {
  count = local.spend_guard_enabled && var.spend_alert_email != "" ? 1 : 0

  display_name = "${var.cluster_name} spend"
  type         = "email"

  labels = {
    email_address = var.spend_alert_email
  }
}

resource "google_service_account" "spend_guard" {
  count = local.spend_guard_enabled ? 1 : 0

  account_id   = "${var.cluster_name}-spend-guard"
  display_name = "Spend guard for the ${var.cluster_name} Jenkins cluster"
  description  = "Reads usage and cost, and sets the agent node pools' autoscaling maximum to zero when a cap is met."

  depends_on = [google_project_service.spend_guard]
}

resource "google_project_iam_member" "spend_guard" {
  for_each = local.spend_guard_enabled ? toset(compact([
    # Reading the vCPU metric, and reading back its own custom metrics.
    "roles/monitoring.viewer",
    "roles/monitoring.metricWriter",
    # Asking whether any instance is running, which is the discriminator between "the metric is empty because
    # nothing is running" and "the metric is empty because something is wrong".  Read-only.
    "roles/compute.viewer",
    "roles/container.clusterAdmin",
    # Its own logs.  Every evaluation prints its whole report, so the figures behind a decision outlive it.
    "roles/logging.logWriter",
    # Running a query against the billing export, when there is one.  The dataset-scoped read is granted
    # separately below; this is the permission to run a job at all, which is a project-level thing.
    var.spend_billing_table == "" ? "" : "roles/bigquery.jobUser",
  ])) : toset([])

  project = var.project
  role    = each.key
  member  = "serviceAccount:${one(google_service_account.spend_guard[*].email)}"
}

# The caps and the kept figures.  Read on both, and add-a-version on the state alone, because the guard must
# never rewrite its own caps: those are the operator's answer.
resource "google_secret_manager_secret_iam_member" "spend_caps_read" {
  count = local.spend_guard_enabled ? 1 : 0

  secret_id = one(google_secret_manager_secret.spend_caps[*].secret_id)
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${one(google_service_account.spend_guard[*].email)}"
}

resource "google_secret_manager_secret_iam_member" "spend_state_read" {
  count = local.spend_guard_enabled ? 1 : 0

  secret_id = one(google_secret_manager_secret.spend_state[*].secret_id)
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${one(google_service_account.spend_guard[*].email)}"
}

resource "google_secret_manager_secret_iam_member" "spend_state_write" {
  count = local.spend_guard_enabled ? 1 : 0

  secret_id = one(google_secret_manager_secret.spend_state[*].secret_id)
  role      = "roles/secretmanager.secretVersionManager"
  member    = "serviceAccount:${one(google_service_account.spend_guard[*].email)}"
}

resource "google_pubsub_topic_iam_member" "spend_guard" {
  count = local.spend_guard_enabled ? 1 : 0

  topic  = one(google_pubsub_topic.spend_alerts[*].name)
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${one(google_service_account.spend_guard[*].email)}"
}

# Reading the billing export, scoped to the one dataset that holds it rather than to the project.  The dataset
# may be in another project, which is why its project is parsed out of the table name rather than assumed.
resource "google_bigquery_dataset_iam_member" "spend_guard" {
  count = local.spend_guard_enabled && var.spend_billing_table != "" ? 1 : 0

  project    = local.spend_billing_dataset.project
  dataset_id = local.spend_billing_dataset.dataset
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${one(google_service_account.spend_guard[*].email)}"
}

# The default build account can lack permissions in projects that disable automatic IAM grants.
# Keep the build separate from the runtime account, which can change node pools and read secrets.
resource "google_service_account" "spend_build" {
  count = local.spend_guard_enabled ? 1 : 0

  account_id   = "${var.cluster_name}-spend-build"
  display_name = "Spend guard builder for the ${var.cluster_name} Jenkins cluster"
  description  = "Builds the spend guard image from its uploaded source."

  depends_on = [google_project_service.spend_guard]
}

# Cloud Functions copies the source into its own buckets and creates the gcf-artifacts repository.
# These project grants cover those build resources as well as the source bucket below.
resource "google_project_iam_member" "spend_build" {
  for_each = local.spend_guard_enabled ? toset([
    "roles/logging.logWriter",
    "roles/artifactregistry.writer",
    "roles/storage.objectViewer",
  ]) : toset([])

  project = var.project
  role    = each.key
  member  = "serviceAccount:${one(google_service_account.spend_build[*].email)}"
}

data "archive_file" "spend_guard" {
  count = local.spend_guard_enabled ? 1 : 0

  type        = "zip"
  output_path = "${path.module}/.terraform/spend-guard.zip"

  source {
    content  = file("${path.module}/../spend-guard.py")
    filename = "main.py"
  }

  source {
    content  = file("${path.module}/../../shared/spend_model.py")
    filename = "spend_model.py"
  }
}

resource "google_storage_bucket" "spend_guard" {
  count = local.spend_guard_enabled ? 1 : 0

  name     = "${var.project}-${var.cluster_name}-spend-guard"
  location = var.region

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true

  labels = var.labels

  depends_on = [google_project_service.spend_guard]
}

resource "google_storage_bucket_object" "spend_guard" {
  count = local.spend_guard_enabled ? 1 : 0

  bucket = one(google_storage_bucket.spend_guard[*].name)

  name   = "spend-guard-${one(data.archive_file.spend_guard[*].output_base64sha256)}.zip"
  source = one(data.archive_file.spend_guard[*].output_path)

  lifecycle {
    # A bucket can be replaced under the same name, deleting the object without changing its inputs.
    replace_triggered_by = [google_storage_bucket.spend_guard[count.index]]
  }
}

resource "google_cloudfunctions2_function" "spend_guard" {
  count = local.spend_guard_enabled ? 1 : 0

  name        = local.spend_guard_name
  location    = var.region
  description = "Compares what ${var.project} has spent against the caps, and stops the agent pools when one is met."

  build_config {
    service_account = one(google_service_account.spend_build[*].name)

    # Pinned to a minor, unlike everything else here, because a runtime is retired on a published date and an
    # unpinned one does not exist.
    runtime = "python312"

    entry_point = "guard"

    source {
      storage_source {
        bucket     = one(google_storage_bucket.spend_guard[*].name)
        object     = one(google_storage_bucket_object.spend_guard[*].name)
        generation = one(google_storage_bucket_object.spend_guard[*].generation)
      }
    }
  }

  service_config {
    # 256Mi and two minutes, matching the sibling: the work is a handful of API calls and a least-squares fit
    # over at most a fortnight of days.
    available_memory = "256Mi"
    timeout_seconds  = 120

    max_instance_count = 1

    # None kept warm.  An evaluation every few minutes does not justify a running instance, and a cold start of
    # a few seconds is nothing against a five-minute interval.
    min_instance_count = 0

    service_account_email = one(google_service_account.spend_guard[*].email)

    ingress_settings               = "ALLOW_ALL"
    all_traffic_on_latest_revision = true

    environment_variables = {
      GKE_CLUSTER_NAME                  = var.cluster_name
      GOOGLE_PROJECT                    = var.project
      GOOGLE_REGION                     = var.region
      GKE_LOCATION                      = var.region
      GKE_SPEND_CAPS_SECRET             = local.spend_caps_secret
      GKE_SPEND_STATE_SECRET            = local.spend_state_secret
      GKE_SPEND_STATE_BOOTSTRAP_VERSION = one(google_secret_manager_secret_version.spend_state[*].version)
      GKE_SPEND_AGENT_POOL_NAMES        = join(" ", local.spend_agent_pool_names)
      GKE_SPEND_AGENT_POOL_MAXIMA       = jsonencode(local.spend_agent_pool_maxima)
      GKE_SPEND_ALERT_TOPIC             = one(google_pubsub_topic.spend_alerts[*].name)
      GKE_SPEND_METRIC_PREFIX           = local.spend_metric_prefix
      GKE_SPEND_BILLING_TABLE           = var.spend_billing_table
      GKE_SPEND_PRICE_PER_VCPU_HOUR     = tostring(var.spend_price_per_vcpu_hour)
      GKE_SPEND_FIXED_USD_PER_DAY       = tostring(var.spend_fixed_usd_per_day)
      GKE_SPEND_COST_REFRESH_HOURS      = tostring(var.spend_cost_refresh_hours)
      GKE_SPEND_GUARD_INTERVAL_MINUTES  = tostring(var.spend_guard_interval_minutes)
    }
  }

  labels = var.labels

  depends_on = [
    google_project_iam_member.spend_build,
    google_project_iam_member.spend_guard,
    google_secret_manager_secret_iam_member.spend_caps_read,
    google_secret_manager_secret_iam_member.spend_state_read,
    google_secret_manager_secret_iam_member.spend_state_write,
    google_pubsub_topic_iam_member.spend_guard,
    google_monitoring_metric_descriptor.spend_guard_evaluation,
    google_monitoring_metric_descriptor.spend_guard_braked,
    google_monitoring_metric_descriptor.spend_guard_cap,
  ]
}

resource "google_service_account" "spend_scheduler" {
  count = local.spend_guard_enabled ? 1 : 0

  account_id   = "${var.cluster_name}-spend-sched"
  display_name = "Invokes the ${var.cluster_name} spend guard"

  depends_on = [google_project_service.spend_guard]
}

resource "google_cloud_run_service_iam_member" "spend_guard_invoker" {
  count = local.spend_guard_enabled ? 1 : 0

  project  = var.project
  location = var.region
  service  = one(google_cloudfunctions2_function.spend_guard[*].name)
  role     = "roles/run.invoker"
  member   = "serviceAccount:${one(google_service_account.spend_scheduler[*].email)}"
}

resource "google_cloud_scheduler_job" "spend_guard" {
  count = local.spend_guard_enabled ? 1 : 0

  name        = local.spend_guard_name
  region      = coalesce(var.spend_scheduler_region, var.region)
  description = "Runs the ${var.cluster_name} spend guard every ${var.spend_guard_interval_minutes} minutes."

  # UTC, because the caps' windows are UTC: a day that begins at midnight somewhere else is a different day
  # from the one the guard measures.
  schedule  = "*/${var.spend_guard_interval_minutes} * * * *"
  time_zone = "UTC"

  # One attempt.  A retry would evaluate the same figures again a moment later, which the next scheduled run
  # does anyway, and a failed evaluation that keeps retrying is a failed evaluation charged several times.
  retry_config {
    retry_count = 0
  }

  http_target {
    uri         = one(google_cloudfunctions2_function.spend_guard[*].service_config[0].uri)
    http_method = "POST"

    oidc_token {
      service_account_email = one(google_service_account.spend_scheduler[*].email)
      # The audience is the function's own address, which is what Cloud Run checks the token against.
      audience = one(google_cloudfunctions2_function.spend_guard[*].service_config[0].uri)
    }
  }

  depends_on = [google_cloud_run_service_iam_member.spend_guard_invoker]
}

# The alert is created before the guard has published its first point.
resource "google_monitoring_metric_descriptor" "spend_guard_evaluation" {
  count = local.spend_guard_enabled ? 1 : 0

  type         = "${local.spend_metric_prefix}/evaluation_ok"
  metric_kind  = "GAUGE"
  value_type   = "DOUBLE"
  unit         = "1"
  display_name = "Spend guard evaluation"
  description  = "One point per spend-guard evaluation: 1 when spend is known, 0 when it is unknown."

  labels {
    key         = "cluster"
    value_type  = "STRING"
    description = "GKE cluster name."
  }

  depends_on = [google_project_service.required]
}

resource "google_monitoring_alert_policy" "spend_guard_stalled" {
  count = local.spend_guard_enabled ? 1 : 0

  display_name = "${var.cluster_name} spend guard has stopped reporting"
  combiner     = "OR"

  conditions {
    display_name = "no evaluation for ${max(10, var.spend_guard_interval_minutes * 6)} minutes"

    condition_prometheus_query_language {
      query               = "absent_over_time({__name__=\"${one(google_monitoring_metric_descriptor.spend_guard_evaluation[*].type)}\", monitored_resource=\"global\", project_id=\"${var.project}\", cluster=\"${var.cluster_name}\"}[${var.spend_guard_interval_minutes * 2}m])"
      duration            = "${max(10, var.spend_guard_interval_minutes * 4) * 60}s"
      evaluation_interval = "60s"
    }
  }

  notification_channels = compact([one(google_monitoring_notification_channel.spend_email[*].id)])

  documentation {
    content   = <<-EOT
      The spend guard for ${var.cluster_name} has not reported for
      ${max(10, var.spend_guard_interval_minutes * 6)} minutes, so nothing is watching what this project
      spends and the agent pools are running unwatched.

          gcloud functions describe ${local.spend_guard_name} --region ${var.region} --gen2
          gcloud scheduler jobs describe ${local.spend_guard_name} --location ${coalesce(var.spend_scheduler_region, var.region)}
          gcloud logging read 'resource.labels.service_name="${local.spend_guard_name}"' --freshness=1h --limit=50
          make -C .jenkins/k8s/gke spend

      A paused scheduler job is the failure that shows as an error nowhere in the console.
    EOT
    mime_type = "text/markdown"
  }

  depends_on = [google_project_service.required]
}

resource "google_billing_budget" "spend" {
  count = local.spend_guard_enabled && var.billing_account != "" && var.spend_cap_monthly_usd != null ? 1 : 0

  billing_account = var.billing_account
  display_name    = "${var.cluster_name} monthly"

  budget_filter {
    projects               = ["projects/${data.google_project.this.number}"]
    calendar_period        = "MONTH"
    credit_types_treatment = "INCLUDE_ALL_CREDITS"
  }

  amount {
    specified_amount {
      currency_code = "USD"
      units         = tostring(floor(var.spend_cap_monthly_usd))
    }
  }

  threshold_rules {
    threshold_percent = 0.8
    spend_basis       = "CURRENT_SPEND"
  }

  threshold_rules {
    threshold_percent = 1.0
    spend_basis       = "CURRENT_SPEND"
  }

  all_updates_rule {
    pubsub_topic                     = one(google_pubsub_topic.spend_alerts[*].id)
    schema_version                   = "1.0"
    monitoring_notification_channels = compact([one(google_monitoring_notification_channel.spend_email[*].id)])
  }
}

resource "google_monitoring_metric_descriptor" "spend_guard_braked" {
  count        = local.spend_guard_enabled ? 1 : 0
  type         = "${local.spend_metric_prefix}/braked"
  metric_kind  = "GAUGE"
  value_type   = "DOUBLE"
  unit         = "1"
  display_name = "Spend guard brake"
  labels {
    key        = "cluster"
    value_type = "STRING"
  }
  depends_on = [google_project_service.required]
}

resource "google_monitoring_alert_policy" "spend_guard_action" {
  count        = local.spend_guard_enabled ? 1 : 0
  display_name = "${var.cluster_name} spend guard cap or evaluation alert"
  combiner     = "OR"
  conditions {
    display_name = "Cap exceeded, braked or unknown spend"
    condition_prometheus_query_language {
      query = join(" or ", [
        "(last_over_time({__name__=\"${one(google_monitoring_metric_descriptor.spend_guard_cap[*].type)}\", monitored_resource=\"global\", project_id=\"${var.project}\", cluster=\"${var.cluster_name}\"}[${var.spend_guard_interval_minutes * 2}m]) > 0)",
        "(last_over_time({__name__=\"${one(google_monitoring_metric_descriptor.spend_guard_braked[*].type)}\", monitored_resource=\"global\", project_id=\"${var.project}\", cluster=\"${var.cluster_name}\"}[${var.spend_guard_interval_minutes * 2}m]) > 0)",
        "(last_over_time({__name__=\"${one(google_monitoring_metric_descriptor.spend_guard_evaluation[*].type)}\", monitored_resource=\"global\", project_id=\"${var.project}\", cluster=\"${var.cluster_name}\"}[${var.spend_guard_interval_minutes * 2}m]) < 1)",
      ])
      duration            = "0s"
      evaluation_interval = "60s"
    }
  }
  notification_channels = compact([one(google_monitoring_notification_channel.spend_email[*].id)])
  alert_strategy {
    notification_prompts = ["OPENED", "CLOSED"]
  }
  documentation {
    content   = "The spend guard reports a cap exceeded, stopped pools or unknown spend. Run make spend or inspect the function logs for details. On closure, confirm recovery with make spend; missing telemetry is covered by the separate stalled-guard alert."
    mime_type = "text/markdown"
  }
}

resource "google_monitoring_metric_descriptor" "spend_guard_cap" {
  count        = local.spend_guard_enabled ? 1 : 0
  type         = "${local.spend_metric_prefix}/cap_exceeded"
  metric_kind  = "GAUGE"
  value_type   = "DOUBLE"
  unit         = "1"
  display_name = "Spend guard cap exceeded"
  labels {
    key        = "cluster"
    value_type = "STRING"
  }
  depends_on = [google_project_service.required]
}
