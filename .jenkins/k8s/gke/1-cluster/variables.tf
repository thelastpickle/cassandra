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

variable "project" {
  type        = string
  description = "The GCP project this cluster is created in."

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.project))
    error_message = "project must be a project id, 6 to 30 characters, starting with a lower-case letter."
  }
}

variable "region" {
  type        = string
  default     = "us-central1"
  description = "Region for the cluster and every node pool. The cluster is regional, so its control plane is replicated across three zones of this region and the node pools are created one per zone."

  validation {
    condition     = can(regex("^[a-z]+[a-z0-9-]*[0-9]$", var.region))
    error_message = "region must be a region such as \"us-central1\", not a zone such as \"us-central1-c\"."
  }
}

variable "cluster_name" {
  type        = string
  default     = "cassandra-jenkins"
  description = "Cluster name, also used in resource names and the generated kubectl context."

  validation {
    condition     = can(regex("^[a-z]([a-z0-9-]{0,38}[a-z0-9])?$", var.cluster_name))
    error_message = "cluster_name must be RFC 1035: lower-case, starting with a letter, letters digits and hyphens only, at most 40 characters."
  }

  validation {
    condition     = length(var.cluster_name) <= 25
    error_message = "cluster_name must be at most 25 characters so the -node service account id fits the 30-character limit."
  }

  validation {
    condition = length(var.cluster_name) <= 18 || alltrue([
      var.spend_cap_daily_usd == null, var.spend_cap_weekly_usd == null, var.spend_cap_monthly_usd == null
    ])
    error_message = "cluster_name must be at most 18 characters when any spend cap is set, so the -spend-guard, -spend-sched and -spend-build service account ids fit the 30-character limit."
  }

  validation {
    condition     = !var.enable_external_dns || length(var.cluster_name) <= 17
    error_message = "cluster_name must be at most 17 characters when enable_external_dns is true, so the -external-dns service account id fits the 30-character limit."
  }
}

variable "kubernetes_version" {
  type        = string
  default     = null
  description = "Kubernetes minor version for the control plane, as `1.34`. Leave null to take the default version of var.release_channel, which is what Google is currently promoting into that channel."

  validation {
    condition     = var.kubernetes_version == null || can(regex("^1\\.[0-9]+(\\.[0-9]+(-gke\\.[0-9]+)?)?$", var.kubernetes_version))
    error_message = "kubernetes_version must be a version such as \"1.34\", \"1.34.1\" or \"1.34.1-gke.1000\", or null to take the channel's default."
  }
}

variable "release_channel" {
  type        = string
  default     = "REGULAR"
  description = "GKE release channel: RAPID, REGULAR or STABLE."

  validation {
    condition     = contains(["RAPID", "REGULAR", "STABLE", "EXTENDED", "UNSPECIFIED"], var.release_channel)
    error_message = "release_channel must be RAPID, REGULAR, STABLE, EXTENDED or UNSPECIFIED."
  }
}

variable "network" {
  type        = string
  default     = null
  description = "VPC network for the cluster. Leave null to use the project's `default` network."
}

variable "subnetwork" {
  type        = string
  default     = null
  description = "Subnetwork for the nodes. Leave null to use the `default` subnetwork of var.region."
}

variable "pod_range_name" {
  type        = string
  default     = null
  description = "Name of the secondary IP range on var.subnetwork that pod addresses come from. Leave null to have GKE create one, which it sizes at /14."
}

variable "services_range_name" {
  type        = string
  default     = null
  description = "Name of the secondary IP range on var.subnetwork that ClusterIP service addresses come from. Leave null to have GKE create one."
}

variable "max_pods_per_node" {
  type        = number
  default     = 110
  description = "How many pods a node may run. 110 is the Kubernetes default and GKE's."

  validation {
    condition     = var.max_pods_per_node >= 8 && var.max_pods_per_node <= 256
    error_message = "max_pods_per_node must be between 8 and 256, which is the range GKE accepts."
  }
}

variable "zone_count" {
  type        = number
  default     = null
  description = "How many zones of var.region the node pools spread across. Null, the default, means every zone the region offers, which is four in us-central1 and three in most others."

  validation {
    condition     = var.zone_count == null || var.zone_count >= 1
    error_message = "zone_count must be at least 1, or null for every zone the region offers."
  }
}

variable "enable_private_nodes" {
  type        = bool
  default     = true
  description = "Give the nodes no external IP addresses."
}

variable "master_authorized_cidrs" {
  type        = list(string)
  default     = null
  description = "CIDR blocks allowed to reach the Kubernetes API server. Leave null to accept the GKE default, which is everywhere."
}

variable "logging_components" {
  type        = list(string)
  default     = ["SYSTEM_COMPONENTS", "APISERVER", "CONTROLLER_MANAGER", "SCHEDULER"]
  description = "Which control plane and system logs go to Cloud Logging."

  validation {
    condition = length(setsubtract(var.logging_components,
    ["SYSTEM_COMPONENTS", "WORKLOADS", "APISERVER", "CONTROLLER_MANAGER", "SCHEDULER"])) == 0
    error_message = "Valid logging components are SYSTEM_COMPONENTS, WORKLOADS, APISERVER, CONTROLLER_MANAGER and SCHEDULER."
  }

  validation {
    condition     = contains(var.logging_components, "SYSTEM_COMPONENTS") || length(var.logging_components) == 0
    error_message = "GKE requires SYSTEM_COMPONENTS whenever any other logging component is enabled; pass [] to turn logging off entirely."
  }
}

variable "monitoring_components" {
  type        = list(string)
  default     = ["SYSTEM_COMPONENTS"]
  description = "Which metrics go to Cloud Monitoring."

  validation {
    condition = length(setsubtract(var.monitoring_components,
    ["SYSTEM_COMPONENTS", "APISERVER", "SCHEDULER", "CONTROLLER_MANAGER", "STORAGE", "HPA", "POD", "DAEMONSET", "DEPLOYMENT", "STATEFULSET", "CADVISOR", "KUBELET"])) == 0
    error_message = "monitoring_components must be components google_container_cluster.monitoring_config accepts."
  }
}

variable "enable_managed_prometheus" {
  type        = bool
  default     = false
  description = "Run Google Cloud Managed Service for Prometheus in the cluster."
}

variable "log_retention_days" {
  type        = number
  default     = 30
  description = "Retention on the project's `_Default` log bucket, in days. Null leaves retention under project administration and creates no bucket configuration resource."

  validation {
    condition     = var.log_retention_days == null ? true : var.log_retention_days >= 1 && var.log_retention_days <= 3650
    error_message = "log_retention_days must be between 1 and 3650, or null to leave retention under project administration."
  }
}

variable "controller_pool" {
  type = object({
    machine_types = optional(list(string), ["e2-standard-8"])
    spot          = optional(bool, false)
    disk_gb       = optional(number, 107)
    disk_type     = optional(string, "pd-ssd")
    min_size      = optional(number, 1)
    max_size      = optional(number, 1)
  })
  default     = {}
  description = "Node pool for the Jenkins controller and persistent platform services."

  validation {
    condition     = var.controller_pool.min_size <= var.controller_pool.max_size
    error_message = "controller_pool needs min_size <= max_size."
  }

  validation {
    condition     = var.controller_pool.min_size >= 1
    error_message = "The controller pool cannot scale to zero: the Jenkins controller is a StatefulSet with a bound PVC."
  }

  validation {
    # Exactly one, for the reason in var.agent_pools: a GKE node pool takes a single machine_type.
    condition     = length(var.controller_pool.machine_types) == 1
    error_message = "controller_pool needs exactly one machine type: a GKE node pool takes a single machine_type."
  }

  validation {
    condition     = contains(["pd-standard", "pd-balanced", "pd-ssd", "hyperdisk-balanced"], var.controller_pool.disk_type)
    error_message = "controller_pool.disk_type must be pd-standard, pd-balanced, pd-ssd or hyperdisk-balanced."
  }

  validation {
    # Same reason as var.agent_pools: locals.tf reads the vCPU count out of the name.
    condition = alltrue([for type in var.controller_pool.machine_types :
    can(regex("^(e2|n1|n2|n2d|t2d|c2|c2d|c3|m1|m2|m3)-[a-z0-9]+-[0-9]+$", type))])
    error_message = "Quota sizing supports predefined E2, N1, N2, N2D, T2D, C2, C2D, C3 and M1/M2/M3 types ending in their vCPU count, such as \"e2-standard-8\"."
  }

  validation {
    condition = alltrue([for type in var.controller_pool.machine_types :
    !can(regex("^(t2a|c4a)-", type))])
    error_message = "Every controller_pool machine type must be x86_64: the t2a- and c4a- series are Arm."
  }
}

variable "small_workers_per_build" {
  type        = number
  default     = 3
  description = "Small worker slots per pipeline, excluding its outer agent. Apply and run make jenkins after changing this value."
  validation {
    condition     = var.small_workers_per_build >= 1 && floor(var.small_workers_per_build) == var.small_workers_per_build
    error_message = "small_workers_per_build must be a positive integer."
  }
}

variable "agent_pools" {
  type = map(object({
    machine_types = list(string)
    spot          = optional(bool, false)
    disk_gb       = optional(number, 200)
    disk_type     = optional(string, "pd-ssd")
    min_size      = optional(number, 0)
    max_size      = number
  }))

  default = {
    small  = { machine_types = ["e2-highcpu-8"], max_size = 10 }
    medium = { machine_types = ["n2-highcpu-8"], max_size = 190 }
    large  = { machine_types = ["n2-standard-8"], max_size = 276 }
    report = { machine_types = ["n2-standard-8"], max_size = 4 }
  }

  description = "Agent pools keyed by the size in their cassandra.jenkins.agent.<size> node label."

  validation {
    condition = alltrue([for size, pool in var.agent_pools :
      size != "small" || pool.max_size == 0 || pool.max_size >= var.small_workers_per_build + 1
    ])
    error_message = "The small pool needs max_size >= small_workers_per_build + 1 for a pipeline and its workers. Set 0 only to disable the pool."
  }

  validation {
    condition     = alltrue([for size in keys(var.agent_pools) : can(regex("^[a-z0-9]+$", size))])
    error_message = "agent_pools keys must be single lower-case words: '-' and '_' are how .build/run-ci splits a node pool name."
  }

  validation {
    condition     = alltrue([for pool in values(var.agent_pools) : pool.min_size <= pool.max_size])
    error_message = "Each agent pool needs min_size <= max_size."
  }

  validation {
    condition     = alltrue([for pool in values(var.agent_pools) : length(pool.machine_types) == 1])
    error_message = "Each agent pool needs exactly one machine type: a GKE node pool takes a single machine_type, with no list to choose from at launch."
  }

  validation {
    condition = alltrue([for pool in values(var.agent_pools) :
    contains(["pd-standard", "pd-balanced", "pd-ssd", "hyperdisk-balanced"], pool.disk_type)])
    error_message = "Each agent pool's disk_type must be pd-standard, pd-balanced, pd-ssd or hyperdisk-balanced."
  }

  validation {
    condition = alltrue(flatten([for pool in values(var.agent_pools) : [
      for type in pool.machine_types : can(regex("^(e2|n1|n2|n2d|t2d|c2|c2d|c3|m1|m2|m3)-[a-z0-9]+-[0-9]+$", type))
    ]]))
    error_message = "Quota sizing supports predefined E2, N1, N2, N2D, T2D, C2, C2D, C3 and M1/M2/M3 types ending in their vCPU count, such as \"n2-standard-8\": a shared-core or custom type has no readable vCPU count in its name."
  }

  validation {
    # The agent images in jenkins-deployment.yaml are amd64, and an Arm node accepts the pod and then fails
    # every container with `exec format error`, which reads as a broken image rather than a wrong node.
    condition = alltrue(flatten([for pool in values(var.agent_pools) : [
      for type in pool.machine_types : !can(regex("^(t2a|c4a)-", type))
    ]]))
    error_message = "Every agent pool machine type must be x86_64: the t2a- and c4a- series are Arm, and the agent images in jenkins-deployment.yaml are amd64."
  }
}

variable "node_auto_repair" {
  type        = bool
  default     = true
  description = "Let GKE replace a node it finds unhealthy."
}

variable "node_auto_upgrade" {
  type        = bool
  default     = true
  description = "Let GKE upgrade the nodes to match the control plane."

  validation {
    condition     = var.node_auto_upgrade || var.release_channel == "UNSPECIFIED"
    error_message = "node_auto_upgrade can only be false when release_channel is UNSPECIFIED: GKE requires auto-upgrade inside a channel."
  }
}

variable "size_pools_to_quotas" {
  type        = bool
  default     = true
  description = "Scale agent pools to CPU, storage and address budgets, reserving minimum capacity first."

  validation {
    condition     = !var.size_pools_to_quotas || var.quota_headroom > 0
    error_message = "size_pools_to_quotas needs quota_headroom above 0."
  }
}

variable "quota_headroom" {
  type        = number
  default     = 0.9
  description = "The share of each quota that var.size_pools_to_quotas may claim for these pools."

  validation {
    condition     = var.quota_headroom > 0 && var.quota_headroom <= 1
    error_message = "quota_headroom is a share of a quota, so it must be above 0 and at most 1."
  }
}

variable "jenkins_hostname" {
  type        = string
  default     = ""
  description = "Public Jenkins hostname. Empty uses the Service load balancer address."

  validation {
    # A hostname, not a URL and not a wildcard.  Certificate Manager accepts a wildcard and every other
    # consumer of this value does not: a Service annotation, a certificate for one name, and a Jenkins URL.
    condition     = var.jenkins_hostname == "" || can(regex("^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$", var.jenkins_hostname))
    error_message = "jenkins_hostname must be a bare lower-case hostname with at least one dot, and no scheme, port, path, or wildcard."
  }
}

variable "dns_managed_zone" {
  type        = string
  default     = ""
  description = "Cloud DNS public managed zone holding var.jenkins_hostname. Required when that is set."

  validation {
    condition     = var.dns_managed_zone == "" || can(regex("^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$", var.dns_managed_zone))
    error_message = "dns_managed_zone must be a Cloud DNS zone name: lower-case, starting with a letter, letters digits and hyphens only."
  }
}

variable "enable_external_dns" {
  type        = bool
  default     = false
  description = "Enable external-dns and its cloud DNS permissions."
}

variable "external_dns_zones" {
  type        = list(string)
  default     = []
  description = "Further Cloud DNS managed zones external-dns may write to, beyond var.dns_managed_zone."
}

variable "spend_cap_daily_usd" {
  type        = number
  default     = null
  description = "What this project may spend in a UTC day, in US dollars, before the agent pools are stopped. Null is no daily cap."

  validation {
    condition     = var.spend_cap_daily_usd == null || var.spend_cap_daily_usd > 0
    error_message = "spend_cap_daily_usd must be above 0, or null for no daily cap."
  }
}

variable "spend_cap_weekly_usd" {
  type        = number
  default     = null
  description = "What this project may spend in a week, in US dollars, before the agent pools are stopped. Null is no weekly cap."

  validation {
    condition     = var.spend_cap_weekly_usd == null || var.spend_cap_weekly_usd > 0
    error_message = "spend_cap_weekly_usd must be above 0, or null for no weekly cap."
  }
}

variable "spend_cap_monthly_usd" {
  type        = number
  default     = null
  description = "What this project may spend in a calendar month, in US dollars, before the agent pools are stopped. Null is no monthly cap."

  validation {
    condition     = var.spend_cap_monthly_usd == null || var.spend_cap_monthly_usd > 0
    error_message = "spend_cap_monthly_usd must be above 0, or null for no monthly cap."
  }
}

variable "spend_alert_email" {
  type        = string
  default     = ""
  description = "Email destination for spend-guard incidents and recovery notifications. Empty disables email."

  validation {
    condition     = var.spend_alert_email == "" || can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", var.spend_alert_email))
    error_message = "spend_alert_email must be an email address, or empty."
  }
}

variable "spend_scheduler_region" {
  type        = string
  default     = null
  description = "Cloud Scheduler region. Null uses var.region; choose another supported region if Scheduler is unavailable there."

  validation {
    condition     = var.spend_scheduler_region == null || can(regex("^[a-z]+[a-z0-9-]*[0-9]$", var.spend_scheduler_region))
    error_message = "spend_scheduler_region must be a region such as europe-west1, or null to use region."
  }
}

variable "spend_guard_interval_minutes" {
  type        = number
  default     = 5
  description = "How often the guard compares spend against the caps."

  validation {
    condition     = var.spend_guard_interval_minutes >= 1 && var.spend_guard_interval_minutes <= 30
    error_message = "spend_guard_interval_minutes must be between 1 and 30, so that `*/N * * * *` is a cron expression that means what it reads as."
  }
}

variable "spend_price_per_vcpu_hour" {
  type        = number
  default     = 0.04
  description = "USD per vCPU-hour used until billing data supports calibration."

  validation {
    condition     = var.spend_price_per_vcpu_hour > 0
    error_message = "spend_price_per_vcpu_hour must be above 0."
  }
}

variable "spend_fixed_usd_per_day" {
  type        = number
  default     = 5
  description = "Estimated fixed daily cost in USD, used until billing data supports calibration."

  validation {
    condition     = var.spend_fixed_usd_per_day >= 0
    error_message = "spend_fixed_usd_per_day cannot be negative."
  }
}

variable "spend_billing_table" {
  type        = string
  default     = ""
  description = "The BigQuery table holding this billing account's detailed usage cost export, as `<project>.<dataset>.gcp_billing_export_resource_v1_<BILLING_ACCOUNT_ID>`. Empty means there is none, which is the default."

  validation {
    condition     = var.spend_billing_table == "" || can(regex("^[a-z0-9-]+[.:][a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+$", var.spend_billing_table))
    error_message = "spend_billing_table must be a fully qualified BigQuery table as project.dataset.table, or empty."
  }
}

variable "spend_cost_refresh_hours" {
  type        = number
  default     = 6
  description = "How often the guard queries var.spend_billing_table. Ignored when that is empty."

  validation {
    condition     = var.spend_cost_refresh_hours >= 1 && var.spend_cost_refresh_hours <= 24
    error_message = "spend_cost_refresh_hours must be between 1 and 24."
  }
}

variable "billing_account" {
  type        = string
  default     = ""
  description = "The billing account this project bills to, as `01ABCD-2345EF-67890A`. Empty means no Cloud Billing budget is created, which is the default."

  validation {
    condition     = var.billing_account == "" || can(regex("^[0-9A-F]{6}-[0-9A-F]{6}-[0-9A-F]{6}$", var.billing_account))
    error_message = "billing_account must be a billing account id such as 01ABCD-2345EF-67890A, or empty."
  }
}

variable "labels" {
  type        = map(string)
  description = "Labels applied to every resource that takes them, through the provider's default_labels."

  default = {
    project    = "apache-cassandra-ci"
    managed-by = "opentofu"
  }

  validation {
    condition = alltrue([for key, value in var.labels :
      can(regex("^[a-z][a-z0-9_-]{0,62}$", key)) && can(regex("^[a-z0-9_-]{0,63}$", value))
    ])
    error_message = "GCP labels are lower-case: keys must start with a letter, and keys and values may hold only lower-case letters, digits, '-' and '_'."
  }
}
