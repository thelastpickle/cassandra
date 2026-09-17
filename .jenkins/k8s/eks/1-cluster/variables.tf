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

variable "cluster_name" {
  description = "Cluster name, also used in resource names and the generated kubectl context."
  type        = string
  default     = "cassandra-jenkins"

  validation {
    # EKS: 1-100 characters, alphanumeric, hyphen and underscore, starting alphanumeric.
    condition     = can(regex("^[0-9A-Za-z][A-Za-z0-9_-]{0,99}$", var.cluster_name))
    error_message = "cluster_name must start alphanumeric and contain only letters, digits, '-' and '_'."
  }
}

variable "region" {
  description = "AWS region for the cluster and every node group."
  type        = string
  default     = "eu-north-1"
}

variable "kubernetes_version" {
  description = "Kubernetes minor version for the control plane, as `1.34`. Leave null to take the newest version EKS reports in STANDARD_SUPPORT."
  type        = string
  default     = null

  validation {
    condition     = var.kubernetes_version == null || can(regex("^1\\.[0-9]+$", var.kubernetes_version))
    error_message = "kubernetes_version must be a minor version such as \"1.34\", or null to derive it."
  }
}

variable "bootstrap_self_managed_addons" {
  description = "Bootstrap unmanaged networking add-ons at cluster creation. Keep false when OpenTofu manages the add-ons."
  type        = bool
  default     = false
}

variable "vpc_id" {
  description = "VPC to place the cluster in. Leave null to use the account's default VPC in var.region."
  type        = string
  default     = null
}

variable "subnet_ids" {
  description = "Subnets for the control plane's cross-account interfaces and for every node group. Leave null to use every subnet of var.vpc_id that lies in the first var.availability_zone_count availability zones."
  type        = list(string)
  default     = null

  validation {
    condition     = var.subnet_ids == null || length(var.subnet_ids) >= 2
    error_message = "EKS requires subnets in at least two availability zones, so pass at least two."
  }
}

variable "availability_zone_count" {
  description = "How many availability zones to spread across when var.subnet_ids is null."
  type        = number
  default     = null

  validation {
    condition     = var.availability_zone_count == null || var.availability_zone_count >= 2
    error_message = "EKS requires at least two availability zones, so pass null for all of them or a number of at least two."
  }
}

variable "public_access_cidrs" {
  description = "CIDR blocks allowed to reach the public Kubernetes API endpoint. Leave null to accept the EKS default, which is everywhere."
  type        = list(string)
  default     = null
}

variable "control_plane_log_types" {
  description = "Control plane log streams to send to CloudWatch Logs."
  type        = list(string)
  default     = ["api", "audit", "authenticator", "controllerManager", "scheduler"]

  validation {
    condition = length(setsubtract(var.control_plane_log_types,
    ["api", "audit", "authenticator", "controllerManager", "scheduler"])) == 0
    error_message = "Valid log types are api, audit, authenticator, controllerManager and scheduler."
  }
}

variable "log_retention_days" {
  description = "Retention on the control plane's CloudWatch log group."
  type        = number
  default     = 30

  validation {
    condition = contains([0, 1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731,
    1096, 1827, 2192, 2557, 2922, 3288, 3653], var.log_retention_days)
    error_message = "log_retention_days must be one of the retention periods CloudWatch Logs accepts (0 keeps forever)."
  }
}

variable "controller_pool" {
  description = "Node pool for the Jenkins controller and persistent platform services."
  type = object({
    instance_types = optional(list(string), ["m7a.2xlarge"])
    capacity_type  = optional(string, "ON_DEMAND")
    disk_gib       = optional(number, 100)
    min_size       = optional(number, 1)
    max_size       = optional(number, 1)
    desired_size   = optional(number, 1)
  })
  default = {}

  validation {
    condition     = contains(["ON_DEMAND", "SPOT", "CAPACITY_BLOCK"], var.controller_pool.capacity_type)
    error_message = "capacity_type must be ON_DEMAND, SPOT or CAPACITY_BLOCK."
  }

  validation {
    condition = (var.controller_pool.min_size <= var.controller_pool.desired_size
    && var.controller_pool.desired_size <= var.controller_pool.max_size)
    error_message = "controller_pool needs min_size <= desired_size <= max_size."
  }

  validation {
    condition     = var.controller_pool.min_size >= 1
    error_message = "The controller pool cannot scale to zero: the Jenkins controller is a StatefulSet with a bound PVC."
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
  description = "Agent pools keyed by the size in their cassandra.jenkins.agent.<size> node label."
  type = map(object({
    instance_types = list(string)
    capacity_type  = optional(string, "ON_DEMAND")
    disk_gib       = optional(number, 100)
    min_size       = optional(number, 0)
    max_size       = number
    desired_size   = optional(number, 0)
  }))

  default = {
    small  = { instance_types = ["c7a.2xlarge"], max_size = 10 }
    medium = { instance_types = ["c7a.2xlarge"], max_size = 190 }
    large  = { instance_types = ["m7a.2xlarge"], max_size = 276 }
    report = { instance_types = ["m7a.2xlarge"], max_size = 4 }
  }

  validation {
    condition = alltrue([for size, pool in var.agent_pools :
      size != "small" || pool.max_size == 0 || pool.max_size >= var.small_workers_per_build + 1
    ])
    error_message = "The small pool needs max_size >= small_workers_per_build + 1 for a pipeline and its workers. Set 0 only to disable the pool."
  }

  validation {
    condition     = alltrue([for size in keys(var.agent_pools) : can(regex("^[a-z0-9]+$", size))])
    error_message = "agent_pools keys must be single lower-case words: '-' and '_' are how .build/run-ci splits a node group name."
  }

  validation {
    condition = alltrue([for pool in values(var.agent_pools) :
    pool.min_size <= pool.desired_size && pool.desired_size <= pool.max_size])
    error_message = "Each agent pool needs min_size <= desired_size <= max_size."
  }

  validation {
    condition     = alltrue([for pool in values(var.agent_pools) : contains(["ON_DEMAND", "SPOT"], pool.capacity_type)])
    error_message = "Agent pool capacity_type must be ON_DEMAND or SPOT."
  }

  validation {
    # locals.tf takes the largest of a pool's instance types to size it against the vCPU quota, and max() of
    # nothing is an error rather than a message about a pool.
    condition     = alltrue([for pool in values(var.agent_pools) : length(pool.instance_types) > 0])
    error_message = "Each agent pool needs at least one instance type."
  }
}

variable "size_pools_to_quotas" {
  description = "Scale agent pools to CPU, storage and address budgets, reserving minimum capacity first."
  type        = bool
  default     = true

  validation {
    condition     = !var.size_pools_to_quotas || var.quota_headroom > 0
    error_message = "size_pools_to_quotas needs quota_headroom above 0."
  }
}

variable "control_plane_zone_count" {
  description = "How many availability zones the cluster's own subnets span. Two is the EKS minimum and the default."
  type        = number
  default     = 2

  validation {
    condition     = var.control_plane_zone_count >= 2
    error_message = "EKS requires the cluster's subnets to span at least two availability zones."
  }
}

variable "quota_headroom" {
  description = "The share of each account quota that var.size_pools_to_quotas may claim for these pools."
  type        = number
  default     = 0.9

  validation {
    condition     = var.quota_headroom > 0 && var.quota_headroom <= 1
    error_message = "quota_headroom is a share of a quota, so it must be above 0 and at most 1."
  }
}

variable "node_auto_repair" {
  description = "Let EKS replace or reboot a node it finds unhealthy, and install the node monitoring agent add-on that widens what \"unhealthy\" covers."
  type        = bool
  default     = true
}

variable "enable_cloudwatch_observability" {
  description = "Install the CloudWatch observability add-on. Disabled by default."
  type        = bool
  default     = false
}

variable "enable_external_dns" {
  description = "Enable external-dns and its cloud DNS permissions."
  type        = bool
  default     = false
}

variable "external_dns_hosted_zone_ids" {
  description = "Additional Route 53 hosted zones external-dns may update, alongside dns_hosted_zone_id."
  type        = list(string)
  default     = []
}

variable "jenkins_hostname" {
  description = "Public Jenkins hostname. Empty uses the Service load balancer address."
  type        = string
  default     = ""

  validation {
    # A hostname, not a URL and not a wildcard.  ACM accepts a wildcard, and every other consumer of
    # this value does not: a Service annotation, a certificate for one name, and a Jenkins URL.
    condition     = var.jenkins_hostname == "" || can(regex("^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$", var.jenkins_hostname))
    error_message = "jenkins_hostname must be a bare lower-case hostname with at least one dot, and no scheme, port, path, or wildcard."
  }
}

variable "dns_hosted_zone_id" {
  description = "Route 53 public hosted zone holding var.jenkins_hostname. Required when that is set."
  type        = string
  default     = ""

  validation {
    condition     = var.dns_hosted_zone_id == "" || can(regex("^[A-Z0-9]{8,32}$", var.dns_hosted_zone_id))
    error_message = "dns_hosted_zone_id must be a bare hosted zone identifier such as Z0123456789ABCDEFGHIJ, with no /hostedzone/ prefix."
  }
}

variable "spend_cap_daily_usd" {
  description = "What this account may spend in a UTC day, in US dollars, before the agent pools are stopped. Null is no daily cap."
  type        = number
  default     = null

  validation {
    condition     = var.spend_cap_daily_usd == null || var.spend_cap_daily_usd > 0
    error_message = "spend_cap_daily_usd must be above 0, or null for no daily cap."
  }
}

variable "spend_cap_weekly_usd" {
  description = "What this account may spend in a week, in US dollars, before the agent pools are stopped. Null is no weekly cap."
  type        = number
  default     = null

  validation {
    condition     = var.spend_cap_weekly_usd == null || var.spend_cap_weekly_usd > 0
    error_message = "spend_cap_weekly_usd must be above 0, or null for no weekly cap."
  }
}

variable "spend_cap_monthly_usd" {
  description = "What this account may spend in a calendar month, in US dollars, before the agent pools are stopped. Null is no monthly cap."
  type        = number
  default     = null

  validation {
    condition     = var.spend_cap_monthly_usd == null || var.spend_cap_monthly_usd > 0
    error_message = "spend_cap_monthly_usd must be above 0, or null for no monthly cap."
  }
}

variable "spend_alert_email" {
  description = "Email destination for spend-guard incidents and recovery notifications. Empty disables email."
  type        = string
  default     = ""

  validation {
    condition     = var.spend_alert_email == "" || can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", var.spend_alert_email))
    error_message = "spend_alert_email must be an email address, or empty."
  }
}

variable "spend_guard_interval_minutes" {
  description = "How often the guard compares spend against the caps."
  type        = number
  default     = 5

  validation {
    condition     = var.spend_guard_interval_minutes >= 2 && var.spend_guard_interval_minutes <= 60
    error_message = "spend_guard_interval_minutes must be between 2 and 60."
  }
}

variable "spend_price_per_vcpu_hour" {
  description = "USD per vCPU-hour used until billing data supports calibration."
  type        = number
  default     = 0.06

  validation {
    condition     = var.spend_price_per_vcpu_hour > 0
    error_message = "spend_price_per_vcpu_hour must be above 0."
  }
}

variable "spend_fixed_usd_per_day" {
  description = "Estimated fixed daily cost in USD, used until billing data supports calibration."
  type        = number
  default     = 5

  validation {
    condition     = var.spend_fixed_usd_per_day >= 0
    error_message = "spend_fixed_usd_per_day cannot be negative."
  }
}

variable "spend_cost_metric" {
  description = "Which Cost Explorer metric the caps are compared against."
  type        = string
  default     = "UnblendedCost"

  validation {
    condition = contains(["UnblendedCost", "NetUnblendedCost", "AmortizedCost", "NetAmortizedCost",
    "BlendedCost"], var.spend_cost_metric)
    error_message = "spend_cost_metric must be one of the cost metrics GetCostAndUsage publishes."
  }
}

variable "spend_cost_refresh_hours" {
  description = "How often the guard calls Cost Explorer."
  type        = number
  default     = 6

  validation {
    condition     = var.spend_cost_refresh_hours >= 1 && var.spend_cost_refresh_hours <= 24
    error_message = "spend_cost_refresh_hours must be between 1 and 24."
  }
}

variable "enable_spend_budgets" {
  description = "Create an AWS Budget for the daily and monthly caps, alongside the guard."
  type        = bool
  default     = true
}

variable "tags" {
  description = "Tags applied to every resource created here, through the provider's default_tags."
  type        = map(string)
  default = {
    Project   = "apache-cassandra-ci"
    ManagedBy = "opentofu"
  }
}
