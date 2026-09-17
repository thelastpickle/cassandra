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

resource "google_project_service" "required" {
  for_each = toset([
    "container.googleapis.com",
    "compute.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
  ])

  project = var.project
  service = each.key

  disable_on_destroy = false
}

resource "google_logging_project_bucket_config" "default" {
  count = var.log_retention_days == null ? 0 : 1

  project        = var.project
  location       = "global"
  bucket_id      = "_Default"
  retention_days = var.log_retention_days

  depends_on = [google_project_service.required]
}

moved {
  from = google_logging_project_bucket_config.default
  to   = google_logging_project_bucket_config.default[0]
}

resource "google_container_cluster" "this" {
  name = var.cluster_name

  location = var.region

  node_locations = local.node_zones

  min_master_version = local.kubernetes_version

  release_channel {
    channel = var.release_channel
  }

  remove_default_node_pool = true
  initial_node_count       = 1

  network    = data.google_compute_network.selected.self_link
  subnetwork = data.google_compute_subnetwork.selected.self_link

  ip_allocation_policy {
    cluster_secondary_range_name  = var.pod_range_name
    services_secondary_range_name = var.services_range_name
  }

  default_max_pods_per_node = var.max_pods_per_node

  private_cluster_config {
    enable_private_nodes = var.enable_private_nodes

    enable_private_endpoint = false
  }

  dynamic "master_authorized_networks_config" {
    for_each = var.master_authorized_cidrs == null ? [] : [var.master_authorized_cidrs]

    content {
      gcp_public_cidrs_access_enabled = false

      dynamic "cidr_blocks" {
        for_each = master_authorized_networks_config.value

        content {
          cidr_block   = cidr_blocks.value
          display_name = "authorized-${cidr_blocks.key}"
        }
      }
    }
  }

  workload_identity_config {
    workload_pool = local.workload_identity_pool
  }

  # Satisfy constraints/container.managed.disableRBACSystemBindings at cluster creation.
  rbac_binding_config {
    enable_insecure_binding_system_authenticated   = false
    enable_insecure_binding_system_unauthenticated = false
  }

  logging_config {
    enable_components = var.logging_components
  }

  monitoring_config {
    enable_components = var.monitoring_components

    managed_prometheus {
      enabled = var.enable_managed_prometheus
    }
  }

  addons_config {
    gce_persistent_disk_csi_driver_config {
      enabled = true
    }

    http_load_balancing {
      disabled = !local.serves_public_name
    }

    horizontal_pod_autoscaling {
      disabled = true
    }
  }

  gateway_api_config {
    channel = local.serves_public_name ? "CHANNEL_STANDARD" : "CHANNEL_DISABLED"
  }

  cluster_autoscaling {
    # Node auto-provisioning off.  It would let GKE invent node pools with machine types nobody chose, and
    # every ceiling in locals.tf is computed from the pools that are declared.
    enabled = false

    autoscaling_profile = "OPTIMIZE_UTILIZATION"
  }

  deletion_protection = false

  resource_labels = var.labels

  lifecycle {
    ignore_changes = [min_master_version, node_config, initial_node_count]

    precondition {
      condition     = local.zone_count >= 1
      error_message = "No zone was resolved for ${var.region}, so there is nowhere to put a node pool.  Check var.region and var.zone_count against `gcloud compute zones list --filter=region:${var.region}`."
    }

    precondition {
      condition     = local.pod_range_node_ceiling >= 1
      error_message = "The pod range holds ${local.pod_range_node_ceiling + var.controller_pool.max_size} nodes at ${var.max_pods_per_node} pods each, which leaves no room for an agent beyond the controller.  Lower max_pods_per_node, or give var.pod_range_name a wider range: it cannot be widened once the cluster exists."
    }
  }

  depends_on = [
    google_project_service.required,
    # The nodes' service account must hold its roles before a node tries to register, and the pools depend on
    # the cluster, so the grants are ordered ahead of the cluster rather than ahead of each pool.
    google_project_iam_member.node,
  ]
}
