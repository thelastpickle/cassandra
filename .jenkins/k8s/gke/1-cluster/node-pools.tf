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

resource "google_container_node_pool" "controller" {
  name     = local.controller_pool_name
  cluster  = google_container_cluster.this.name
  location = var.region

  node_locations = [local.controller_zone]

  autoscaling {
    min_node_count = var.controller_pool.min_size
    max_node_count = var.controller_pool.max_size
  }

  initial_node_count = var.controller_pool.min_size

  management {
    auto_repair  = var.node_auto_repair
    auto_upgrade = var.node_auto_upgrade
  }

  upgrade_settings {
    strategy        = "SURGE"
    max_surge       = 1
    max_unavailable = 0
  }

  max_pods_per_node = var.max_pods_per_node

  node_config {
    machine_type = var.controller_pool.machine_types[0]
    disk_size_gb = var.controller_pool.disk_gb
    disk_type    = var.controller_pool.disk_type
    image_type   = "COS_CONTAINERD"
    spot         = var.controller_pool.spot

    # Kubernetes node labels, which is what a nodeSelector matches.  Not `resource_labels`, which are GCE
    # labels and reject the dots in these keys.
    labels = local.controller_labels

    resource_labels = var.labels

    service_account = google_service_account.node.email

    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]

    workload_metadata_config {
      mode = "GKE_METADATA"
    }

    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }
  }

  lifecycle {
    # The autoscaler owns the node count once the pool exists, for the same reason the sibling ignores
    # `desired_size`: every scale-up would otherwise show as drift and every apply would propose undoing it.
    ignore_changes = [initial_node_count]
  }

  depends_on = [google_project_iam_member.node]
}

resource "google_container_node_pool" "agents" {
  for_each = local.agent_pool_specs

  name     = each.key
  cluster  = google_container_cluster.this.name
  location = var.region

  node_locations = [each.value.zone]

  autoscaling {
    min_node_count = each.value.min_size
    max_node_count = each.value.max_size
  }

  initial_node_count = each.value.min_size

  management {
    auto_repair  = var.node_auto_repair
    auto_upgrade = var.node_auto_upgrade
  }

  upgrade_settings {
    strategy        = "SURGE"
    max_surge       = 5
    max_unavailable = 0
  }

  max_pods_per_node = var.max_pods_per_node

  node_config {
    machine_type = each.value.machine_types[0]
    disk_size_gb = each.value.disk_gb
    disk_type    = each.value.disk_type
    image_type   = "COS_CONTAINERD"
    spot         = each.value.spot

    labels          = each.value.labels
    resource_labels = var.labels

    service_account = google_service_account.node.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]

    workload_metadata_config {
      mode = "GKE_METADATA"
    }

    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }
  }

  lifecycle {
    ignore_changes = [initial_node_count]
  }

  depends_on = [google_project_iam_member.node]
}
