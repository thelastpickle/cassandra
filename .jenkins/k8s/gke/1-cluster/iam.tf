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

resource "google_service_account" "node" {
  account_id   = "${var.cluster_name}-node"
  display_name = "GKE node identity for the ${var.cluster_name} Jenkins cluster"
  description  = "Attached to every node pool.  Holds only what a kubelet needs; no build uses it."

  depends_on = [google_project_service.required]
}

resource "google_project_iam_member" "node" {
  for_each = toset([
    "roles/container.nodeServiceAccount",

    "roles/artifactregistry.reader",
  ])

  project = var.project
  role    = each.key
  member  = "serviceAccount:${google_service_account.node.email}"
}

resource "google_service_account" "external_dns" {
  count = var.enable_external_dns ? 1 : 0

  account_id   = "${var.cluster_name}-external-dns"
  display_name = "external-dns for the ${var.cluster_name} Jenkins cluster"
  description  = "Impersonated by ${local.external_dns_namespace}/${local.external_dns_service_account} through Workload Identity.  Writes records in the zones named by var.dns_managed_zone and var.external_dns_zones."

  lifecycle {
    precondition {
      condition     = length(local.external_dns_zones) > 0
      error_message = "enable_external_dns is true and no zone was named, so external-dns would have nothing it may write to.  Set var.dns_managed_zone, or add zones to var.external_dns_zones."
    }
  }

  depends_on = [google_project_service.required]
}

resource "google_service_account_iam_member" "external_dns" {
  count = var.enable_external_dns ? 1 : 0

  service_account_id = one(google_service_account.external_dns[*].name)
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${local.workload_identity_pool}[${local.external_dns_namespace}/${local.external_dns_service_account}]"
}

resource "google_dns_managed_zone_iam_member" "external_dns" {
  for_each = var.enable_external_dns ? toset(local.external_dns_zones) : toset([])

  project      = var.project
  managed_zone = each.key
  role         = "roles/dns.admin"
  member       = "serviceAccount:${one(google_service_account.external_dns[*].email)}"
}

resource "google_project_iam_member" "external_dns_list" {
  count = var.enable_external_dns ? 1 : 0

  project = var.project
  role    = "roles/dns.reader"
  member  = "serviceAccount:${one(google_service_account.external_dns[*].email)}"
}
