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

resource "google_certificate_manager_dns_authorization" "jenkins" {
  count = local.serves_public_name ? 1 : 0

  name        = "${var.cluster_name}-jenkins"
  domain      = var.jenkins_hostname
  description = "Proves ${var.jenkins_hostname} is ours, for the ${var.cluster_name} Jenkins certificate."

  labels = var.labels

  lifecycle {
    precondition {
      condition     = var.dns_managed_zone != ""
      error_message = "jenkins_hostname needs dns_managed_zone: the record below has to be written into the zone that holds the name, and this configuration cannot find that zone on its own."
    }

    precondition {
      condition     = endswith(var.jenkins_hostname, trimsuffix(one(data.google_dns_managed_zone.jenkins[*].dns_name), "."))
      error_message = "jenkins_hostname must lie inside the zone named by dns_managed_zone."
    }
  }

  depends_on = [google_project_service.certificate_manager]
}

# Certificate Manager is not in the list in cluster.tf, because it is needed only when a public name is set and
# enabling an API nobody uses is a permission asked for nothing.
resource "google_project_service" "certificate_manager" {
  count = local.serves_public_name ? 1 : 0

  project = var.project
  service = "certificatemanager.googleapis.com"

  disable_on_destroy = false
}

resource "google_dns_record_set" "certificate_validation" {
  count = local.serves_public_name ? 1 : 0

  project      = var.project
  managed_zone = var.dns_managed_zone

  name = one(google_certificate_manager_dns_authorization.jenkins[*].dns_resource_record[0].name)
  type = one(google_certificate_manager_dns_authorization.jenkins[*].dns_resource_record[0].type)
  ttl  = 60

  rrdatas = [one(google_certificate_manager_dns_authorization.jenkins[*].dns_resource_record[0].data)]
}

# Not a wildcard, deliberately, as in the sibling: every consumer of this wants one name, and a wildcard
# certificate for a CI host is a certificate for everything else in the zone as well.
resource "google_certificate_manager_certificate" "jenkins" {
  count = local.serves_public_name ? 1 : 0

  name        = "${var.cluster_name}-jenkins"
  description = "TLS for ${var.jenkins_hostname}."

  labels = var.labels

  managed {
    domains            = [var.jenkins_hostname]
    dns_authorizations = [one(google_certificate_manager_dns_authorization.jenkins[*].id)]
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "google_certificate_manager_certificate_map" "jenkins" {
  count = local.serves_public_name ? 1 : 0

  name        = "${var.cluster_name}-jenkins"
  description = "Attach this to an Ingress or Gateway to serve ${var.jenkins_hostname} over TLS."

  labels = var.labels
}

resource "google_certificate_manager_certificate_map_entry" "jenkins" {
  count = local.serves_public_name ? 1 : 0

  name         = "${var.cluster_name}-jenkins"
  map          = one(google_certificate_manager_certificate_map.jenkins[*].name)
  certificates = [one(google_certificate_manager_certificate.jenkins[*].id)]
  hostname     = var.jenkins_hostname

  labels = var.labels
}
