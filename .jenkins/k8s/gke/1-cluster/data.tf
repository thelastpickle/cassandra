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

# The project number, which is not the project id and is needed by name in three places: the Workload
# Identity member string, the Compute Engine default service agent, and the Cloud Build style bucket names.
data "google_project" "this" {}

data "google_container_engine_versions" "this" {
  location = var.region
}

data "google_compute_zones" "available" {
  region = var.region
  status = "UP"
}

# The network and subnetwork, read whether they were named or defaulted, so that the pod range arithmetic
# below holds either way.
data "google_compute_network" "selected" {
  name = coalesce(var.network, "default")
}

data "google_compute_subnetwork" "selected" {
  name   = coalesce(var.subnetwork, "default")
  region = var.region
}

# The DNS zone holding var.jenkins_hostname.  Read, never created: see var.dns_managed_zone.
data "google_dns_managed_zone" "jenkins" {
  count = local.serves_public_name ? 1 : 0

  name = var.dns_managed_zone
}

data "external" "quotas" {
  count = var.size_pools_to_quotas ? 1 : 0

  program = ["bash", "${path.module}/read-quotas.sh"]

  query = {
    project = var.project
    region  = var.region
  }
}
