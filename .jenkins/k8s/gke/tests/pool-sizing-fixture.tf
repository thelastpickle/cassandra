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

variable "test_scale" { default = 675 / 3760 }
variable "test_quota" { default = 2400 }
variable "test_addresses" { default = 1000 }
locals {
  pool_scale              = var.test_scale
  binding_metrics         = ["CPUS"]
  quota_limits            = { CPUS = var.test_quota }
  controller_demand       = { CPUS = 8 }
  pool_quota_units        = { for size in keys(var.agent_pools) : size => { CPUS = 8 } }
  pod_range_node_ceiling  = var.test_addresses
  node_range_node_ceiling = var.test_addresses
}
