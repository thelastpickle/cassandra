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

variable "test_scale" { default = 0.1 }
variable "test_vcpus" { default = 5000 }
variable "test_storage_gib" { default = 100000 }
variable "test_addresses" { default = { a = 6000, b = 6000 } }
locals {
  pool_scale              = var.test_scale
  declared_vcpus_per_node = { for size in keys(var.agent_pools) : size => 8 }
  controller_vcpus        = 8
  controller_gib          = 100
  controller_addresses    = 6
  addresses_per_node      = 6
  claimable_vcpus         = var.test_vcpus
  claimable_storage_gib   = var.test_storage_gib
  controller_zone         = "a"
  node_group_zones        = ["a", "b"]
  zone_count              = 2
  zone_address_capacity   = var.test_addresses
}
