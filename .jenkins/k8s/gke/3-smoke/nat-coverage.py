#!/usr/bin/env python3
#
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
"""Print a public NAT covering the selected subnet's primary addresses."""
import json
import sys



def identity(name, project, region):
    if "/" not in name:
        return f"projects/{project}/regions/{region}/subnetworks/{name}"
    return "projects/" + name.split("projects/", 1)[-1]



def covering(nats, subnet, project, region):
    target = identity(subnet, project, region)
    for nat in nats:
        if nat.get("type", "PUBLIC") != "PUBLIC":
            continue
        mode = nat.get("sourceSubnetworkIpRangesToNat")
        if mode in {"ALL_SUBNETWORKS_ALL_IP_RANGES", "ALL_SUBNETWORKS_ALL_PRIMARY_IP_RANGES"}:
            return nat["name"]
        if mode == "LIST_OF_SUBNETWORKS":
            for entry in nat.get("subnetworks", []):
                ranges = entry.get("sourceIpRangesToNat") or ["ALL_IP_RANGES"]
                if identity(entry.get("name", ""), project, region) == target and set(ranges) & {"ALL_IP_RANGES", "PRIMARY_IP_RANGE"}:
                    return nat["name"]
    return ""


if __name__ == "__main__":
    try:
        print(covering(json.load(sys.stdin), *sys.argv[1:]))
    except (ValueError, KeyError, TypeError):
        sys.exit(1)
