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
"""Quota membership and bounded pool-capacity regressions."""
import importlib.util
from pathlib import Path
import unittest
import json
import os
import subprocess
import tempfile
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("quota", Path(__file__).with_name("vcpu-quota.py"))
quota = importlib.util.module_from_spec(spec)
spec.loader.exec_module(quota)


def pool(series="e2", maximum=1, cpus=8, **kw):
    return dict(machine_types=[f"{series}-standard-{cpus}"], max_size=maximum, disk_gb=100, disk_type="pd-ssd", **kw)


class QuotaTest(unittest.TestCase):
    def test_dimensioned_cpu_quotas_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "does not support"):
            quota.pool_row("large", pool("n4", 10))

    def test_quota_names_are_normalized(self):
        with patch.object(quota, "gcloud_json", side_effect=[
                {"quotas": [{"metric": "HDB-TOTAL-GB", "limit": 200}]},
                {"quotas": [{"metric": "CPUS_ALL_REGIONS", "limit": 32}]}]):
            table = quota.quota_table("region", "project")
        self.assertEqual(200, table["HDB_TOTAL_GB"]["limit"])
        self.assertEqual(32, table["CPUS_ALL_REGIONS"]["limit"])

    def derive(self, pools, limits, controller=None):
        table = {key:{"limit":value,"usage":0,"scope":"region"} for key,value in limits.items()}
        with patch.object(quota,"quota_table",return_value=table):
            return quota.derive(pools,controller or pool(),"region","project",None)

    def test_disjoint_families_keep_all_agents(self):
        d=self.derive({"large":pool("n2",84),"small":pool(maximum=2)}, {"CPUS":24,"N2_CPUS":672,"E2_CPUS":500})
        self.assertEqual(86,d["max_agent_nodes"])
        self.assertNotIn("E2_CPUS",[m["metric"] for m in d["metrics"]])
        self.assertFalse(d["over_ask"])

    def test_disabled_pool_does_not_bind(self):
        self.assertEqual(2,self.derive({"large":pool("n2",0),"small":pool(maximum=2)},{"CPUS":24,"N2_CPUS":0})["max_agent_nodes"])

    def test_controller_reserves_its_actual_size(self):
        self.assertEqual(1,self.derive({"small":pool()},{"CPUS":24},pool(cpus=16))["max_agent_nodes"])

    def test_spot_falls_back_to_regular_quota(self):
        self.assertEqual(2,self.derive({"small":pool(maximum=20,spot=True)},{"CPUS":24})["max_agent_nodes"])

    def test_separate_spot_quota_does_not_spend_family_quota(self):
        d=self.derive({"large":pool("n2",4,spot=True)},{"CPUS":8,"N2_CPUS":0,"PREEMPTIBLE_CPUS":32})
        self.assertEqual(4,d["max_agent_nodes"])
        self.assertNotIn("N2_CPUS",[m["metric"] for m in d["metrics"]])

    def test_global_cpu_binds(self):
        self.assertEqual(3,self.derive({"small":pool(maximum=100)},{"CPUS":800,"CPUS_ALL_REGIONS":32})["max_agent_nodes"])

    def test_balanced_and_hyperdisk_capacity(self):
        for disk, metric in (("pd-balanced","SSD_TOTAL_GB"),("hyperdisk-balanced","HDB_TOTAL_GB")):
            agent=pool(maximum=10)
            agent["disk_type"]=disk
            controller=pool()
            controller["disk_type"]="pd-standard"
            self.assertEqual(2,self.derive({"large":agent},{metric:200},controller)["max_agent_nodes"])


class QuotaMembershipHclTest(unittest.TestCase):
    def test_production_membership_agrees_with_python(self):
        source=Path(__file__).with_name("1-cluster")/"quota-membership.tf"
        cases=[("e2",False,"pd-ssd",{}), ("n2",False,"pd-balanced",{}),
               ("n2",True,"pd-standard",{}), ("n2",True,"pd-ssd",{"PREEMPTIBLE_CPUS":8}),
               ("n2",False,"hyperdisk-balanced",{})]
        with tempfile.TemporaryDirectory() as tmp:
            directory=Path(tmp)
            (directory/"quota-membership.tf").write_text(source.read_text())
            (directory/"inputs.tf").write_text("""
variable "pools" { type = any }
variable "quotas" { type = any }
locals {
  quota_pools = var.pools
  quota_limits = var.quotas
}
""")
            for series, spot, disk, quotas in cases:
                item=pool(series,spot=spot)
                item["disk_type"]=disk
                node={"vcpus":8,"series":[series],"spot":spot,"disk_type":disk,"disk_gb":100}
                (directory/"terraform.tfvars.json").write_text(json.dumps({"pools":{"test":node},"quotas":quotas}))
                result=subprocess.run([os.environ.get("TOFU","tofu"), "-chdir="+tmp, "console"],input="jsonencode(local.pool_quota_units.test)\n",capture_output=True,text=True,check=True)
                actual=json.loads(json.loads(result.stdout))
                expected=quota.charged_metrics(quota.pool_row("test",item),quotas)
                self.assertEqual(expected,actual)
                self.assertEqual(8,actual["CPUS_ALL_REGIONS"])


if __name__ == "__main__":
    unittest.main()
