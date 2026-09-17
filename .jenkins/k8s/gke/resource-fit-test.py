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
"""Resource requests, limits and GKE reservations."""
import contextlib
import importlib.util
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import yaml

ROOT = Path(__file__).resolve().parents[1]


def load(path, name):
    spec=importlib.util.spec_from_file_location(name, path)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


controller=load(ROOT/"gke/controller-fit.py", "controller")


class ResourceFitTest(unittest.TestCase):
    def test_request_defaults_to_limit_in_both_clouds(self):
        for cloud in ("gke", "eks"):
            module=load(ROOT/cloud/"3-smoke/check-pool-fit.py", cloud+"fit")
            for request, expected in (({},16_000_000_000),({"resourceRequestMemory":"0"},0)):
                template=[{"nodeSelector":"cassandra.jenkins.agent.small=true", "containers":[dict(resourceLimitMemory="16G", **request)]}]
                with tempfile.TemporaryDirectory() as tmp:
                    file=Path(tmp)/"values.yaml"
                    file.write_text(yaml.safe_dump({"agent":{"podTemplates":{"small":yaml.safe_dump(template)}}}))
                    self.assertEqual(expected,module.agent_requests(file)["small"]["memory"])

    def derive(self, count=40, machine="e2-standard-8", resources=None):
        shape = {"machine_type":machine,"vcpus":int(machine.rsplit("-",1)[1]),"memory_mib":4096*int(machine.rsplit("-",1)[1])}
        with patch.object(controller,"machine_shape",return_value=shape), patch.object(controller,"candidate_shapes",return_value=[shape]):
            return controller.derive(count,{"machine_types":[machine]}, {"controller":{"javaOpts":"-Xmx8G", "resources": resources or {}}},"zone","project",0.04)

    def test_controller_missing_requests_default_to_limits(self):
        d=self.derive(resources={"limits":{"cpu":"8","memory":"20G"}})
        self.assertFalse(d["requests_schedulable"])
        self.assertEqual(8000,d["request_cpu_millicores"])

    def test_large_node_cannot_override_pod_limits(self):
        d=self.derive(count=2000,machine="e2-standard-32",resources={"requests":{"cpu":"4","memory":"16G"},"limits":{"cpu":"8","memory":"20G"}})
        self.assertFalse(d["fits"])

    def test_unlimited_report(self):
        with contextlib.redirect_stdout(io.StringIO()) as out:
            controller.print_check(self.derive())
        self.assertIn("unlimited",out.getvalue())

    def test_memory_reservation_matches_gke_tiers(self):
        module=load(ROOT/"gke/3-smoke/check-pool-fit.py","gkefit")
        for size, expected in ((512,255),(8192,1843.2),(16384,2662.4)):
            self.assertAlmostEqual(expected,controller.gke_reserved_memory_mib(size))
            self.assertAlmostEqual(expected,module.gke_reserved_memory_mib(size))


if __name__ == "__main__":
    unittest.main()
