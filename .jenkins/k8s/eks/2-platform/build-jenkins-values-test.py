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

"""Check that concurrent pipelines cannot consume every small worker slot."""

import copy
import importlib.util
from pathlib import Path
import unittest

import yaml

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("values_builder", HERE / "build-jenkins-values.py")
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)


class PipelineCapacityTest(unittest.TestCase):
    def setUp(self):
        self.deployment = yaml.safe_load((HERE.parent.parent / "jenkins-deployment.yaml").read_text())

    def templates(self, slots, workers_per_build=3, container_cap=None):
        pools = {"small": {"max_size": slots, "small_workers_per_build": workers_per_build}}
        merged = copy.deepcopy(self.deployment["agent"]["podTemplates"])
        options = {"container_cap": container_cap} if container_cap is not None else {}
        merged.update(builder.scaled_pod_templates(self.deployment, pools, **options))
        return [template for body in merged.values() for template in yaml.safe_load(body)]

    def test_concurrent_pipelines_leave_a_worker(self):
        templates = self.templates(4)
        outer = [t for t in templates if "cassandra-small" in t["label"].split()]
        workers = [t for t in templates if "cassandra-amd64-small" in t["label"].split()]
        # Saturate outer agents. Additional pipelines must wait while all three JAR workers can run.
        self.assertEqual(1, sum(t["instanceCap"] for t in outer))
        self.assertEqual(3, sum(t["instanceCap"] for t in workers))
        self.assertTrue(set(t["name"] for t in outer).isdisjoint(t["name"] for t in workers))
        for template in outer + workers:
            self.assertEqual(str(template["instanceCap"]), template["instanceCapStr"])
            self.assertEqual("EXCLUSIVE", template["nodeUsageMode"])

    def test_concurrency_follows_capacity_without_consuming_worker_budget(self):
        for slots in (4, 7, 8, 10, 20, 100):
            with self.subTest(slots=slots):
                templates = self.templates(slots)
                outer = sum(t["instanceCap"] for t in templates if "cassandra-small" in t["label"].split())
                workers = sum(t["instanceCap"] for t in templates if "cassandra-amd64-small" in t["label"].split())
                self.assertEqual(slots, outer + workers)
                self.assertGreaterEqual(workers, 3 * outer)
                # One more pipeline would leave fewer than three workers per admitted build.
                self.assertLess(slots - outer - 1, 3 * (outer + 1))

    def test_global_cap_also_bounds_admitted_pipelines(self):
        templates = self.templates(20, container_cap=8)
        outer = sum(t["instanceCap"] for t in templates if "cassandra-small" in t["label"].split())
        workers = sum(t["instanceCap"] for t in templates if "cassandra-amd64-small" in t["label"].split())
        self.assertEqual((2, 6), (outer, workers))

    def test_worker_budget_can_follow_a_different_build_matrix(self):
        templates = self.templates(21, workers_per_build=6)
        self.assertEqual(3, sum(t["instanceCap"] for t in templates
                                if "cassandra-small" in t["label"].split()))
        self.assertEqual(18, sum(t["instanceCap"] for t in templates
                                 if "cassandra-amd64-small" in t["label"].split()))

    def test_too_few_slots_stop_generation(self):
        for slots in (0, 2, 3):
            with self.subTest(slots=slots), self.assertRaisesRegex(ValueError, "at least 4"):
                self.templates(slots)

    def test_ambiguous_outer_templates_are_rejected(self):
        templates = self.deployment["agent"]["podTemplates"]
        templates["another-outer"] = templates["agent-dind-small"]
        with self.assertRaisesRegex(ValueError, "expected one cassandra-small"):
            self.templates(4)

    def test_outer_template_must_select_the_small_pool(self):
        templates = self.deployment["agent"]["podTemplates"]
        templates["agent-dind-small"] = templates["agent-dind-small"].replace(
            "cassandra.jenkins.agent.small=true", "cassandra.jenkins.agent.medium=true")
        with self.assertRaisesRegex(ValueError, "small-pool template"):
            self.templates(4)

    def test_split_preserves_pod_configuration_and_stable_ids(self):
        original = yaml.safe_load(self.deployment["agent"]["podTemplates"]["agent-dind-small"])[0]
        first = [t for t in self.templates(4) if t["nodeSelector"] == original["nodeSelector"]]
        second = [t for t in self.templates(8) if t["nodeSelector"] == original["nodeSelector"]]
        self.assertEqual(2, len(first))
        self.assertEqual({t["id"] for t in first}, {t["id"] for t in second})
        self.assertEqual(2, len({t["id"] for t in first}))
        for template in first:
            for key in ("containers", "volumes", "yaml", "slaveConnectTimeout"):
                self.assertEqual(original[key], template[key])


if __name__ == "__main__":
    unittest.main()
