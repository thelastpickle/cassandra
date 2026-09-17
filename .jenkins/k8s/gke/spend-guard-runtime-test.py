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
"""Runtime API and spend-protection regressions; no cloud access."""
import base64
import importlib.util
import json
import os
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch, Mock

spec = importlib.util.spec_from_file_location("guard", Path(__file__).with_name("spend-guard.py"))
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


class RuntimeTest(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {"K_SERVICE": "guard"})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.client = guard.Gcp("test-project", "europe-west9")
        self.process = patch.object(guard.subprocess, "run", side_effect=AssertionError("runtime executed a CLI"))
        self.process.start()
        self.addCleanup(self.process.stop)

    def test_metadata_authentication(self):
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.read.return_value = b'{"access_token":"test-token"}'
        with patch("urllib.request.urlopen", return_value=response) as request:
            self.assertEqual("test-token", self.client.token())
            self.assertEqual("test-token", self.client.token())
        self.assertEqual(1, request.call_count)
        self.assertEqual("Google", request.call_args.args[0].get_header("Metadata-flavor"))

    def test_unknown_spend_brakes_and_reports_without_a_cli(self):
        maximum = 4

        def reply(method, url, body=None, headers=None, timeout=20):
            nonlocal maximum
            if url.startswith("http://metadata.google.internal/"):
                return {"access_token": "test-token"}
            self.assertEqual("Bearer test-token", headers["Authorization"])
            if url.endswith("/versions/latest:access"):
                value = {"daily": 100} if "/secrets/caps/" in url else {"version": 1}
                return {"payload": {"data": base64.b64encode(json.dumps(value).encode()).decode()}}
            if "monitoring.googleapis.com" in url and method == "GET":
                return {"timeSeries": []}
            if "compute.googleapis.com" in url:
                return {"items": {"zones/a": {"instances": [{"status": "RUNNING"}]}}}
            if url.endswith(":setAutoscaling"):
                maximum = body["autoscaling"]["maxNodeCount"]
                return {"name": "operation"}
            if url.endswith("/nodePools"):
                return {"nodePools": [{"name": "small", "autoscaling": {"enabled": True, "maxNodeCount": maximum}}]}
            if url.endswith("/versions"):
                return {"versions": []}
            if method == "POST" and url.endswith((":addVersion", "/timeSeries", ":publish")):
                return {}
            self.fail(f"unexpected runtime request: {method} {url}")

        with patch.dict(os.environ, {
                "GOOGLE_PROJECT": "test-project", "GKE_LOCATION": "europe-west9",
                "GKE_CLUSTER_NAME": "ci", "GKE_SPEND_CAPS_SECRET": "caps",
                "GKE_SPEND_STATE_SECRET": "state", "GKE_SPEND_AGENT_POOL_NAMES": "small",
                "GKE_SPEND_AGENT_POOL_MAXIMA": '{"small":4}', "GKE_SPEND_ALERT_TOPIC": "alerts",
                "GKE_SPEND_BILLING_TABLE": ""}):
            config = guard.config_from_environment()
        with patch.object(self.client, "http_json", side_effect=reply) as calls:
            result = guard.enforce(self.client, config, datetime(2026, 9, 17, 12, tzinfo=timezone.utc))
        self.assertTrue(result["unknown"])
        self.assertTrue(result["braked"])
        self.assertEqual(0, maximum)
        posted = [call.args[1] for call in calls.call_args_list if call.args[0] == "POST"]
        self.assertTrue(any(url.endswith(":addVersion") for url in posted))
        self.assertTrue(any(url.endswith("/timeSeries") for url in posted))
        self.assertTrue(any(url.endswith(":publish") for url in posted))

    def test_caps_and_state_use_secret_api(self):
        payload = base64.b64encode(b'{"daily":100}').decode()
        with patch.object(self.client, "rest", return_value={"payload": {"data": payload}}) as call:
            self.assertEqual(100, guard.read_caps(self.client, "caps")["daily"])
        self.assertTrue(call.call_args.args[1].endswith("/secrets/caps/versions/latest:access"))

    def test_cap_event_is_reported_before_braking_finishes(self):
        result = {"windows": [], "over": ["daily"], "braked": False,
                  "unknown": [], "vcpu_hours_today": 100}
        config = {"project": "test-project", "cluster": "ci", "metric_prefix": "custom.googleapis.com/guard"}
        with patch.object(self.client, "rest") as call:
            guard.publish_metrics(self.client, config, result)
        metrics = {series["metric"]["type"]: series["points"][0]["value"]["doubleValue"]
                   for series in call.call_args.args[2]["timeSeries"]}
        self.assertEqual(1, metrics["custom.googleapis.com/guard/cap_exceeded"])
        self.assertEqual(0, metrics["custom.googleapis.com/guard/braked"])

    def test_compute_listing_follows_pages(self):
        with patch.object(self.client, "rest", side_effect=[{"nextPageToken":"next"}, {"items":{"zones/a":{"instances":[{"status":"RUNNING"}]}}}]):
            self.assertTrue(guard.anything_running(self.client))

    def test_pool_update_and_listing(self):
        with patch.object(self.client, "rest", return_value={"nodePools":[{"name":"small", "autoscaling":{"enabled":True,"maxNodeCount":4}}]}) as call:
            self.assertEqual({"small":4}, guard.pool_ceilings(self.client,"cluster",["small"]))
            self.assertEqual("set", guard.set_pool_maximum(self.client,"cluster","small",0))
        self.assertEqual({"autoscaling":{"enabled":True,"minNodeCount":0,"maxNodeCount":0}}, call.call_args.args[2])

    def test_billing_query_is_project_scoped_and_in_usd(self):
        responses = [{"jobComplete":False,"jobReference":{"jobId":"job","location":"EU"}},
                     {"jobComplete":True,"rows":[{"f":[{"v":"2026-09-01"},{"v":"10"}]}]}]
        with patch.object(self.client, "rest", side_effect=responses) as call:
            costs = guard.cost_by_day(self.client,"billing.dataset.table",datetime(2026,9,1).date(),datetime(2026,9,2).date())
        self.assertEqual({"2026-09-01":10}, costs)
        body=call.call_args_list[0].args[2]
        self.assertIn("project.id = @project", body["query"])
        self.assertIn("currency_conversion_rate", body["query"])
        self.assertEqual("test-project", body["queryParameters"][0]["parameterValue"]["value"])
        self.assertIn("location=EU",call.call_args_list[1].args[1])

    def test_state_prunes_only_after_successful_write(self):
        versions=[{"name":"projects/test-project/secrets/state/versions/"+str(i),"state":"ENABLED"} for i in range(1,8)]
        versions[0]["state"] = "DISABLED"
        with patch.dict(os.environ, {"GKE_SPEND_STATE_BOOTSTRAP_VERSION": "2"}):
            with patch.object(self.client,"rest",side_effect=[{"name":versions[-1]["name"]},{"versions":versions}, {}, {}, {}]) as call:
                guard.write_state(self.client,"state",{"cost_by_day":{}})
        destroyed=[c.args[1] for c in call.call_args_list if c.args[1].endswith(":destroy")]
        self.assertEqual([versions[i]["name"] for i in (0, 2, 3)], [u.split("/v1/")[1].removesuffix(":destroy") for u in destroyed])
        with patch.object(self.client,"rest",side_effect=guard.GcpError("write failed")) as call:
            with self.assertRaises(guard.GcpError):
                guard.write_state(self.client,"state",{})
        self.assertEqual(1,call.call_count)

    def test_stale_usage_retains_latest_observation(self):
        end=datetime(2026,9,17,12,tzinfo=timezone.utc)
        with patch.object(self.client,"rest",return_value={"timeSeries":[{"points":[{"interval":{"endTime":"2026-09-16T12:00:00Z"},"value":{"doubleValue":8}}]}]}):
            result=guard.vcpu_usage(self.client,end-timedelta(days=2),end)
        self.assertEqual(end-timedelta(days=1),result["latest"])


class ProtectionTest(unittest.TestCase):
    def test_one_node_fallback_does_not_stop_a_one_node_pool(self):
        self.assertFalse(guard.pool_brakes({"small": 1}, {"small": 1}, ["small"])["small"])

    def test_malformed_config_does_not_restore_old_state(self):
        with patch.dict(os.environ, {
                "GOOGLE_PROJECT": "test", "GKE_LOCATION": "region", "GKE_CLUSTER_NAME": "ci",
                "GKE_SPEND_CAPS_SECRET": "caps", "GKE_SPEND_STATE_SECRET": "state",
                "GKE_SPEND_AGENT_POOL_MAXIMA": "not json"}):
            with self.assertRaisesRegex(ValueError, "integer maxima"):
                guard.config_from_environment()

    def setUp(self):
        self.now = datetime(2026,9,17,12,tzinfo=timezone.utc)
        self.config = {"project":"test-project", "location":"europe-west9", "cluster":"ci", "state_secret":"state",
            "caps_secret":"caps", "pool_names":["small"], "pool_maxima":{"small":1}, "billing_table":"",
            "price":0.04, "fixed":5, "refresh_hours":6, "alert_topic":"topic"}
        self.client = guard.Gcp("test-project", "europe-west9")
        self.state = {"version":1,"cost_by_day":{},"pool_maxima":{"small":10}}
        for name, value in (("read_state",self.state), ("read_caps",{"daily":1000}),
                            ("pool_ceilings",{"small":1}), ("write_state",None), ("publish_metrics",None), ("notify",None)):
            target=patch.object(guard,name,return_value=value)
            target.start()
            self.addCleanup(target.stop)

    def test_old_samples_do_not_release_running_instances(self):
        usage={"hours":{self.now.date()-timedelta(days=1):192},"latest":self.now-timedelta(days=1),"points":24,"alignment":3600}
        with patch.object(guard,"vcpu_usage",return_value=usage), patch.object(guard,"anything_running",return_value=True), patch.object(guard,"apply_brake",return_value={"set":["small"],"raised_floor":[],"busy":[],"failed":{}}) as brake:
            result=guard.enforce(self.client,self.config,self.now)
        self.assertTrue(result["unknown"])
        self.assertEqual({"small":0},brake.call_args.args[2])

    def test_recent_samples_crossing_midnight_are_valid(self):
        now=self.now.replace(hour=0,minute=5)
        usage={"hours":{now.date()-timedelta(days=1):8},"latest":now-timedelta(minutes=10),"points":1,"alignment":3600}
        with patch.object(guard,"vcpu_usage",return_value=usage), patch.object(guard,"anything_running",side_effect=AssertionError("fresh metric needs no compute read")):
            result=guard.evaluate(self.client,self.config,now)
        self.assertFalse(result["unknown"])

    def test_redeployment_cannot_restore_an_obsolete_maximum(self):
        usage={"hours":{self.now.date():8},"latest":self.now,"points":1,"alignment":3600}
        with patch.object(guard,"vcpu_usage",return_value=usage), patch.object(guard,"apply_brake") as brake:
            result=guard.enforce(self.client,self.config,self.now)
        brake.assert_not_called()
        self.assertEqual({"small":1},result["pool_maxima"])
        self.assertEqual({"small":1},self.state["pool_maxima"])


if __name__ == "__main__":
    unittest.main()
