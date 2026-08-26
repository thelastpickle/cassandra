#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
#
# Used to test `.build/run-ci`
# Run with `python .build/run-ci.d/run-ci-test.py`
#
#
# lint with:
#  `pylint --disable=C0301,W0511,C0114,C0103,W0702,C0415,C0116,C0115,R0914,W0603,R0915,R0913,R0911 run-ci-test.py`


import argparse
import contextlib
import io
from pathlib import Path
import unittest
from unittest.mock import patch, MagicMock
from urllib.error import HTTPError

import jenkins

# Import the functions from the script
from run_ci import (
    debug,
    install_jenkins,
    get_jenkins,
    trigger_jenkins_build,
    spin_while,
    retry_call,
    wait_for_build_number,
    wait_for_build_complete,
    delete_remote_junit_files,
    cleanup_and_maybe_teardown,
    helm_installation_lock,
)


def quietly(action):
    """
    Calls action() with the spinner's output discarded and its sleep skipped.
    spin_while writes ten cursor-control frames per poll, which floods the test output.
    """
    with patch('run_ci.time.sleep'), contextlib.redirect_stdout(io.StringIO()):
        return action()


class TestCIPipeline(unittest.TestCase):

    def setUp(self):
        print("\ntesting ", self._testMethodName)

    @patch('run_ci.os.environ.get')
    @patch('run_ci.print')
    def test_debug(self, mock_print, mock_get):
        mock_get.return_value = "1"
        debug("Test message")
        mock_print.assert_called_with("Test message")

    @patch('run_ci.subprocess.run')
    def test_install_jenkins(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        install_jenkins("test-namespace", Path("/fake/cassandra/dir"), "default")
        mock_run.assert_any_call(["helm", "repo", "add", "jenkins", "https://charts.jenkins.io"], check=True)
        mock_run.assert_any_call(["helm", "repo", "update"], check=True)

    @patch('run_ci.subprocess.run')
    @patch('run_ci.jenkins.Jenkins')
    def test_get_jenkins(self, mock_jenkins, mock_run):
        mock_k8s_client = MagicMock()
        mock_run.return_value = MagicMock(stdout="fake-password")
        mock_jenkins_instance = MagicMock()
        mock_jenkins.return_value = mock_jenkins_instance
        # hack – use False values instead of None
        args = argparse.Namespace(kubeconfig="/fake/kubeconfig", kubecontext="test-context", user=False, url=False)
        _, server = get_jenkins(mock_k8s_client, args, "default")
        self.assertEqual(server, mock_jenkins_instance)

    @patch('run_ci.jenkins.Jenkins.build_job')
    @patch('run_ci.wait_for_build_number')
    def test_trigger_jenkins_build(self, mock_wait_for_build_number, mock_build_job):
        mock_server = MagicMock()
        mock_build_job.return_value = mock_server.build_job.return_value = 123
        mock_wait_for_build_number.return_value = 456
        # a MagicMock job_info shows no parameterDefinitions, so this takes the
        # non-parameter build path, which sleeps for six seconds
        with patch('run_ci.spin_while', side_effect=lambda msg, condition: 0):
            queue_item = quietly(lambda: trigger_jenkins_build(mock_server, "test-job", param1="value1"))
        self.assertEqual(queue_item, 123)

    def test_spin_while(self):
        result = quietly(lambda: spin_while("Testing", lambda: True))
        self.assertEqual(result, 0)

    def test_wait_for_build_complete_ignores_mid_build_result(self):
        """A pipeline latches UNSTABLE while later stages still run. That is not completion."""
        mock_server = MagicMock()
        mock_server.get_build_info.side_effect = [
            {'building': True, 'result': None},
            {'building': True, 'result': 'UNSTABLE'},
            {'building': True, 'result': 'UNSTABLE'},
            {'building': False, 'result': 'UNSTABLE'},
        ]
        quietly(lambda: wait_for_build_complete(mock_server, "test-job", 456))
        self.assertEqual(mock_server.get_build_info.call_count, 4)

    def test_wait_for_build_complete_missing_building_field(self):
        """An absent `building` field must never read as finished."""
        mock_server = MagicMock()
        mock_server.get_build_info.side_effect = [
            {'result': 'SUCCESS'},
            {'building': False, 'result': 'SUCCESS'},
        ]
        quietly(lambda: wait_for_build_complete(mock_server, "test-job", 456))
        self.assertEqual(mock_server.get_build_info.call_count, 2)

    def test_wait_for_build_complete_survives_api_error(self):
        mock_server = MagicMock()
        mock_server.get_build_info.side_effect = [
            jenkins.JenkinsException("connection reset"),
            jenkins.JenkinsException("connection reset"),
            {'building': False, 'result': 'SUCCESS'},
        ]
        quietly(lambda: wait_for_build_complete(mock_server, "test-job", 456))
        self.assertEqual(mock_server.get_build_info.call_count, 3)

    def test_wait_for_build_number_pending_executable(self):
        """A queued item can carry "executable": null before Jenkins starts it."""
        mock_server = MagicMock()
        mock_server.get_queue_item.side_effect = [
            {'executable': None},
            {'executable': {'number': 456}},
        ]
        self.assertEqual(quietly(lambda: wait_for_build_number(mock_server, 123)), 456)

    def test_retry_call_returns_result(self):
        self.assertEqual(retry_call(lambda: "downloaded", "download it", IOError, 3, 0), "downloaded")

    def test_retry_call_raises_after_retries(self):
        attempts = []

        def always_fails():
            attempts.append(1)
            raise IOError("connection reset by peer")

        with self.assertRaises(IOError):
            retry_call(always_fails, "download it", IOError, 3, 0)
        self.assertEqual(len(attempts), 3)

    def test_retry_call_does_not_retry_a_404(self):
        """An artifact a build never archived stays absent, so one attempt is enough."""
        attempts = []

        def not_found():
            attempts.append(1)
            raise HTTPError("http://ci/artifact", 404, "Not Found", {}, None)

        with self.assertRaises(HTTPError):
            retry_call(not_found, "download it", IOError, 5, 0)
        self.assertEqual(len(attempts), 1)

    def test_retry_call_retries_a_503(self):
        """A gateway error is transient, unlike a 404."""
        attempts = []

        def unavailable():
            attempts.append(1)
            raise HTTPError("http://ci/artifact", 503, "Service Unavailable", {}, None)

        with self.assertRaises(HTTPError):
            retry_call(unavailable, "download it", IOError, 3, 0)
        self.assertEqual(len(attempts), 3)

    def test_retry_call_always_attempts_once(self):
        """max_retries=0 must not raise None."""
        with self.assertRaises(IOError):
            retry_call(lambda: (_ for _ in ()).throw(IOError("nope")), "download it", IOError, 0, 0)

    @patch('run_ci.stream.stream')
    def test_delete_remote_junit_files(self, mock_stream):
        mock_k8s_client = MagicMock()
        delete_remote_junit_files(mock_k8s_client, "test-pod", "test-namespace", "test-job", 456)
        delete_remote_junit_files(mock_k8s_client, "test-pod", "test-namespace", "cassandra-5.0", 456)
        mock_stream.assert_called()

    @patch('run_ci.subprocess.run')
    def test_cleanup_and_maybe_teardown(self, mock_run):
        cleanup_and_maybe_teardown(None, None, "test-namespace", True)
        mock_run.assert_called_with(["helm", "--namespace", "test-namespace", "uninstall", "cassius"], check=True)

    @patch('run_ci.fcntl.flock')
    def test_helm_installation_lock(self, mock_flock):
        with helm_installation_lock(Path("/tmp/.fake.lock")):
            mock_flock.assert_called()

if __name__ == '__main__':
    unittest.main()
