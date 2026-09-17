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
"""Deployment context selection and failed OpenTofu output reads."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]


class MakeEnvTest(unittest.TestCase):
    def setup_tree(self, tmp, cloud):
        root=Path(tmp)
        (root/"Makefile").write_text((ROOT/cloud/"Makefile").read_text())
        binary=root/"bin"
        binary.mkdir()
        scripts={
            "tofu": """#!/bin/sh
case "$*" in
  *'output -raw environment'*)
    [ "${FAIL_ENV:-0}" = 0 ] || exit 23
    printf "export GOOGLE_PROJECT=test\nexport GOOGLE_REGION=europe-west9\nexport GKE_CLUSTER_NAME=ci\n" ;;
  *) echo '{}' ;;
esac
""",
            "helm": "#!/bin/sh\necho 'diff 3.0'\n",
            "helmfile": "#!/bin/sh\nprintf '%s\\n' \"$*\" > \"$CALLS\"\n".replace('\\"','"'),
        }
        for name, text in scripts.items():
            path=binary/name
            path.write_text(text)
            path.chmod(0o755)
        env=dict(os.environ,PATH=str(binary)+os.pathsep+os.environ["PATH"],CALLS=str(root/"calls"))
        return root,env

    def test_failed_output_preserves_generated_files(self):
        for cloud in ("eks","gke"):
            with tempfile.TemporaryDirectory() as tmp:
                root,env=self.setup_tree(tmp,cloud)
                for name in ("env","pools","controller"):
                    (root/name).write_text("unchanged")
                env["FAIL_ENV"]="1"
                result=subprocess.run(["make","env","ENV_FILE=env","POOLS_FILE=pools","CTL_FILE=controller"],cwd=root,env=env,capture_output=True,text=True)
                self.assertNotEqual(0,result.returncode)
                self.assertEqual(["unchanged"]*3,[(root/name).read_text() for name in ("env","pools","controller")])

    def test_external_dns_names_target_context(self):
        with tempfile.TemporaryDirectory() as tmp:
            root,env=self.setup_tree(tmp,"gke")
            result=subprocess.run(["make","externaldns"],cwd=root,env=env,capture_output=True,text=True)
            self.assertEqual(0,result.returncode,result.stderr)
            self.assertIn("--kube-context gke_test_europe-west9_ci",(root/"calls").read_text())


if __name__ == "__main__":
    unittest.main()
