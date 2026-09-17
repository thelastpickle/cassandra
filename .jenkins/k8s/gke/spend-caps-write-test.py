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
"""Caps-file write failures and HCL string round trips."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


class CapsWriteTest(unittest.TestCase):
    def test_atomic_replace_and_escaped_email(self):
        for cloud in ("gke", "eks"):
            spec=importlib.util.spec_from_file_location("caps_"+cloud, Path(__file__).resolve().parents[1]/cloud/"spend-caps.py")
            module=importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            caps={"daily":100,"weekly":500,"monthly":2000,"email": '\"john${local.x}%{if true}\\x\"@example.org'}
            with tempfile.TemporaryDirectory() as tmp:
                path=Path(tmp)/"caps.tfvars"
                module.write(path,caps,5)
                saved=path.read_text()
                value=saved.split(module.EMAIL_VARIABLE)[-1].split("=",1)[1].strip()
                self.assertEqual(caps["email"],json.loads(value).replace("$${","${").replace("%%{","%{"))
                self.assertEqual(caps["email"],module.read_existing(path)["email"])
                with patch.object(module.os,"replace",side_effect=OSError("disk full")):
                    with self.assertRaises(OSError):
                        module.write(path,dict(caps,daily=200),5)
                self.assertEqual(saved,path.read_text())
                self.assertEqual([path],list(Path(tmp).iterdir()))
                declarations = [f'variable "{name}" {{ type = number }}'
                                for name in module.CAP_VARIABLE.values()]
                declarations.append(f'variable "{module.EMAIL_VARIABLE}" {{ type = string }}')
                (Path(tmp) / "variables.tf").write_text("\n".join(declarations))
                result = subprocess.run(
                    [os.environ.get("TOFU", "tofu"), "-chdir=" + tmp, "console", "-var-file=caps.tfvars"],
                    input=f"jsonencode(var.{module.EMAIL_VARIABLE})\n", text=True,
                    capture_output=True, check=True)
                self.assertEqual(caps["email"], json.loads(json.loads(result.stdout)))


if __name__ == "__main__":
    unittest.main()
