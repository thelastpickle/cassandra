<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# The cloud-neutral half of the CI cluster tooling

tl;dr: arithmetic that every `.jenkins/k8s/<cloud>` directory needs and none of them owns.  Nothing here names AWS, GCP, or any API.

`eks/` was written first, and the modules here are the parts of it that would otherwise be copied into `gcp/` verbatim.  Each one was extracted because a duplicate already existed, or because it is a body of arithmetic with no cloud reference in it at all.

The directory is `shared` and not `lib` because the repository's root `.gitignore` carries a bare `lib/`, for Cassandra's jar directory, and a bare directory pattern matches at every depth.  Named `lib`, everything here is ignored, `git add` skips it without a word, and the first anybody knows is a clone that cannot run its own tests.

| Module | What it holds | Why it is here and not in a cloud directory |
|---|---|---|
| `k8s_values.py` | Kubernetes quantity parsing, human formatting, the Helm merge rule, a JVM heap size | Quantities and Helm are Kubernetes and Helm, not a cloud.  Three scripts had four implementations of two of these. |
| `controller_model.py` | how much CPU and memory a Jenkins controller needs for N agents | The model is about Jenkins.  The node it is compared against is the cloud's, and stays there. |
| `spend_model.py` | UTC spend windows, whether a day's bill has settled, and the least-squares fit of a bill onto usage | Windows are UTC arithmetic and the fit is least squares.  What is billed and what is measured are the cloud's. |
| `shell_exports.py` | writing `export NAME='value'` lines a shell can source | Every script does this, and quoting a value correctly is the whole of it. |

## What is deliberately not here

**The cloud client.**  `eks/spend-guard.py` calls AWS through boto3 in Lambda and the `aws` CLI on a laptop, keyed on one environment variable.  The shape of that will recur, and the code will not: operation names, pagination tokens and the SDK's own import path differ per cloud.  A shared client would be an interface with one implementation, which is a guess about the second.

**Node allocatable, and reserved capacity.**  What a kubelet holds back before a pod may have anything is a fact about the node image, and EKS and GKE reserve differently.  `eks/3-smoke/check-pool-fit.py` models the EKS figures; the GCP equivalent will model its own.

**Instance-size ladders and machine-type names.**  `m7a.4xlarge` and `n2-standard-8` do not share a grammar.

## Using it

The scripts are executables rather than an installed package, so each adds this directory to `sys.path` by walking up from its own location:

```python
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "shared"))    # from .jenkins/k8s/<cloud>/
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))    # from a subdirectory of one
```

There is no `__init__.py` and no package name: `import k8s_values` is what the scripts do.  Adding a dependency here that is not in the standard library would break that, because these scripts are run from a laptop with nothing installed but what `.build/run-ci` already needs.

## Testing

Every module is exercised through the callers' own suites, which `make -C .jenkins/k8s/<cloud> test` runs and `.github/workflows/jenkins-check.yaml` runs on every change under `.jenkins/`.  The quantity parsing has its own cases in `shared-test.sh` beside this file, because it is the one part with no caller-visible output to assert against and a 7% error in it reads as a pool that fits.
