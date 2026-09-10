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

"""Kubernetes quantities, Helm's merge rule, and a JVM heap size.

None of this is about a cloud.  It is here because three scripts held four implementations of two of these
functions, and the two memory ones disagreed about their own unit: one returned bytes and one returned MiB,
under names that said neither.

The unit is in every name for that reason.  A memory quantity read in the wrong base is wrong by 7% at Gi,
which is the difference between an agent pool that fits its node and one that does not, and it produces no
error anywhere.
"""

import copy
import re

MIB = 1024 ** 2
GIB = 1024 ** 3


def cpu_millicores(value, default=None):
    """A Kubernetes CPU quantity as millicores.  `2` is 2000, `500m` is 500.

    `default` is returned for None, which is how an absent limit or request is spelled.  Kubernetes treats an
    absent limit as unlimited and an absent request as zero, and the caller knows which it is asking about.
    """
    if value is None:
        return default
    text = str(value).strip()
    if text.endswith("m"):
        return float(text[:-1])
    return float(text) * 1000


def memory_bytes(value, default=None):
    """A Kubernetes memory quantity as bytes.

    The suffix decides the base, and the two differ by 7% at Gi: `16G` is 16 000 000 000 bytes while `16Gi`
    is 17 179 869 184.  Every quantity in jenkins-deployment.yaml's pod templates is decimal (`1G`, `3400M`,
    `16G`) and every quantity a kubelet reports is binary (`32120052Ki`).  Reading either as the other is the
    difference between a pool that fits and a pool that does not.

    A bare number is bytes, which is what Kubernetes means by one.  An earlier version of this shared a code
    path with cpu_millicores() and multiplied a bare number by 1000, so `memory: 20` read as 20 kilobytes.
    """
    if value is None:
        return default
    text = str(value).strip()
    binary = {"Ki": 1024, "Mi": 1024 ** 2, "Gi": 1024 ** 3, "Ti": 1024 ** 4, "Pi": 1024 ** 5}
    decimal = {"k": 1000, "K": 1000, "M": 1000 ** 2, "G": 1000 ** 3, "T": 1000 ** 4, "P": 1000 ** 5}
    for suffix, factor in binary.items():
        if text.endswith(suffix):
            return int(float(text[: -len(suffix)]) * factor)
    for suffix, factor in decimal.items():
        if text.endswith(suffix):
            return int(float(text[: -len(suffix)]) * factor)
    # Bare bytes, and the exponent form Kubernetes also accepts (`1e9`).
    return int(float(text))


def memory_mib(value, default=None):
    """A Kubernetes memory quantity as whole MiB, for the callers that report in MiB."""
    if value is None:
        return default
    return int(memory_bytes(value) / MIB)


def human_cpu(millicores: float) -> str:
    return f"{millicores / 1000:.2f}".rstrip("0").rstrip(".") + " cpu"


def human_memory(byte_count: float) -> str:
    return f"{byte_count / GIB:.1f} GiB"


def human_storage(byte_count: float) -> str:
    return f"{byte_count / GIB:.0f} GiB"


def heap_mib(java_opts: str) -> int:
    """The -Xmx in a JVM argument string, in MiB.  0 when none is set, which the caller reports.

    A bare -Xmx is bytes, and `k`, `m` and `g` are binary, which is the JVM's own grammar rather than
    Kubernetes'.  The two appear in the same file: -Xmx8G is 8 GiB and `memory: 8G` beside it is 8 GB.
    """
    match = re.search(r"-Xmx(\d+)([kmgKMG]?)", java_opts or "")
    if not match:
        return 0
    scale = {"": 1 / MIB, "k": 1 / 1024, "m": 1, "g": 1024}[match.group(2).lower()]
    return int(int(match.group(1)) * scale)


def deep_merge(base, overlay):
    """Merge overlay into a copy of base, the way Helm merges two -f files.

    Maps merge key by key; everything else, lists included, is replaced whole.  That is Helm's rule and not a
    simplification of it: every caller's answer is read by Helm afterwards, so matching Helm is the point.

    A deep copy, so that no part of the result aliases `base`.  A shallow one would leave every key that the
    overlay does not mention pointing into the input, and a caller that then edited the result would edit the
    file it had loaded.  These are values files, so the copy costs nothing worth measuring.
    """
    merged = copy.deepcopy(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_yaml(path):
    """One values file, or an empty map for an empty one.

    PyYAML is imported here rather than at module scope so that a caller needing none of this can still
    import the rest: `yaml` is a .build/run-ci dependency and not part of the standard library.
    """
    import yaml
    with open(path, encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}
