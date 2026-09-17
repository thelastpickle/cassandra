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
"""Measure project spending and enforce agent-pool caps. Cloud Functions uses REST APIs; local reports use cloud CLIs."""

import argparse
import base64
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

_HERE = Path(__file__).resolve().parent
for _candidate in (_HERE, _HERE.parent / "shared", _HERE.parent.parent / "shared"):
    if (_candidate / "spend_model.py").is_file():
        sys.path.insert(0, str(_candidate))
        break

from spend_model import (  # noqa: E402
    WINDOWS,
    audit_model,
    calibrate,
    day_figures,
    days_between,
    is_settled,
    scale_to_bill,
    spend_in_window,
    window_start,
)

# The kept figures.  A version, so a shape change is detected rather than misread; 45 days, because the fit
# looks back a fortnight and the windows reach back a month, and a secret version is cheap but not free.
STATE_VERSION = 1
STATE_DAYS = 45

# The gauge Compute Engine publishes for allocated vCPU.  A gauge and not a counter, which is what makes the
# alignment below the whole of the arithmetic.
USAGE_METRIC = "compute.googleapis.com/instance/cpu/reserved_cores"

USAGE_ALIGNMENT_SECONDS = 3600

# How many pool-update failures to name in one message before saying "and N more".  A hundred-pool cluster that
# cannot be braked at all should not put a hundred lines into a notification.
FAILURES_NAMED = 5


class GcpError(RuntimeError):
    """A cloud call failed.  Carries what was being asked, because the caller reports it to a human."""


def utc_now() -> datetime:
    """Now, in UTC, or whatever GKE_SPEND_NOW says."""
    override = os.environ.get("GKE_SPEND_NOW")
    if not override:
        return datetime.now(timezone.utc)
    parsed = as_datetime(override)
    if parsed is None:
        raise ValueError("GKE_SPEND_NOW is not an ISO 8601 datetime: %r" % override)
    return parsed


def as_datetime(value):
    """A datetime from what an API or an environment variable gives, or None."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    text = re.sub(r"Z$", "+00:00", text)
    text = re.sub(r"([+-]\d{2})(\d{2})$", r"\1:\2", text)
    # Monitoring returns fractional seconds with any number of digits; fromisoformat wants 3 or 6 of them.
    text = re.sub(r"\.(\d+)", lambda m: "." + m.group(1)[:6].ljust(6, "0"), text)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def rfc3339(moment: datetime) -> str:
    """The form Monitoring's interval arguments take."""
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Gcp:
    """Every cloud call, in one place, so what this guard may do is one list."""

    def __init__(self, project: str, location: str):
        self.project = project
        self.location = location
        self.in_function = bool(os.environ.get("K_SERVICE"))
        self._token = None

    # -- subprocesses -------------------------------------------------------------------------------

    def run(self, args: list, stdin: str = None) -> str:
        """A command, with its stderr folded into the error rather than the answer."""
        try:
            done = subprocess.run(args, input=stdin, capture_output=True, text=True, check=True)
        except FileNotFoundError:
            raise GcpError("%s is not on the PATH" % args[0])
        except subprocess.CalledProcessError as error:
            raise GcpError("%s failed: %s" % (" ".join(args[:3]), (error.stderr or error.stdout or "").strip()))
        return done.stdout

    def gcloud(self, args: list, project: bool = True) -> str:
        command = ["gcloud"] + args
        if project:
            command += ["--project", self.project]
        return self.run(command)

    def gcloud_json(self, args: list, project: bool = True):
        text = self.gcloud(args + ["--format=json"], project=project).strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except ValueError as error:
            raise GcpError("gcloud did not answer with JSON: %s" % error)

    def token(self) -> str:
        """Use the runtime identity in Cloud Functions and gcloud credentials locally."""
        if self._token is None:
            if self.in_function:
                answer = self.http_json("GET",
                    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
                    headers={"Metadata-Flavor": "Google"}, timeout=3)
                self._token = answer.get("access_token")
            else:
                self._token = self.gcloud(["auth", "print-access-token"], project=False).strip()
            if not self._token:
                raise GcpError("no access token returned")
        return self._token

    @staticmethod
    def http_json(method, url, body=None, headers=None, timeout=20):
        headers = dict(headers or {})
        if body is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, method=method, headers=headers,
            data=json.dumps(body).encode() if body is not None else None)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            raise GcpError("%s %s: %s" % (method, url.split("?")[0], error.read().decode(errors="replace"))) from error
        except (urllib.error.URLError, OSError, ValueError) as error:
            raise GcpError("%s %s: %s" % (method, url.split("?")[0], error)) from error

    def rest(self, method: str, url: str, body: dict = None) -> dict:
        if self.in_function:
            return self.http_json(method, url, body, {"Authorization": "Bearer " + self.token()})
        args = ["curl", "--silent", "--show-error", "--fail-with-body", "--max-time", "60",
                "-H", "Authorization: Bearer " + self.token(), "-X", method, url]
        if body is not None:
            args += ["-H", "Content-Type: application/json", "--data-binary", "@-"]
        text = self.run(args, stdin=json.dumps(body) if body is not None else None).strip()
        try:
            return json.loads(text) if text else {}
        except ValueError as error:
            raise GcpError("%s %s did not answer with JSON: %s" % (method, url.split("?")[0], error)) from error

    def secret_url(self, secret):
        name = secret if secret.startswith("projects/") else "projects/%s/secrets/%s" % (self.project, secret)
        return "https://secretmanager.googleapis.com/v1/" + name

    def secret(self, secret):
        if not self.in_function:
            return self.gcloud(["secrets", "versions", "access", "latest", "--secret", secret])
        answer = self.rest("GET", self.secret_url(secret) + "/versions/latest:access")
        try:
            return base64.b64decode(answer["payload"]["data"], validate=True).decode()
        except (KeyError, ValueError, UnicodeError) as error:
            raise GcpError("invalid secret payload for %s" % secret) from error

    def pool_url(self, cluster):
        return "https://container.googleapis.com/v1/projects/%s/locations/%s/clusters/%s/nodePools" % (
            self.project, self.location, cluster)


def config_from_environment() -> dict:
    """What layer 1 put in the environment, and what the Cloud Function is given."""
    project = os.environ.get("GOOGLE_PROJECT", "").strip()
    location = (os.environ.get("GKE_LOCATION") or os.environ.get("GOOGLE_REGION") or "").strip()
    cluster = os.environ.get("GKE_CLUSTER_NAME", "").strip()
    caps_secret = os.environ.get("GKE_SPEND_CAPS_SECRET", "").strip()
    state_secret = os.environ.get("GKE_SPEND_STATE_SECRET", "").strip()

    missing = [name for name, value in (
        ("GOOGLE_PROJECT", project),
        ("GKE_LOCATION or GOOGLE_REGION", location),
        ("GKE_CLUSTER_NAME", cluster),
        ("GKE_SPEND_CAPS_SECRET", caps_secret),
        ("GKE_SPEND_STATE_SECRET", state_secret),
    ) if not value]
    if missing:
        raise ValueError(
            "the environment does not describe a cluster with a spend cap: %s unset.\n"
            "`make spend` sources .gke-env, which `make env` writes from layer 1's outputs; a cluster with no"
            " cap set has no guard and nothing to report." % ", ".join(missing))

    def number(name: str, default: float) -> float:
        raw = os.environ.get(name, "").strip()
        if not raw:
            return default
        try:
            return float(raw)
        except ValueError:
            raise ValueError("%s is not a number: %r" % (name, raw))

    maxima = {}
    raw_maxima = os.environ.get("GKE_SPEND_AGENT_POOL_MAXIMA", "").strip()
    if raw_maxima:
        try:
            maxima = {str(k): int(v) for k, v in json.loads(raw_maxima).items()}
        except (ValueError, AttributeError, TypeError):
            raise ValueError("GKE_SPEND_AGENT_POOL_MAXIMA must map pool names to integer maxima")

    return {
        "project": project,
        "location": location,
        "cluster": cluster,
        "caps_secret": caps_secret,
        "state_secret": state_secret,
        "pool_names": os.environ.get("GKE_SPEND_AGENT_POOL_NAMES", "").split(),
        "pool_maxima": maxima,
        "alert_topic": os.environ.get("GKE_SPEND_ALERT_TOPIC", "").strip(),
        "metric_prefix": os.environ.get(
            "GKE_SPEND_METRIC_PREFIX", "custom.googleapis.com/cassandra_jenkins/spend_guard").strip(),
        "billing_table": os.environ.get("GKE_SPEND_BILLING_TABLE", "").strip(),
        "price": number("GKE_SPEND_PRICE_PER_VCPU_HOUR", 0.04),
        "fixed": number("GKE_SPEND_FIXED_USD_PER_DAY", 5.0),
        "refresh_hours": number("GKE_SPEND_COST_REFRESH_HOURS", 6.0),
        "interval_minutes": number("GKE_SPEND_GUARD_INTERVAL_MINUTES", 5.0),
    }


def vcpu_usage(gcp: Gcp, start: datetime, end: datetime) -> dict:
    """vCPU-hours per UTC day, from the reserved_cores gauge."""
    url = (
        "https://monitoring.googleapis.com/v3/projects/{project}/timeSeries"
        "?filter={filter}"
        "&interval.startTime={start}&interval.endTime={end}"
        "&aggregation.alignmentPeriod={alignment}s"
        "&aggregation.perSeriesAligner=ALIGN_MEAN"
        "&aggregation.crossSeriesReducer=REDUCE_SUM"
    ).format(
        project=gcp.project,
        filter="metric.type%3D%22" + USAGE_METRIC + "%22",
        start=rfc3339(start),
        end=rfc3339(end),
        alignment=USAGE_ALIGNMENT_SECONDS,
    )

    hours = {}
    points_seen = 0
    latest = None
    page = None
    while True:
        answer = gcp.rest("GET", url + ("&pageToken=%s" % page if page else ""))
        for series in answer.get("timeSeries") or []:
            for point in series.get("points") or []:
                interval = point.get("interval") or {}
                bucket_end = as_datetime(interval.get("endTime"))
                bucket_start = as_datetime(interval.get("startTime")) or (
                    bucket_end - timedelta(seconds=USAGE_ALIGNMENT_SECONDS) if bucket_end else None)
                # GAUGE points may repeat endTime as startTime, including after ALIGN_MEAN.
                # The requested alignment, rather than that zero-length interval, defines the hour.
                if bucket_end is not None and bucket_start == bucket_end:
                    bucket_start = bucket_end - timedelta(seconds=USAGE_ALIGNMENT_SECONDS)
                cores = (point.get("value") or {}).get("doubleValue")
                if cores is None:
                    cores = (point.get("value") or {}).get("int64Value")
                if bucket_end is None or bucket_start is None or cores is None:
                    continue
                latest = max(latest, bucket_end) if latest else bucket_end
                points_seen += 1
                for day, seconds in _split_across_days(bucket_start, bucket_end):
                    hours[day] = hours.get(day, 0.0) + float(cores) * (seconds / 3600.0)
        page = answer.get("nextPageToken")
        if not page:
            break

    return {"hours": hours, "points": points_seen, "alignment": USAGE_ALIGNMENT_SECONDS, "latest": latest}


def _split_across_days(start: datetime, end: datetime) -> list:
    """[(UTC day, seconds of this bucket that fall in it)], so a bucket crossing midnight is shared."""
    start = start.astimezone(timezone.utc)
    end = end.astimezone(timezone.utc)
    if end <= start:
        return []
    pieces = []
    cursor = start
    while cursor < end:
        midnight = datetime.combine(cursor.date(), datetime.min.time(), tzinfo=timezone.utc) + timedelta(days=1)
        stop = min(midnight, end)
        pieces.append((cursor.date(), (stop - cursor).total_seconds()))
        cursor = stop
    return pieces


def anything_running(gcp: Gcp) -> bool:
    """Is any instance running in this project?"""
    if gcp.in_function:
        url = "https://compute.googleapis.com/compute/v1/projects/%s/aggregated/instances?filter=status%%3DRUNNING&maxResults=5&returnPartialSuccess=true" % gcp.project
        page = ""
        while True:
            answer = gcp.rest("GET", url + ("&pageToken=" + urllib.parse.quote(page) if page else ""))
            if any(group.get("instances") for group in (answer.get("items") or {}).values()):
                return True
            page = answer.get("nextPageToken")
            if not page:
                return False
    answer = gcp.gcloud_json([
        "compute", "instances", "list", "--filter=status=RUNNING", "--limit", "5",
    ])
    return bool(answer)


def cost_by_day(gcp: Gcp, table: str, start: date, end: date) -> dict:
    """{ISO date: cost} from the billing export, credits included."""
    if not re.fullmatch(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+", table):
        raise GcpError("invalid billing table name: %r" % table)
    query = (
        "SELECT FORMAT_DATE('%Y-%m-%d', DATE(usage_start_time)) AS day, "
        "SUM((cost + IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)) / currency_conversion_rate) AS total "
        "FROM `{table}` WHERE project.id = @project "
        "AND usage_start_time >= TIMESTAMP('{start}') AND usage_start_time < TIMESTAMP('{end}') "
        "GROUP BY day"
    ).format(table=table, start=start.isoformat(), end=end.isoformat())

    if gcp.in_function:
        url = "https://bigquery.googleapis.com/bigquery/v2/projects/%s/queries" % gcp.project
        answer = gcp.rest("POST", url, {"query": query, "useLegacySql": False, "timeoutMs": 10000,
            "parameterMode": "NAMED", "queryParameters": [{"name": "project", "parameterType": {"type": "STRING"},
                "parameterValue": {"value": gcp.project}}]})
        deadline = time.monotonic() + 30
        rows = []
        while True:
            if answer.get("errors"):
                raise GcpError("billing query failed: %s" % answer["errors"])
            complete = answer.get("jobComplete", False)
            if complete:
                rows.extend({"day": row["f"][0]["v"], "total": row["f"][1]["v"]} for row in answer.get("rows", []))
                if not answer.get("pageToken"):
                    break
            if time.monotonic() >= deadline:
                raise GcpError("billing query did not finish within 30 seconds")
            job = answer["jobReference"]
            parameters = {"location": job.get("location", "US"), "timeoutMs": 1000}
            if answer.get("pageToken"):
                parameters["pageToken"] = answer["pageToken"]
            answer = gcp.rest("GET", url + "/" + job["jobId"] + "?" + urllib.parse.urlencode(parameters))
    else:
        text = gcp.run(["bq", "query", "--project_id", gcp.project, "--use_legacy_sql=false", "--format=json",
                        "--parameter=project:STRING:" + gcp.project, query]).strip()
        try:
            rows = json.loads(text) if text else []
        except ValueError as error:
            raise GcpError("bq did not answer with JSON: %s" % error) from error

    costs = {}
    for row in rows or []:
        day = row.get("day")
        total = row.get("total")
        if day is None or total is None:
            continue
        try:
            costs[str(day)] = float(total)
        except (TypeError, ValueError):
            continue
    return costs


def cost_refresh_due(state: dict, needed: list, now: datetime, refresh_hours: float) -> bool:
    """Is it time to ask the bill again?"""
    if not state.get("cost_by_day"):
        return True
    as_of = as_datetime(state.get("cost_as_of"))
    if as_of is None:
        return True
    if now - as_of >= timedelta(hours=refresh_hours):
        return True
    return any(is_settled(day, now) and day.isoformat() not in state["cost_by_day"] for day in needed)


def read_state(gcp: Gcp, secret: str) -> dict:
    """The kept figures, or an empty set of them."""
    empty = {"version": STATE_VERSION, "cost_as_of": None, "sample_interval": None,
             "cost_by_day": {}, "pool_maxima": {}}
    try:
        text = gcp.secret(secret)
    except GcpError:
        return empty
    try:
        kept = json.loads(text)
    except ValueError:
        return empty
    if not isinstance(kept, dict) or kept.get("version") != STATE_VERSION:
        return empty
    kept.setdefault("cost_by_day", {})
    kept.setdefault("pool_maxima", {})
    kept.setdefault("cost_as_of", None)
    return kept


def write_state(gcp: Gcp, secret: str, state: dict) -> None:
    """Keep the figures, as a new secret version."""
    kept = dict(state)
    kept["version"] = STATE_VERSION
    days = sorted(kept.get("cost_by_day") or {})
    kept["cost_by_day"] = {day: round(float(kept["cost_by_day"][day]), 4) for day in days[-STATE_DAYS:]}
    data = json.dumps(kept, separators=(",", ":"), sort_keys=True)
    if gcp.in_function:
        gcp.rest("POST", gcp.secret_url(secret) + ":addVersion",
                 {"payload": {"data": base64.b64encode(data.encode()).decode()}})
    else:
        gcp.run(["gcloud", "secrets", "versions", "add", secret, "--data-file=-", "--project", gcp.project], stdin=data)
    # Keep OpenTofu's bootstrap version so a later apply does not recreate it over current state.
    bootstrap = os.environ.get("GKE_SPEND_STATE_BOOTSTRAP_VERSION", "1")
    versions = []
    page = ""
    while True:
        answer = gcp.rest("GET", gcp.secret_url(secret) + "/versions" +
                          ("?pageToken=" + urllib.parse.quote(page) if page else ""))
        versions.extend(v for v in answer.get("versions", []) if v.get("state") != "DESTROYED")
        page = answer.get("nextPageToken")
        if not page:
            break
    versions.sort(key=lambda v: int(v["name"].rsplit("/", 1)[1]))
    for version in versions[:-3]:
        if version["name"].rsplit("/", 1)[1] != bootstrap:
            gcp.rest("POST", "https://secretmanager.googleapis.com/v1/" + version["name"] + ":destroy", {})


def read_caps(gcp: Gcp, secret: str) -> dict:
    """The three caps."""
    text = gcp.secret(secret)
    try:
        raw = json.loads(text)
    except ValueError as error:
        raise GcpError("the caps secret %s does not hold JSON: %s" % (secret, error))
    if not isinstance(raw, dict):
        raise GcpError("the caps secret %s does not hold an object" % secret)
    caps = {}
    for window in WINDOWS:
        value = raw.get(window)
        # 0, "" and null all mean no cap for that window, so that a cap removed by hand reads as removed rather
        # than as a cap of nothing that is met immediately.
        caps[window] = None if value in (None, "", 0) else float(value)
    if not any(caps[window] is not None for window in WINDOWS):
        raise GcpError("no cap is set in %s, so there is nothing to compare spend against" % secret)
    return caps


def pool_ceilings(gcp: Gcp, cluster: str, names: list) -> dict:
    """{pool: its autoscaling maximum}, read off the pools themselves."""
    if gcp.in_function:
        listing = gcp.rest("GET", gcp.pool_url(cluster)).get("nodePools", [])
    else:
        listing = gcp.gcloud_json([
            "container", "node-pools", "list", "--cluster", cluster, "--region", gcp.location,
        ]) or []
    ceilings = {}
    for pool in listing:
        name = pool.get("name")
        if name not in names:
            continue
        autoscaling = pool.get("autoscaling") or {}
        if not autoscaling.get("enabled"):
            # A pool with autoscaling off has no maximum to read and cannot be braked by lowering one.  Reported
            # as unknown rather than as zero, because zero would read as already braked.
            ceilings[name] = None
            continue
        ceilings[name] = max(int(autoscaling.get("maxNodeCount") or 0),
                             int(autoscaling.get("totalMaxNodeCount") or 0))
    for name in names:
        ceilings.setdefault(name, None)
    return ceilings


def pool_brakes(ceilings: dict, maxima: dict, recorded=None) -> dict:
    """Use the recorded brake marker, with a ceiling-based fallback for old state."""
    return {pool: ceiling == 0 or (ceiling == 1 and maxima.get(pool, 1) > 1 and
                                 (recorded is None or pool in recorded))
            for pool, ceiling in ceilings.items()}


def set_pool_maximum(gcp: Gcp, cluster: str, pool: str, maximum: int) -> str:
    """Set one pool's autoscaling maximum. Returns "set", "busy" or "refused"."""
    try:
        if gcp.in_function:
            gcp.rest("POST", gcp.pool_url(cluster) + "/" + pool + ":setAutoscaling",
                     {"autoscaling": {"enabled": True, "minNodeCount": 0, "maxNodeCount": maximum}})
        else:
            gcp.gcloud([
                "container", "node-pools", "update", pool,
                "--cluster", cluster, "--region", gcp.location,
                "--enable-autoscaling", "--min-nodes", "0", "--max-nodes", str(maximum),
                "--async", "--quiet",
            ])
        return "set"
    except GcpError as error:
        text = str(error).lower()
        if "failed_precondition" in text or ("already" in text and "operation" in text):
            return "busy"
        if "invalid_argument" in text or "invalid value" in text or "must be greater" in text:
            return "refused"
        raise


def apply_brake(gcp: Gcp, cluster: str, targets: dict) -> dict:
    """Set every named pool to its target maximum, and report what happened to each."""
    outcome = {"set": [], "busy": [], "raised_floor": [], "failed": {}}
    for pool in sorted(targets):
        wanted = targets[pool]
        try:
            result = set_pool_maximum(gcp, cluster, pool, wanted)
            if result == "refused" and wanted == 0:
                result = set_pool_maximum(gcp, cluster, pool, 1)
                if result == "set":
                    outcome["raised_floor"].append(pool)
                    continue
            if result == "set":
                outcome["set"].append(pool)
            elif result == "busy":
                outcome["busy"].append(pool)
            else:
                outcome["failed"][pool] = "GKE refused a maximum of %d" % wanted
        except GcpError as error:
            outcome["failed"][pool] = str(error)
    return outcome


def evaluate(gcp: Gcp, config: dict, now: datetime, allow_bill: bool = True) -> dict:
    """What has been spent, against what caps, and whether that means braking. Changes nothing."""
    result = {
        "caps": None, "windows": [], "model": None, "figures": {}, "audit": None,
        "unknown": [], "warnings": [], "vcpu_hours_today": 0.0,
        "cost_refreshed": False, "state_dirty": False, "mode": "configured",
    }

    state = read_state(gcp, config["state_secret"])
    result["state"] = state
    result["pool_maxima"] = {**(state.get("pool_maxima") or {}), **config["pool_maxima"]}

    try:
        result["caps"] = read_caps(gcp, config["caps_secret"])
    except (GcpError, ValueError) as error:
        result["unknown"].append("the caps could not be read, so there is nothing to compare against: %s" % error)

    earliest_window = min(window_start(window, now) for window in WINDOWS)
    range_start = min(earliest_window, now - timedelta(days=14)).replace(
        hour=0, minute=0, second=0, microsecond=0)

    usage = {"hours": {}, "points": 0, "alignment": USAGE_ALIGNMENT_SECONDS}
    try:
        usage = vcpu_usage(gcp, range_start, now)
    except GcpError as error:
        result["unknown"].append("Cloud Monitoring would not report what has been running: %s" % error)

    hours = usage["hours"]
    result["vcpu_hours_today"] = hours.get(now.date(), 0.0)
    result["alignment"] = usage["alignment"]
    result["points"] = usage["points"]

    if not usage.get("latest") or now - usage["latest"] > timedelta(seconds=USAGE_ALIGNMENT_SECONDS + 600):
        # An empty metric and an idle project are the same empty answer, so Compute is asked which it is.
        try:
            if anything_running(gcp):
                result["unknown"].append(
                    "instances are running in %s and the vCPU metric is empty or stale, so what they are"
                    " costing cannot be established.  Confirm %s is being published:\n"
                    "    gcloud monitoring dashboards list   # any Monitoring read, to check the credential\n"
                    % (config["project"], USAGE_METRIC))
            else:
                result["warnings"].append(
                    "nothing is running in %s, so the only spend is the standing charge." % config["project"])
        except GcpError as error:
            result["unknown"].append("neither the metric nor Compute could say what is running: %s" % error)

    # The bill, if there is one to read.  A failure here is a warning and never unknown: every day can still be
    # estimated from the metric, so what is lost is the fit and not the figure.
    costs = dict(state.get("cost_by_day") or {})
    if config["billing_table"]:
        needed = days_between(range_start, now)
        if not allow_bill:
            result["warnings"].append("the billing export was not read, because --no-billing-export was given.")
        elif cost_refresh_due(state, needed, now, config["refresh_hours"]):
            try:
                fresh = cost_by_day(gcp, config["billing_table"],
                                    range_start.date(), now.date() + timedelta(days=1))
                costs.update(fresh)
                state["cost_by_day"] = costs
                state["cost_as_of"] = now.isoformat()
                result["cost_refreshed"] = True
            except GcpError as error:
                result["warnings"].append(
                    "the billing export could not be read, so the price below is the configured one rather than"
                    " a fitted one: %s" % error)
        if not costs:
            result["warnings"].append(
                "the billing export %s returned no rows yet.  It backfills nothing and is not final for about"
                " a day, so a newly enabled export reads empty for a while."
                % config["billing_table"])
    else:
        result["warnings"].append(
            "GKE_SPEND_BILLING_TABLE is not set, so there is no bill to fit against and the caps are compared"
            " against the configured price and standing charge.  See var.spend_billing_table.")

    model = calibrate(costs, hours, now, config["price"], config["fixed"])
    figures = day_figures(days_between(earliest_window, now), now, costs, hours, model)
    audit = audit_model(figures)

    # Rescue a configured guess the bill contradicts by a factor.  Only the fallback is scaled: scaling a fit
    # that already identified a slope would be fitting the same data twice.
    if model["mode"] == "fallback" and audit["off"] and audit["ratio"]:
        model = scale_to_bill(model, figures, audit)
        figures = day_figures(days_between(earliest_window, now), now, costs, hours, model)
        audit = audit_model(figures)

    if audit["off"] and audit["ratio"]:
        result["warnings"].append(
            "the model and the bill disagree by a factor of %.2f over %d settled day(s): estimated $%.2f"
            " against $%.2f billed.  `--days` prints the decomposition; spend_price_per_vcpu_hour and"
            " spend_fixed_usd_per_day are what to correct."
            % (audit["ratio"], audit["days"], audit["estimated"], audit["billed"]))

    result["model"] = model
    result["figures"] = figures
    result["audit"] = audit
    result["mode"] = model["mode"]

    for window in WINDOWS:
        spent = spend_in_window(window, now, figures)
        cap = (result["caps"] or {}).get(window)
        spent["cap"] = cap
        spent["over"] = cap is not None and spent["spend"] > cap
        result["windows"].append(spent)

    result["over"] = [w["window"] for w in result["windows"] if w["over"]]

    # The whole policy, in one line: unknown counts as over.
    result["should_brake"] = bool(result["over"]) or bool(result["unknown"])
    return result


def enforce(gcp: Gcp, config: dict, now: datetime, dry_run: bool = False) -> dict:
    """Evaluate, then brake or release, then say what happened."""
    result = evaluate(gcp, config, now)
    state = result["state"]
    names = config["pool_names"]

    ceilings = {}
    if names:
        try:
            ceilings = pool_ceilings(gcp, config["cluster"], names)
        except GcpError as error:
            result["unknown"].append("the agent node pools could not be read, so the brake's state is unknown:"
                                     " %s" % error)
            result["should_brake"] = True

    kept_maxima = dict(result["pool_maxima"])
    braked_now = pool_brakes(ceilings, result["pool_maxima"], state.get("braked_pools"))
    for pool, ceiling in ceilings.items():
        if pool not in config["pool_maxima"] and ceiling is not None and not braked_now[pool] and kept_maxima.get(pool) != ceiling:
            kept_maxima[pool] = ceiling
            state["pool_maxima"] = kept_maxima
            result["state_dirty"] = True
    result["pool_maxima"].update(kept_maxima)
    if state.get("pool_maxima") != kept_maxima:
        state["pool_maxima"] = kept_maxima
        result["state_dirty"] = True

    have = bool(braked_now) and all(braked_now.values())
    uniform = len(set(braked_now.values())) <= 1
    want = result["should_brake"]

    result["ceilings"] = ceilings
    result["brake_was"] = braked_now
    result["acted"] = False
    result["outcome"] = None

    if names and (want != have or not uniform):
        if dry_run:
            result["warnings"].append("--dry-run, so the brake was not changed.")
        else:
            targets = {}
            for pool in names:
                if want:
                    targets[pool] = 0
                else:
                    restored = kept_maxima.get(pool) or config["pool_maxima"].get(pool)
                    if restored is None:
                        result["warnings"].append(
                            "%s has no recorded maximum to restore, so it is left as it is.  `make apply` puts"
                            " every pool back to what var.agent_pools declares." % pool)
                        continue
                    targets[pool] = int(restored)
            if targets:
                outcome = apply_brake(gcp, config["cluster"], targets)
                result["outcome"] = outcome
                result["acted"] = bool(outcome["set"] or outcome["raised_floor"])
                if outcome["failed"]:
                    named = sorted(outcome["failed"])[:FAILURES_NAMED]
                    more = len(outcome["failed"]) - len(named)
                    result["unknown"].append(
                        "the brake could not be set on %s%s, so those pools are in an unknown state: %s"
                        % (", ".join(named), " and %d more" % more if more > 0 else "",
                           "; ".join(outcome["failed"][pool] for pool in named)))

    # Read from what the brake *is*, not from what it was meant to be: a failed update leaves a pool running,
    # and a report that said otherwise would be the one thing worse than no report.
    if names and not dry_run and result["outcome"] is not None:
        try:
            ceilings = pool_ceilings(gcp, config["cluster"], names)
            result["ceilings"] = ceilings
        except GcpError as error:
            result["warnings"].append("the brake was set but could not be read back: %s" % error)

    recorded = {pool for pool, braked in braked_now.items() if braked}
    changed = set((result["outcome"] or {}).get("set", [])) | set((result["outcome"] or {}).get("raised_floor", []))
    if want:
        recorded |= changed
    else:
        recorded = {pool for pool in recorded if result["ceilings"].get(pool) is None or
                    result["ceilings"][pool] < result["pool_maxima"].get(pool, 1)}
    if state.get("braked_pools") != sorted(recorded):
        state["braked_pools"] = sorted(recorded)
        result["state_dirty"] = True
    result["braked_observed"] = bool(result["ceilings"]) and all(
        pool_brakes(result["ceilings"], result["pool_maxima"], recorded).values())

    accepted = set((result["outcome"] or {}).get("set", [])) | set((result["outcome"] or {}).get("raised_floor", []))
    result["braking"] = bool(names) and want and accepted >= set(names)
    result["braked"] = result["braked_observed"] or result["braking"]

    if (result["cost_refreshed"] or result["state_dirty"]) and not dry_run:
        try:
            write_state(gcp, config["state_secret"], state)
        except GcpError as error:
            result["warnings"].append("the kept figures could not be written: %s" % error)

    if not dry_run:
        try:
            publish_metrics(gcp, config, result)
        except GcpError as error:
            result["warnings"].append(
                "the custom metrics could not be written, so the alert policy watching this guard will fire:"
                " %s" % error)

        # Announce a change, or an unknown on a cluster that is not braked.  Never a steady state: a message
        # every five minutes is a message nobody reads.
        if result["acted"] or (result["unknown"] and not have):
            try:
                notify(gcp, config, result)
            except GcpError as error:
                result["warnings"].append("the notification could not be published: %s" % error)

    return result


def publish_metrics(gcp: Gcp, config: dict, result: dict) -> None:
    """Publish spend, cap state, brake state and the evaluation heartbeat."""
    now = rfc3339(utc_now())
    prefix = config["metric_prefix"].rstrip("/")
    series = []

    def point(metric_type: str, value: float, labels: dict = None):
        series.append({
            "metric": {"type": metric_type, "labels": dict(labels or {}, cluster=config["cluster"])},
            "resource": {"type": "global", "labels": {"project_id": config["project"]}},
            "points": [{"interval": {"endTime": now}, "value": {"doubleValue": float(value)}}],
        })

    for window in result["windows"]:
        point("%s/estimated_spend_usd" % prefix, round(window["spend"], 4), {"window": window["window"]})
    point("%s/braked" % prefix, 1 if result.get("braked") else 0)
    point("%s/cap_exceeded" % prefix, 1 if result["over"] else 0)
    point("%s/evaluation_ok" % prefix, 0 if result["unknown"] else 1)
    point("%s/vcpu_hours_today" % prefix, round(result["vcpu_hours_today"], 4))

    gcp.rest("POST", "https://monitoring.googleapis.com/v3/projects/%s/timeSeries" % gcp.project,
             {"timeSeries": series})


def notify(gcp: Gcp, config: dict, result: dict) -> None:
    """Publish the whole report, so the figures behind a decision travel with it."""
    if not config["alert_topic"]:
        return
    message = brake_subject(config["cluster"], result) + "\n\n" + render(result)
    if gcp.in_function:
        topic = config["alert_topic"]
        if not topic.startswith("projects/"):
            topic = "projects/%s/topics/%s" % (config["project"], topic)
        gcp.rest("POST", "https://pubsub.googleapis.com/v1/" + topic + ":publish",
                 {"messages": [{"data": base64.b64encode(message.encode()).decode()}]})
    else:
        gcp.run(["gcloud", "pubsub", "topics", "publish", config["alert_topic"],
                 "--project", config["project"], "--message", message])


def brake_subject(cluster: str, result: dict) -> str:
    """One line saying which of four things happened."""
    if result.get("braked") and result["unknown"]:
        return "%s: agent pools stopped, because what it is spending could not be established" % cluster
    if result.get("braked"):
        over = result["over"]
        if len(over) == 1:
            return "%s: agent pools stopped, the %s cap is met" % (cluster, over[0])
        return "%s: agent pools stopped, the %s caps are met" % (cluster, " and ".join(over))
    return "%s: agent pools released, spend is under every cap" % cluster


def money(amount: float) -> str:
    """A figure somebody acts on: cents below ten dollars, and none above."""
    if abs(amount) < 10:
        return "${:,.2f}".format(amount)
    return "${:,.0f}".format(amount)


def render(result: dict, days: bool = False) -> str:
    """The one renderer, so a notification and a terminal cannot disagree."""
    lines = []
    lines.append("%-8s %-12s %12s %12s %7s" % ("window", "since", "spent", "cap", "used"))
    for window in result["windows"]:
        cap = window["cap"]
        cap_text = "unknown" if result["caps"] is None else money(cap) if cap else "none"
        used = "" if not cap else "%d%%" % round(100 * window["spend"] / cap)
        lines.append("%-8s %-12s %12s %12s %7s%s" % (
            window["window"], window["start"].date().isoformat(),
            money(window["spend"]), cap_text, used,
            "  OVER" if window["over"] else ""))

    lines.append("")
    model = result["model"] or {}
    rate = model.get("price", 0.0)
    fixed = model.get("fixed_per_day", 0.0)
    if model.get("mode") == "calibrated":
        lines.append("fitted from the bill: $%.4f a vCPU-hour and %s a day standing still, over %d settled"
                     " day(s), +/- %s a day"
                     % (rate, money(fixed), model.get("days", 0), money(model.get("rms") or 0.0)))
    elif model.get("mode") == "scaled":
        lines.append("configured figures scaled to the bill by %.2f: $%.4f a vCPU-hour and %s a day standing"
                     " still.  %s" % (model.get("ratio") or 0.0, rate, money(fixed), model.get("how", "")))
    else:
        lines.append("configured, not fitted: $%.4f a vCPU-hour and %s a day standing still"
                     % (rate, money(fixed)))
        why = model.get("why")
        if why:
            lines.append("  the fit was refused: %s" % why)
        fitted = model.get("fitted")
        if fitted:
            lines.append("  what the days did give: $%.4f a vCPU-hour and %s a day standing still"
                         % (fitted.get("price", 0.0), money(fitted.get("fixed_per_day") or 0.0)))

    lines.append("%.1f vCPU-hours so far today, from %d aligned point(s) of %ds"
                 % (result["vcpu_hours_today"], result.get("points", 0), result.get("alignment", 0)))

    outcome = result.get("outcome") or {}
    raised = sorted(outcome.get("raised_floor") or [])

    ceilings = result.get("ceilings")
    if ceilings is None:
        pass
    elif not ceilings:
        lines.append("brake: no agent node pool was named, so there is nothing to stop")
    else:
        braked = [pool for pool, stopped in pool_brakes(
            ceilings, result["pool_maxima"], result["state"].get("braked_pools")).items() if stopped]
        if len(braked) == len(ceilings):
            floors = sorted(pool for pool, ceiling in ceilings.items() if ceiling == 1) or raised
            lines.append("brake: STOPPED, every one of %d agent pool(s) is at its floor%s"
                         % (len(ceilings),
                            " (%d held at one node, because GKE refused a maximum of zero)" % len(floors)
                            if floors else ""))
        elif result.get("braking"):
            # Accepted on every pool and not yet visible, which is what an asynchronous update looks like for
            # the first minute or so.
            lines.append("brake: STOPPING, accepted on every one of %d agent pool(s) and not yet reported by"
                         " GKE, because a node pool update is asynchronous%s"
                         % (len(ceilings),
                            " (%d held at one node, because GKE refused a maximum of zero)" % len(raised)
                            if raised else ""))
        elif braked:
            lines.append("brake: PART STOPPED, %d of %d agent pool(s): %s"
                         % (len(braked), len(ceilings), ", ".join(sorted(braked))))
        else:
            lines.append("brake: off, the agent pools may create nodes")

    if days:
        lines.append("")
        lines.append(render_days(result))

    for message in result["unknown"]:
        lines.append("")
        lines.append("UNKNOWN: %s" % message)
    for message in result["warnings"]:
        lines.append("")
        lines.append("WARNING: %s" % message)
    return "\n".join(lines)


def render_days(result: dict) -> str:
    """The per-day decomposition, with the estimate split into its two terms."""
    lines = ["%-11s %10s %10s %10s %10s %10s %8s %9s"
             % ("day", "vCPU-h", "from load", "+ standing", "= estimate", "billed", "settled", "taken")]
    for day in sorted(result["figures"]):
        row = result["figures"][day]
        billed = row["billed"]
        lines.append("%-11s %10.1f %10s %10s %10s %10s %8s %9s" % (
            day.isoformat(), row["vcpu_hours"],
            money(row["estimated"] - row["fixed_part"]), money(row["fixed_part"]), money(row["estimated"]),
            money(billed) if billed is not None else "-",
            "yes" if row["settled"] else "no", row["taken"]))
    audit = result.get("audit") or {}
    if audit.get("ratio"):
        lines.append("over %d settled day(s) the model reads %.2fx the bill" % (audit["days"], audit["ratio"]))
    return "\n".join(lines)


def guard(request):  # noqa: ARG001 - the signature is Cloud Functions', and the body carries nothing
    """The Cloud Function. Enforces, and prints its whole report into the log."""
    config = config_from_environment()
    gcp = Gcp(config["project"], config["location"])
    result = enforce(gcp, config, utc_now())
    print(brake_subject(config["cluster"], result))
    print(render(result))
    return {
        "braked": bool(result.get("braked")),
        "over": result["over"],
        "unknown": result["unknown"],
        "spend": {w["window"]: round(w["spend"], 2) for w in result["windows"]},
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--report", action="store_true",
                        help="read and print, changing nothing. The default, named so a caller states intent.")
    parser.add_argument("--enforce", action="store_true",
                        help="evaluate and then stop or release the agent pools. What the Cloud Function does.")
    parser.add_argument("--dry-run", action="store_true",
                        help="with --enforce, say what would change and change nothing.")
    parser.add_argument("--no-billing-export", action="store_true",
                        help="use only the figures already kept, and do not query BigQuery.")
    parser.add_argument("--days", action="store_true",
                        help="print the per-day decomposition behind each window's figure.")
    parser.add_argument("--json", action="store_true", help="emit JSON instead of the table.")
    args = parser.parse_args(argv)

    try:
        config = config_from_environment()
    except ValueError as error:
        print(error, file=sys.stderr)
        return 2

    gcp = Gcp(config["project"], config["location"])
    now = utc_now()

    if args.enforce:
        result = enforce(gcp, config, now, dry_run=args.dry_run)
    else:
        result = evaluate(gcp, config, now, allow_bill=not args.no_billing_export)
        # A report reads the brake so it can say whether the pools are stopped, which is the question somebody
        # runs `make spend` to answer when a build is not getting nodes.
        if config["pool_names"]:
            try:
                result["ceilings"] = pool_ceilings(gcp, config["cluster"], config["pool_names"])
                result["braked"] = bool(result["ceilings"]) and all(
                    pool_brakes(result["ceilings"], result["pool_maxima"], result["state"].get("braked_pools")).values())
            except GcpError as error:
                result["warnings"].append("the agent node pools could not be read: %s" % error)

    if args.json:
        print(json.dumps({
            "windows": [{k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in w.items()}
                        for w in result["windows"]],
            "model": {k: v for k, v in (result["model"] or {}).items() if k != "fitted"},
            "braked": bool(result.get("braked")),
            "over": result["over"],
            "unknown": result["unknown"],
            "warnings": result["warnings"],
        }, indent=2, sort_keys=True))
    else:
        print(render(result, days=args.days))

    if result["unknown"]:
        return 2
    if result["over"] or result.get("braked"):
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
