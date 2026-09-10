#!/usr/bin/env python3
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
"""
What has this account spent today, this week and this month, and should the agent pools be stopped?

This runs in two places and is one file for one reason: the arithmetic that decides to stop the pools and
the arithmetic `make spend` prints have to be the same lines.  Two implementations of it would disagree,
and the one nobody reads is the one that brakes the cluster.

    ./spend-guard.py --report          read only, print the table.  This is `make spend`
    ./spend-guard.py --enforce         the same, and then suspend or resume the agent pools

In AWS it is a Lambda on a five-minute EventBridge schedule, calling handler() to do `--enforce`.  It
takes its configuration from the environment, under the same names ../Makefile exports into .eks-env, so
there is one name per value in both worlds.  See 1-cluster/spend-guard.tf.

Nothing here is installed: boto3 in Lambda, and the `aws` CLI on an operator's machine, which is already a
prerequisite of this directory.  See class Aws.

WHY AN ESTIMATE, AND NOT A BILL

AWS has no low-latency spend feed.  Cost Explorer is refreshed at least once a day and its figure for the
current day is partial, so a daily cap enforced from Cost Explorer alone reacts a day late.  These pools
can hold hundreds of on-demand nodes, which is over a hundred dollars an hour, so a day late is the whole
cap and more.

So spend is measured twice, and which figure a day takes depends on whether its bill has settled:

    spend(window) = sum over days d in window of  ce[d]                                  d settled
                                                 max( ce[d], price * V[d] + fixed * e )  otherwise

`ce[d]` is Cost Explorer's unblended cost for that day, kept between evaluations because each call costs a
cent.  `V[d]` is vCPU-hours from the CloudWatch metric AWS/Usage ResourceCount, which is free to read and is
the same measurement Service Quotas alarms on.

A settled day takes the bill, whatever the model makes of it: 48 hours after a day ends there is nothing left
for a model to add.  An earlier version took the larger everywhere, on the reasoning that a complete Cost
Explorer figure is the larger anyway; that holds only while the model is right, and when it was not the
error reached into every past day of every window.  Now it can only reach the last two.

The current day has no bill to fall back on, so the model is the whole of its figure.  audit_model() is what
says whether that figure is worth anything: over the settled days both figures exist, and their ratio is the
model's error against the bill.  Two of those ratios have been thirty, and both were arithmetic faults in
this file.

`price` and `fixed` are fitted, not written down.  Over the settled days of the last fortnight,

    ce[d] = fixed + price * V[d]

is two unknowns against several days of different load, so least squares gives both, and the residual is
reported as the error margin.  A fitted price covers the root volumes, the cross-zone transfer and the
control plane without any of them being listed here, and it follows a price change or a new instance type
on its own.  When the days cannot answer, it falls back to EKS_SPEND_PRICE_PER_VCPU_HOUR and
EKS_SPEND_FIXED_USD_PER_DAY, and says which mode it used and why; see calibrate() for the four refusals.
The one that matters is a fortnight of an idle cluster, whose days differ by a few vCPU-hours: least squares
answers that with a price near zero, and a price near zero estimates a full-size build at nothing at all.

A vCPU-hour is Sum times the metric's own sample interval, and that interval is measured from the
metric's own timestamps on every evaluation.  Two earlier versions inferred it instead and were both wrong by
a factor: a written-down 60 seconds against an account publishing every 300 read a fifth of the truth, and
`3600 / max(SampleCount)` read some thirty times the bill.  See vcpu_usage().

The cap is on the whole account's bill in this region, not on what is tagged for this cluster.  That is how
this directory already treats the vCPU quota: an account limit, and not a share of one.  Anything else
running in the region therefore brakes these pools, which is the conservative reading of a cap on a bill.
Run this cluster in an account of its own.

In this region is literal, and is a `REGION` filter on the Cost Explorer call.  Without it the bill covers
every region while the vCPU-hours cover one, and the fit below regresses the first onto the second; see
cost_by_day().  Spend in another region is therefore outside these caps, and so is anything global.

WHAT THE BRAKE IS

Suspending the `Launch` and `AZRebalance` processes on every agent Auto Scaling group.  The group accepts
the target size the cluster autoscaler asks for and no instance appears, so:

  - no new agent node starts;
  - every running agent finishes, untouched;
  - `Terminate` is not suspended, so the autoscaler still removes each node as it goes idle, and the pools
    drain to zero on their own.

`AZRebalance` goes with `Launch` because a rebalance under a suspended `Launch` can terminate an instance
without replacing it.  Each group here is one zone, so nothing rebalances, and suspending it costs nothing.

The brake's state is the state of the thing it brakes: `SuspendedProcesses` on the group.  There is no
second record of it to drift, and `tofu plan` sees nothing, because no size is changed.  The controller's
own group is never touched: it holds the build queue and jenkins_home, and the four pools are what the
money is in.

What a build sees, once braked, is a pod that stays Pending, then the churn loop of ../README.md's "Agent
pods churn and no agent connects", with the brake in the quota's place.  A braked cluster is a stopped
cluster, and 3-smoke/smoke-test.sh reports the brake by name so that it is not diagnosed twice.

WHEN SPEND CANNOT BE READ

Unknown is treated as over the cap: brake, publish to SNS, and release on the next evaluation that reads a
figure.  A guard that cannot see stops CI loudly, rather than leaving the pools running unwatched.  Three
things are unknown, and every other failure is a warning:

  - the caps parameter cannot be read, so there is nothing to compare against;
  - the CloudWatch read fails;
  - CloudWatch returns no vCPU data at all while instances are running in the region.

The third is a discriminator rather than a guess: an empty metric and an idle account are the same empty
answer, so EC2 is asked whether anything is running.  Nothing running is a spend of zero; instances running
is a read that has broken, and the message names the dimensions to confirm.

Not unknown, and reported: Cost Explorer failing or answering nothing.  Every day in every window can be
estimated from CloudWatch alone, so the figure survives; what is lost is the fit, and the fallback price is
then used and named.

EXIT STATUS

    0  under every cap
    1  over a cap, or braked
    2  spend could not be established, or the configuration is incomplete
"""

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# The windows, the settled-day rule and the least-squares fit: arithmetic with no cloud in it, shared with the
# other clouds' directories.  See ../shared/README.md, and that module's docstring for why spend is measured
# twice and why both of its terms are fitted.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "shared"))
from spend_model import (CALIBRATION_DAYS, MODEL_AUDIT_FACTOR,  # noqa: E402 - after the sys.path line above
                         SETTLED_AFTER_HOURS, WINDOWS, audit_model, calibrate, day_elapsed_fraction,
                         day_figures, days_between, is_settled, scale_to_bill, spend_in_window, window_start)

# The CloudWatch metric that carries how much on-demand standard vCPU is running in this region.  It is
# what Service Quotas measures L-1216C47A against, so it counts exactly the instances ../vcpu-quota.py
# charges to that quota, the controller's node included.
USAGE_NAMESPACE = "AWS/Usage"
USAGE_METRIC = "ResourceCount"
USAGE_DIMENSIONS = {
    "Service": "EC2",
    "Class": "Standard/OnDemand",
    "Type": "Resource",
    "Resource": "vCPU",
}

# What the metric is published at, in seconds, where AWS publishes one of these two.  Nothing divides by
# these: sample_interval_seconds measures the interval from the metric's own timestamps on every evaluation,
# and this pair only decides whether the measurement is worth a line in the report.  The account this was
# written against publishes every 300 seconds.
USAGE_KNOWN_INTERVALS = (60.0, 300.0)

# One hour per datapoint, whatever the window.  Sum is exact at any period, and one-hour data is retained
# for 15 months where one-minute data is retained for 15 days, so a month-long window has no gaps in it.
USAGE_PERIOD_SECONDS = 3600

# Cost Explorer is a global service with one endpoint, in us-east-1, whatever region the cluster is in.
COST_EXPLORER_REGION = "us-east-1"

# What the brake suspends.  AZRebalance goes with Launch: a rebalance under a suspended Launch can
# terminate an instance without replacing it.
BRAKE_PROCESSES = ["Launch", "AZRebalance"]

# DescribeAutoScalingGroups takes at most 50 names in one call.
ASG_NAMES_PER_CALL = 50

# How many days of Cost Explorer figures the state parameter keeps.  An SSM standard parameter holds 4 KB
# and a day is about 26 bytes of it, so this is a fifth of the room; the rest is headroom for the keys
# around it rather than a limit anybody should meet.
STATE_DAYS = 45
STATE_VERSION = 1


# ---------------------------------------------------------------------------------------------------
# Calling AWS
# ---------------------------------------------------------------------------------------------------

class AwsError(Exception):
    """An AWS call that failed, with the operation and the message AWS gave."""


class Aws:
    """One way to call AWS from two places.

    In Lambda that is boto3, which the runtime carries.  Anywhere else it is the `aws` CLI, which this
    directory already requires and which needs nothing installed for this script.  Both are given the same
    thing: an API operation in the name AWS documents it under, and its parameters as the API shapes them.
    `--cli-input-json` is what makes one shape serve both.

    Chosen by AWS_LAMBDA_FUNCTION_NAME rather than by trying to import boto3, so which path runs is a fact
    about where the code is and not about what happens to be installed beside it.
    """

    def __init__(self, region: str):
        self.region = region
        self.in_lambda = bool(os.environ.get("AWS_LAMBDA_FUNCTION_NAME"))
        self._clients = {}

    def call(self, service: str, operation: str, region: str = None, **params) -> dict:
        """One call.  `operation` is the API name, as `GetCostAndUsage`."""
        region = region or self.region
        if self.in_lambda:
            return self._call_boto3(service, operation, region, params)
        return self._call_cli(service, operation, region, params)

    def _call_boto3(self, service: str, operation: str, region: str, params: dict) -> dict:
        import boto3  # Imported here so that the CLI path needs nothing installed.
        from botocore.exceptions import BotoCoreError, ClientError

        key = (service, region)
        if key not in self._clients:
            self._clients[key] = boto3.client(service, region_name=region)
        method = getattr(self._clients[key], snake_case(operation))
        try:
            answer = method(**params)
        except (BotoCoreError, ClientError) as error:
            raise AwsError(f"{service} {operation}: {error}") from error
        # The response metadata is noise here, and its presence differs from the CLI's answer.
        answer.pop("ResponseMetadata", None)
        return answer

    def _call_cli(self, service: str, operation: str, region: str, params: dict) -> dict:
        command = ["aws", service, kebab_case(operation), "--region", region, "--output", "json"]
        if params:
            command += ["--cli-input-json", json.dumps(params, default=str)]
        try:
            done = subprocess.run(command, capture_output=True, text=True, check=True)
        except subprocess.CalledProcessError as error:
            message = (error.stderr or "").strip() or f"exit {error.returncode}"
            raise AwsError(f"{service} {operation}: {message}") from error
        except FileNotFoundError as error:
            raise AwsError("the aws CLI is not on the PATH, and this is not Lambda") from error
        if not done.stdout.strip():
            # Some operations answer with nothing at all, which is not an error.
            return {}
        return json.loads(done.stdout)


def snake_case(operation: str) -> str:
    """`GetCostAndUsage` to `get_cost_and_usage`, which is boto3's name for it."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", operation).lower()


def kebab_case(operation: str) -> str:
    """`GetCostAndUsage` to `get-cost-and-usage`, which is the CLI's name for it."""
    return snake_case(operation).replace("_", "-")


# ---------------------------------------------------------------------------------------------------
# Windows
# ---------------------------------------------------------------------------------------------------

def utc_now() -> datetime:
    """Now, in UTC, or what $EKS_SPEND_NOW says instead.

    A seam for spend-guard-test.sh, and the only one in this file.  The window arithmetic below is where a
    fault would be invisible and expensive: a month boundary, a Monday, and a day that has not finished are
    each a case that occurs on one date in thirty and cannot be reached at all by a test that runs at the
    real time.  An ISO 8601 timestamp here is what makes those cases reachable offline.
    """
    override = os.environ.get("EKS_SPEND_NOW")
    if not override:
        return datetime.now(timezone.utc)
    stated = datetime.fromisoformat(override.replace("Z", "+00:00"))
    return stated if stated.tzinfo else stated.replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------------------------------------------
# What the account has run: vCPU-hours from CloudWatch
# ---------------------------------------------------------------------------------------------------

def usage_metric_stat(period: int, stat: str) -> dict:
    return {
        "Metric": {
            "Namespace": USAGE_NAMESPACE,
            "MetricName": USAGE_METRIC,
            "Dimensions": [{"Name": name, "Value": value}
                           for name, value in sorted(USAGE_DIMENSIONS.items())],
        },
        "Period": period,
        "Stat": stat,
    }


def as_datetime(value) -> datetime:
    """An AWS timestamp: a datetime from boto3, or one of three string forms.

    `datetime.fromisoformat` before Python 3.11 rejects both `Z` and an offset without a colon, and Lambda
    reports `LastModified` as `2026-09-09T14:12:34.000+0000`.  Both are normalised rather than left to the
    interpreter's version, because which of them runs this is not this file's business.
    """
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).replace("Z", "+00:00")
    text = re.sub(r"([+-]\d{2})(\d{2})$", r"\1:\2", text)
    return datetime.fromisoformat(text)


def get_metric_data(aws: Aws, start: datetime, end: datetime, period: int, stats: tuple) -> dict:
    """{stat: {timestamp: value}} for the usage metric, following NextToken.

    Several statistics in one call, because `Sum` alone cannot say what a sample is worth: see vcpu_usage.
    GetMetricData is charged per metric requested rather than per call, so asking for two costs the same
    whether it is one call or two.
    """
    ids = {f"q{index}": stat for index, stat in enumerate(stats)}
    points = {stat: {} for stat in stats}
    token = None
    while True:
        params = {
            "MetricDataQueries": [
                {"Id": key, "MetricStat": usage_metric_stat(period, stat), "ReturnData": True}
                for key, stat in ids.items()
            ],
            "StartTime": start.isoformat(),
            "EndTime": end.isoformat(),
            "ScanBy": "TimestampAscending",
        }
        if token:
            params["NextToken"] = token
        answer = aws.call("cloudwatch", "GetMetricData", **params)
        for result in answer.get("MetricDataResults", []):
            stat = ids.get(result.get("Id"))
            if stat is None:
                continue
            for timestamp, value in zip(result.get("Timestamps", []), result.get("Values", [])):
                points[stat][as_datetime(timestamp)] = value
        token = answer.get("NextToken")
        if not token:
            return points


def sample_interval_seconds(aws: Aws, now: datetime) -> float:
    """How often the metric is published, measured from its own timestamps.

    The smallest gap between consecutive datapoints, read at one-minute resolution over the last six hours.
    Timestamps are the only thing here that cannot be misread: a gap is a gap, whereas `SampleCount` is a
    number whose meaning has to be assumed, and assuming it wrongly is what put this figure out by a factor
    of thirty.  The minimum rather than the mean, because the publication rate is the closest two datapoints
    and anything wider is a gap in coverage.

    Returns None when fewer than two datapoints exist, which happens on an account that ran nothing in those
    six hours and on data older than the fifteen days CloudWatch keeps at one-minute resolution.
    """
    points = get_metric_data(aws, now - timedelta(hours=6), now, 60, ("Sum",))["Sum"]
    if len(points) < 2:
        return None
    stamps = sorted(points)
    return min((later - earlier).total_seconds() for earlier, later in zip(stamps, stamps[1:]))


def vcpu_usage(aws: Aws, start: datetime, end: datetime, now: datetime, known: float = None) -> dict:
    """vCPU-hours per UTC date over [start, end), and how the metric was read to get them.

    The metric is a gauge: each datapoint is how many vCPU were running at that moment, so an hour's `Sum` is
    the total of its samples and one sample is worth however long it stands for.

        vCPU-hours(hour) = Sum(hour) * interval / 3600

    The interval comes from the metric's own timestamps; see sample_interval_seconds.  Two earlier versions
    of this line were wrong in the same direction and for the same reason, which is why it is measured and no
    longer inferred:

      - a written-down 60 seconds, against an account publishing every 300, read a fifth of the truth;
      - `3600 / max(SampleCount)` read a figure some thirty times the bill, because SampleCount over an hour
        of this metric is not the count of raw samples in it.

    `Sum` and not `Average`.  Average is Sum over SampleCount, the mean of the samples that exist, so an hour
    the metric was absent for most of reads as though it ran the whole hour.  Sum times the interval charges
    only for what was published, which is right for a partial hour and for an idle one alike.

    `known` is the interval a previous evaluation measured, from the state parameter.  It is used when the
    probe finds nothing, which happens on an account that has run nothing for six hours: without it a day
    whose load ended seven hours ago would read as an idle day, and an under-read is the direction that fails
    to brake.  The interval is a property of the metric and not of the load, so yesterday's answer is good.

    With no interval from either source there is no arithmetic to do, so `hours` is empty and `interval` is
    None; the caller reads that together with whether anything is running.  `samples` is the fullest hour's
    SampleCount, kept only so that the report can print what this once wrongly divided by.
    """
    points = get_metric_data(aws, start, end, USAGE_PERIOD_SECONDS, ("Sum", "SampleCount"))
    counts = [count for count in points["SampleCount"].values() if count > 0]
    measured = sample_interval_seconds(aws, now)
    interval = measured if measured is not None else known
    if interval is None or not points["Sum"]:
        return {"hours": {}, "interval": interval, "measured": measured,
                "samples": max(counts) if counts else 0}

    hours = {}
    for timestamp, total in points["Sum"].items():
        day = timestamp.astimezone(timezone.utc).date()
        hours[day] = hours.get(day, 0.0) + total * interval / 3600.0
    return {"hours": hours, "interval": interval, "measured": measured,
            "samples": max(counts) if counts else 0}


def anything_running(aws: Aws) -> bool:
    """Is any EC2 instance running in this region?

    The discriminator for an empty metric.  No datapoints with nothing running is an idle account and a
    spend of zero; no datapoints with instances running is a fault in the read, and the two are the same
    empty answer.  DescribeInstanceStatus rather than DescribeInstances, and five at most: the question is
    whether there are any, not how many, and 480 instance descriptions is a megabyte an evaluation.
    """
    answer = aws.call("ec2", "DescribeInstanceStatus", MaxResults=5, IncludeAllInstances=False,
                      Filters=[{"Name": "instance-state-name", "Values": ["running"]}])
    return bool(answer.get("InstanceStatuses"))


# ---------------------------------------------------------------------------------------------------
# What the account was charged: Cost Explorer
# ---------------------------------------------------------------------------------------------------

def cost_by_day(aws: Aws, start: date, end: date, metric: str) -> dict:
    """Cost per UTC date over [start, end) in this region, from Cost Explorer.

    One call, in us-east-1 whatever region the cluster is in, and each call costs a cent, which is why the
    answer is kept in the state parameter and refreshed on a schedule rather than every evaluation.

    Filtered to `aws.region`, and that filter is what makes the two halves of the estimate comparable.  The
    vCPU-hours the model is fitted against come from a CloudWatch metric that is per region, so an unfiltered
    bill regresses every region's spend onto one region's vCPU: on an account with anything in a second
    region the fitted rate and the standing-still figure are both wrong, and the report has no way to say so.

    What the filter drops is the global services, Route 53 among them, which are cents a month against a
    controller node.  What it does not do is make the cap a cap on this cluster: everything else in this
    region still spends from it, which is why ../README.md asks for an account of its own.
    """
    costs = {}
    token = None
    while True:
        params = {
            "TimePeriod": {"Start": start.isoformat(), "End": end.isoformat()},
            "Granularity": "DAILY",
            "Metrics": [metric],
            "Filter": {"Dimensions": {"Key": "REGION", "Values": [aws.region]}},
        }
        if token:
            params["NextPageToken"] = token
        answer = aws.call("ce", "GetCostAndUsage", region=COST_EXPLORER_REGION, **params)
        for result in answer.get("ResultsByTime", []):
            day = date.fromisoformat(result["TimePeriod"]["Start"])
            costs[day] = float(result["Total"][metric]["Amount"])
        token = answer.get("NextPageToken")
        if not token:
            return costs


# ---------------------------------------------------------------------------------------------------
# The state parameter: the Cost Explorer figures, between evaluations
# ---------------------------------------------------------------------------------------------------

def read_state(aws: Aws, parameter: str) -> dict:
    """The Cost Explorer figures kept from earlier evaluations, or none.

    A parameter that is missing, unreadable or malformed keeps no figures and is not a failure: every day can
    be estimated from CloudWatch alone, so what is lost is accuracy and the fit, both of which are
    reported.  The alternative, braking when nothing has been kept yet, would brake every cluster on its
    first evaluation.
    """
    try:
        answer = aws.call("ssm", "GetParameter", Name=parameter)
        held = json.loads(answer["Parameter"]["Value"])
    except (AwsError, KeyError, ValueError):
        return {"version": STATE_VERSION, "cost_by_day": {}, "cost_as_of": None,
                "sample_interval": None}
    if held.get("version") != STATE_VERSION:
        return {"version": STATE_VERSION, "cost_by_day": {}, "cost_as_of": None,
                "sample_interval": None}
    return {
        "version": STATE_VERSION,
        "cost_by_day": held.get("cost_by_day", {}),
        "cost_as_of": held.get("cost_as_of"),
        "sample_interval": held.get("sample_interval"),
    }


def write_state(aws: Aws, parameter: str, state: dict) -> None:
    kept = dict(sorted(state["cost_by_day"].items())[-STATE_DAYS:])
    value = json.dumps({
        "version": STATE_VERSION,
        "cost_as_of": state["cost_as_of"],
        "sample_interval": state.get("sample_interval"),
        "cost_by_day": {day: round(cost, 4) for day, cost in kept.items()},
    }, separators=(",", ":"))
    aws.call("ssm", "PutParameter", Name=parameter, Value=value, Type="String", Overwrite=True)


def cost_refresh_due(state: dict, needed: list, now: datetime, refresh_hours: float) -> bool:
    """Is a Cost Explorer call due?

    Due when the kept figures have aged past the refresh interval, when none are held, or when a settled day in
    the range is missing from it.  That last case is what makes a cluster that was switched off for a week
    refill them on its first evaluation rather than on its fifth hour.
    """
    if not state.get("cost_as_of") or not state.get("cost_by_day"):
        return True
    try:
        as_of = datetime.fromisoformat(state["cost_as_of"])
    except ValueError:
        return True
    if now - as_of >= timedelta(hours=refresh_hours):
        return True
    return any(day.isoformat() not in state["cost_by_day"]
               for day in needed if is_settled(day, now))


# ---------------------------------------------------------------------------------------------------
# The model: what a vCPU-hour costs, and what the cluster costs standing still
# ---------------------------------------------------------------------------------------------------







# ---------------------------------------------------------------------------------------------------
# The caps
# ---------------------------------------------------------------------------------------------------

def read_caps(aws: Aws, parameter: str) -> dict:
    """The three caps, from SSM.

    Read on every evaluation rather than baked into the Lambda's environment, so a cap can be raised during
    an incident with one `aws ssm put-parameter` and no deploy.  Layer 1 owns the value, so the next
    `tofu apply` puts the configured figure back; ../README.md says so where it says this.

    Raises AwsError, which the caller turns into a braked cluster: with no caps there is nothing to compare
    against, and a spend guard that cannot read its caps is not watching anything.
    """
    answer = aws.call("ssm", "GetParameter", Name=parameter)
    held = json.loads(answer["Parameter"]["Value"])
    caps = {}
    for window in WINDOWS:
        value = held.get(window)
        caps[window] = None if value in (None, "", 0) else float(value)
    if all(cap is None for cap in caps.values()):
        raise AwsError(f"the caps parameter {parameter} sets no cap at all")
    return caps


# ---------------------------------------------------------------------------------------------------
# Deciding, and acting
# ---------------------------------------------------------------------------------------------------

def evaluate(aws: Aws, config: dict, now: datetime, allow_cost_explorer: bool = True) -> dict:
    """Everything the report and the brake both need.  Reads only."""
    result = {
        "cluster": config["cluster"],
        "region": aws.region,
        "now": now,
        "caps": None,
        "windows": [],
        "model": None,
        "unknown": [],
        "warnings": [],
        "vcpu_hours_today": 0.0,
        "sample_interval": None,
    }

    try:
        result["caps"] = read_caps(aws, config["caps_parameter"])
    except (AwsError, KeyError, ValueError) as error:
        result["unknown"].append(f"the caps could not be read: {error}")

    # The whole range any window or the fit needs, in one pair of reads.  A week beginning on a Monday can
    # start in the previous month, so the earliest window start is taken rather than the month's.
    earliest_window = min(window_start(window, now) for window in WINDOWS)
    earliest = min(earliest_window, now - timedelta(days=CALIBRATION_DAYS))
    range_start = earliest.replace(hour=0, minute=0, second=0, microsecond=0)

    # Read before CloudWatch, because it carries the sample interval a previous evaluation measured.
    state = read_state(aws, config["state_parameter"])

    try:
        usage = vcpu_usage(aws, range_start, now, now, known=state.get("sample_interval"))
    except AwsError as error:
        result["unknown"].append(f"CloudWatch would not answer, so what ran is unknown: {error}")
        vcpu_hours = {}
    else:
        vcpu_hours = usage["hours"]
        result["sample_interval"] = usage["interval"]
        result["vcpu_hours_today"] = vcpu_hours.get(now.date(), 0.0)

        # Kept for the evaluation that cannot measure it.  Written only when it changes, so this is one
        # PutParameter on the first evaluation and none afterwards.
        if usage["measured"] is not None and usage["measured"] != state.get("sample_interval"):
            state["sample_interval"] = usage["measured"]
            result["state_dirty"] = True
        if usage["measured"] is None and usage["interval"] is not None:
            result["warnings"].append(
                f"nothing has been published to the vCPU metric for six hours, so its interval could not be"
                f" measured and the {usage['interval']:.0f}s a previous evaluation measured was used. That is"
                " a property of the metric rather than of the load, so it holds; the figures for a day whose"
                " load ended more than six hours ago are the ones to check against the bill.")

        if usage["interval"] is None:
            # No datapoints at all.  Either nothing is running, which is a spend of zero, or the read is
            # broken, and the two answers are the same empty list.
            try:
                running = anything_running(aws)
            except AwsError as error:
                result["unknown"].append(f"the metric is empty and EC2 would not say whether anything is"
                                         f" running: {error}")
            else:
                if running:
                    result["unknown"].append(
                        "instances are running and the vCPU metric has no datapoints at all,"
                        f" so what ran is unknown. Confirm {USAGE_NAMESPACE} {USAGE_METRIC} carries"
                        f" {USAGE_DIMENSIONS}.")
        elif usage["interval"] not in USAGE_KNOWN_INTERVALS:
            # Not a fault: the interval is measured and the arithmetic follows it, so an unfamiliar one is
            # right rather than wrong.  Reported because it is the figure every other figure scales with.
            result["warnings"].append(
                f"the vCPU metric was sampled {usage['samples']:.0f} times in its fullest hour, which is every"
                f" {usage['interval']:.0f}s rather than the 60s or 300s AWS publishes it at. Every figure"
                " below scales with that interval, so confirm it before acting on them:"
                f" aws cloudwatch get-metric-statistics --namespace {USAGE_NAMESPACE}"
                f" --metric-name {USAGE_METRIC} --statistics SampleCount --period {USAGE_PERIOD_SECONDS}")

    needed = days_between(range_start, now)
    if allow_cost_explorer and cost_refresh_due(state, needed, now, config["cost_refresh_hours"]):
        try:
            fresh = cost_by_day(aws, range_start.date(), now.date() + timedelta(days=1),
                               config["cost_metric"])
        except AwsError as error:
            result["warnings"].append(
                f"Cost Explorer would not answer, so the figures below are estimated from what ran and"
                f" the fit is the one it was last given: {error}")
        else:
            state["cost_by_day"].update({day.isoformat(): cost for day, cost in fresh.items()})
            state["cost_as_of"] = now.isoformat()
            result["cost_refreshed"] = True
    elif not allow_cost_explorer:
        result["warnings"].append("Cost Explorer was not called, so only the figures already kept are used.")

    if not state["cost_by_day"]:
        result["warnings"].append(
            "no Cost Explorer figure for any day, so every figure below is an estimate and the fallback"
            " price is in force. Cost Explorer has to be enabled once per account, and takes up to 24"
            " hours to answer after it is.")

    result["state"] = state
    result["model"] = calibrate(state["cost_by_day"], vcpu_hours, now,
                                config["fallback_price"], config["fallback_fixed"])
    result["day_figures"] = day_figures(days_between(earliest_window, now), now,
                                        state["cost_by_day"], vcpu_hours, result["model"])

    # The model against the bill, on the days where both figures exist.  This is what tells a reader whether
    # today's figure, which no bill can check yet, is worth anything at all.
    result["audit"] = audit_model(result["day_figures"])

    # A configured guess that the bill contradicts by a factor is brought onto the bill rather than left to
    # brake the cluster on it.  Only the fallback is scaled: a fit that the days identified is already the
    # account's own figure, and scaling it would be fitting the same data twice.
    if result["model"]["mode"] == "fallback" and result["audit"]["off"]:
        result["model"] = scale_to_bill(result["model"], result["day_figures"], result["audit"])
        result["day_figures"] = day_figures(days_between(earliest_window, now), now,
                                           state["cost_by_day"], vcpu_hours, result["model"])
        result["audit"] = audit_model(result["day_figures"])

    if result["audit"]["off"]:
        ratio = result["audit"]["ratio"]
        result["warnings"].append(
            f"over {result['audit']['days']} settled day(s) the estimate reads"
            f" {money(result['audit']['estimated'])} against a bill of"
            f" {money(result['audit']['billed'])}, which is {ratio:.3g}x."
            " Those days are counted at the bill, so the windows are right about them; today is not billed"
            " yet and is a model figure, so it carries the same error. Read `--days` before acting on it, and"
            " set var.spend_price_per_vcpu_hour and var.spend_fixed_usd_per_day from what the bill says.")

    for window in WINDOWS:
        row = spend_in_window(window, now, result["day_figures"])
        cap = (result["caps"] or {}).get(window)
        row["cap"] = cap
        row["over"] = cap is not None and row["spend"] > cap
        result["windows"].append(row)

    result["over"] = [row["window"] for row in result["windows"] if row["over"]]
    result["should_brake"] = bool(result["over"]) or bool(result["unknown"])
    return result


def brake_state(aws: Aws, asg_names: list) -> dict:
    """Which agent groups have the brake on, read off the groups themselves."""
    suspended = {}
    for index in range(0, len(asg_names), ASG_NAMES_PER_CALL):
        chunk = asg_names[index:index + ASG_NAMES_PER_CALL]
        answer = aws.call("autoscaling", "DescribeAutoScalingGroups", AutoScalingGroupNames=chunk)
        for group in answer.get("AutoScalingGroups", []):
            processes = {each["ProcessName"] for each in group.get("SuspendedProcesses", [])}
            suspended[group["AutoScalingGroupName"]] = "Launch" in processes
    return suspended


def set_brake(aws: Aws, asg_names: list, on: bool) -> list:
    """Suspend or resume BRAKE_PROCESSES on every named group.  Returns the groups changed.

    Idempotent per group, and it acts on each one whatever the others did: a group that answers with an
    error must not leave the rest of the pools running, and suspending an already suspended process is
    accepted rather than an error.
    """
    operation = "SuspendProcesses" if on else "ResumeProcesses"
    changed = []
    failures = []
    for name in asg_names:
        try:
            aws.call("autoscaling", operation,
                     AutoScalingGroupName=name, ScalingProcesses=BRAKE_PROCESSES)
            changed.append(name)
        except AwsError as error:
            failures.append(f"{name}: {error}")
    if failures:
        raise AwsError(f"{operation} failed on {len(failures)} of {len(asg_names)} group(s): "
                       + "; ".join(failures))
    return changed


def deployed_guard(aws: Aws, function_name: str) -> dict:
    """When the Lambda was last deployed, against when this file was last changed.

    One file runs in two places, and that is only one arithmetic while the two copies are the same version.
    `make spend` reads the file on disk; the brake was set by the Lambda, which runs whatever `make apply`
    last put there.  A report that cannot see that difference sends the reader to look for a fault in the
    figures, when the answer is that the deployed guard computed different ones.

    Returns None when there is no function to ask about or AWS will not say.  `stale` is this file being newer
    than the deploy, which proves the file changed and not that the change matters.
    """
    if not function_name:
        return None
    try:
        answer = aws.call("lambda", "GetFunctionConfiguration", FunctionName=function_name)
        deployed = as_datetime(answer["LastModified"])
    except (AwsError, KeyError, ValueError):
        return None
    changed = datetime.fromtimestamp(os.path.getmtime(__file__), tz=timezone.utc)
    return {"deployed": deployed, "changed": changed, "stale": changed > deployed}


def publish_metrics(aws: Aws, config: dict, result: dict, braked: bool) -> None:
    """Five metrics, so that spend has a graph and a dead guard has an alarm.

    EvaluationOk is the heartbeat: the alarm on it treats missing data as breaching, which is what catches a
    Lambda that has stopped running rather than one that is running and unhappy.
    """
    cluster = [{"Name": "ClusterName", "Value": config["cluster"]}]
    data = [{
        "MetricName": "EstimatedSpendUsd",
        "Dimensions": cluster + [{"Name": "Window", "Value": row["window"]}],
        "Value": round(row["spend"], 4),
        "Unit": "None",
    } for row in result["windows"]]
    data.append({"MetricName": "Braked", "Dimensions": cluster,
                 "Value": 1 if braked else 0, "Unit": "None"})
    data.append({"MetricName": "EvaluationOk", "Dimensions": cluster,
                 "Value": 0 if result["unknown"] else 1, "Unit": "None"})
    aws.call("cloudwatch", "PutMetricData", Namespace=config["metric_namespace"], MetricData=data)


def notify(aws: Aws, config: dict, subject: str, body: str) -> None:
    if not config["topic_arn"]:
        return
    # SNS truncates a subject at 100 characters and rejects a newline in one.
    aws.call("sns", "Publish", TopicArn=config["topic_arn"],
             Subject=subject.replace("\n", " ")[:100], Message=body)


# ---------------------------------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------------------------------

def money(amount) -> str:
    return "-" if amount is None else f"${amount:,.2f}"


def render_days(result: dict) -> list:
    """Every day behind the totals: what ran, what it was billed, what it was estimated at.

    The figure a cap is met by is a sum, and a sum nobody can decompose is a figure nobody can argue with.
    `estimated` is split into its two terms, so a day that reads high can be attributed to the load or to the
    standing-still figure without doing the arithmetic by hand.
    """
    model = result["model"]
    how = {"calibrated": "fitted", "scaled": "scaled onto the bill"}.get(model["mode"], "configured")
    lines = [f"  Day by day, at ${model['price']:.4f} a vCPU-hour and"
             f" {money(model['fixed_per_day'])} a day standing still ({how}):", ""]
    lines.append(f"  {'day':<12}{'vCPU-hours':>12}{'from load':>12}{'+ standing':>12}"
                 f"{'= estimate':>12}{'billed':>12}{'settled':>9}{'taken':>11}")
    for day in sorted(result["day_figures"]):
        row = result["day_figures"][day]
        load = row["estimated"] - row["fixed_part"]
        lines.append(f"  {day.isoformat():<12}{row['vcpu_hours']:>12,.0f}{money(load):>12}"
                     f"{money(row['fixed_part']):>12}{money(row['estimated']):>12}"
                     f"{money(row['billed']):>12}{('yes' if row['settled'] else 'no'):>9}"
                     f"{row['taken']:>11}")
    lines.append("")
    lines.append("  A settled day takes the bill, whatever the estimate makes of it: 48 hours after a day ends")
    lines.append("  there is nothing left for a model to add. An unsettled day takes the larger of the two,")
    lines.append("  because Cost Explorer's figure for it is partial.")
    audit = result.get("audit") or {}
    if audit.get("ratio"):
        lines.append(f"  Over the {audit['days']} settled day(s) the estimate reads"
                     f" {money(audit['estimated'])} against a bill of {money(audit['billed'])},"
                     f" which is {audit['ratio']:.3g}x.")
    return lines


def render(result: dict, brake: dict = None, days: bool = False) -> str:
    """The table, and every caveat that applies to it.  One renderer, so SNS and a terminal agree."""
    lines = [f"Cluster {result['cluster']} in {result['region']},"
             f" at {result['now'].strftime('%Y-%m-%d %H:%M')} UTC", ""]

    lines.append(f"  {'window':<8}  {'since':<16}  {'spent':>12}  {'cap':>12}  {'used':>6}")
    for row in result["windows"]:
        used = "-" if not row["cap"] else f"{100.0 * row['spend'] / row['cap']:.0f}%"
        flag = "  OVER" if row["over"] else ""
        lines.append(f"  {row['window']:<8}  {row['start'].strftime('%Y-%m-%d %H:%M'):<16}"
                     f"  {money(row['spend']):>12}  {money(row['cap']):>12}  {used:>6}{flag}")
    lines.append("")

    model = result["model"]
    if model["mode"] == "calibrated":
        margin = f" +/- {money(model['rms'])} a day" if model["rms"] is not None else ""
        lines.append(f"  Fitted over {model['days']} settled day(s): ${model['price']:.4f} a vCPU-hour"
                     f" and {money(model['fixed_per_day'])} a day standing still{margin}.")
    else:
        if model["mode"] == "scaled":
            lines.append(f"  Not fitted, and the configured figures read {model['ratio']:.3g}x the bill over"
                         f" {model['days']} settled day(s), so they are scaled onto it:"
                         f" ${model['price']:.4f} a vCPU-hour and"
                         f" {money(model['fixed_per_day'])} a day standing still.")
            lines.append(f"  How: {model['how']}.")
        else:
            lines.append(f"  Not fitted, so the configured figures are in force: ${model['price']:.4f}"
                         f" a vCPU-hour and {money(model['fixed_per_day'])} a day standing still.")
        if model["why"]:
            lines.append(f"  Why it was not fitted: {model['why']}.")
        # The refused fit's own figures.  The standing-still term survives a fortnight that cannot identify a
        # rate, and it is the figure to set var.spend_fixed_usd_per_day from.
        refused = model.get("fitted")
        if refused:
            margin = f" +/- {money(refused['rms'])} a day" if refused["rms"] is not None else ""
            lines.append(f"  Those {refused['days']} day(s) would have fitted"
                         f" ${refused['price']:.4f} a vCPU-hour and"
                         f" {money(refused['fixed_per_day'])} a day standing still{margin}.")
    billed = sum(row["billed_days"] for row in result["windows"])
    sampled = ("" if not result["sample_interval"]
               else f", from a metric sampled every {result['sample_interval']:.0f}s")
    lines.append(f"  {result['vcpu_hours_today']:,.0f} vCPU-hours so far today{sampled}."
                 f" {billed} day-figure(s) across the three windows came from Cost Explorer rather than"
                 " from the estimate.")

    if brake is not None:
        on = [name for name, suspended in sorted(brake.items()) if suspended]
        if not brake:
            lines += ["", "  No agent Auto Scaling group was named, so nothing can be braked."]
        elif len(on) == len(brake):
            lines += ["", f"  BRAKED: Launch is suspended on all {len(brake)} agent group(s)."
                          " No new agent can start; the ones running will finish and their nodes"
                          " will scale in."]

            # The brake was set by the Lambda, and these figures were computed here.  When nothing here
            # justifies the brake, the difference between the two is the answer, so it is stated rather than
            # left for the reader to deduce from a table that looks fine.
            if not result["over"] and not result["unknown"]:
                lines.append("  Nothing above justifies that: every window is under its cap and spend was"
                             " read. The brake was set by the")
                lines.append("  Lambda, which runs the copy of this file that `make apply` last deployed, so"
                             " it is deciding on other figures.")
                guard = result.get("deployed")
                if guard and guard["stale"]:
                    lines.append(f"  This file was changed at"
                                 f" {guard['changed'].strftime('%Y-%m-%d %H:%M')} UTC and the function was"
                                 f" deployed at {guard['deployed'].strftime('%Y-%m-%d %H:%M')} UTC, so the"
                                 " deployed copy is the older one:")
                    lines.append("    make apply")
                elif guard:
                    lines.append(f"  The function was deployed at"
                                 f" {guard['deployed'].strftime('%Y-%m-%d %H:%M')} UTC, which is no older"
                                 " than this file, so read its own log for what it decided:")
                    lines.append("    aws logs tail /aws/lambda/<cluster>-spend-guard --since 15m")
                lines.append("  Then either wait for the next evaluation, or make it happen now:")
                lines.append("    aws lambda invoke --function-name <cluster>-spend-guard /dev/stdout")
        elif on:
            lines += ["", f"  PART BRAKED: Launch is suspended on {len(on)} of {len(brake)} agent"
                          " group(s), so the pools are inconsistent. The next evaluation makes them"
                          " uniform."]
        else:
            lines += ["", f"  Running: Launch is not suspended on any of the {len(brake)} agent group(s)."]

    if days and result.get("day_figures"):
        lines += [""] + render_days(result)

    for message in result["unknown"]:
        lines += ["", f"  UNKNOWN: {message}"]
    for message in result["warnings"]:
        lines += ["", f"  WARNING: {message}"]

    return "\n".join(lines)


# ---------------------------------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------------------------------

def config_from_environment() -> dict:
    """Every value this needs, under the names ../Makefile exports and 1-cluster/spend-guard.tf sets.

    One set of names for both, rather than a Lambda-shaped set and a shell-shaped set: two names for one
    value is two places to change it and one place to forget.
    """
    missing = [name for name in ("EKS_CLUSTER_NAME", "EKS_SPEND_CAPS_PARAMETER",
                                 "EKS_SPEND_STATE_PARAMETER")
               if not os.environ.get(name)]
    if missing:
        raise ValueError(f"{', '.join(missing)} not set. Run this through ../Makefile, which fills the"
                         " environment from `tofu output`; see 1-cluster/outputs.tf.")
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if not region:
        raise ValueError("AWS_REGION is not set.")
    return {
        "cluster": os.environ["EKS_CLUSTER_NAME"],
        "region": region,
        "caps_parameter": os.environ["EKS_SPEND_CAPS_PARAMETER"],
        "state_parameter": os.environ["EKS_SPEND_STATE_PARAMETER"],
        "asg_names": os.environ.get("EKS_SPEND_AGENT_ASG_NAMES", "").split(),
        "topic_arn": os.environ.get("EKS_SPEND_ALERT_TOPIC_ARN", ""),
        "metric_namespace": os.environ.get("EKS_SPEND_METRIC_NAMESPACE",
                                           "CassandraJenkins/SpendGuard"),
        "cost_metric": os.environ.get("EKS_SPEND_COST_METRIC", "UnblendedCost"),
        "fallback_price": float(os.environ.get("EKS_SPEND_PRICE_PER_VCPU_HOUR", "0.06")),
        "fallback_fixed": float(os.environ.get("EKS_SPEND_FIXED_USD_PER_DAY", "5.0")),
        "cost_refresh_hours": float(os.environ.get("EKS_SPEND_CE_REFRESH_HOURS", "6")),
        # Only for the report, and only when the brake is on for no reason this file can see.
        "function_name": os.environ.get("EKS_SPEND_GUARD_FUNCTION", ""),
    }


# ---------------------------------------------------------------------------------------------------
# The two entry points
# ---------------------------------------------------------------------------------------------------

def brake_subject(cluster: str, braked: bool, over: list) -> str:
    """The one line an email client shows, so it has to carry which cap and which cluster.

    Separated from enforce() because the unknown case reads as a cap that was met when it is written as one:
    a subject saying "spend could not be established is met" is a sentence nobody can act on.
    """
    if not braked:
        return f"{cluster}: agent pools released, spend is under every cap"
    if not over:
        return f"{cluster}: agent pools stopped, spend could not be established"
    if len(over) == 1:
        return f"{cluster}: agent pools stopped, the {over[0]} cap is met"
    named = " and ".join([", ".join(over[:-1]), over[-1]])
    return f"{cluster}: agent pools stopped, the {named} caps are met"


def enforce(aws: Aws, config: dict, now: datetime, dry_run: bool = False) -> dict:
    """Evaluate, then make the brake match the answer.  Announces a change and not a state."""
    result = evaluate(aws, config, now)

    if (result.get("cost_refreshed") or result.get("state_dirty")) and not dry_run:
        try:
            write_state(aws, config["state_parameter"], result["state"])
        except AwsError as error:
            result["warnings"].append(f"the Cost Explorer figures could not be kept, so the next"
                                      f" evaluation pays for them again: {error}")

    try:
        current = brake_state(aws, config["asg_names"]) if config["asg_names"] else {}
    except AwsError as error:
        result["unknown"].append(f"the agent groups could not be read, so the brake's state is unknown:"
                                 f" {error}")
        result["should_brake"] = True
        current = {}

    want = result["should_brake"]
    have = bool(current) and all(current.values())
    uniform = len(set(current.values())) <= 1
    result["brake_before"] = current
    result["acted"] = False

    if config["asg_names"] and (want != have or not uniform):
        if dry_run:
            result["warnings"].append(f"a dry run: the brake would have been turned"
                                      f" {'on' if want else 'off'} here.")
        else:
            try:
                set_brake(aws, config["asg_names"], want)
                result["acted"] = True
                current = {name: want for name in config["asg_names"]}
            except AwsError as error:
                result["unknown"].append(f"the brake could not be set: {error}")

    # What the brake is, and not what it was meant to be: a SuspendProcesses that failed leaves the pools
    # running, and a metric saying otherwise would be the one place a reader trusts.
    braked = bool(current) and all(current.values())

    if not dry_run:
        try:
            publish_metrics(aws, config, result, braked=braked)
        except AwsError as error:
            result["warnings"].append(f"the metrics could not be published, so the alarm on this guard"
                                      f" will fire: {error}")

    result["brake_after"] = current
    body = render(result, current)

    if not dry_run and (result["acted"] or (result["unknown"] and not have)):
        notify(aws, config, brake_subject(config["cluster"], want, result["over"]), body)

    result["report"] = body
    return result


def handler(event, context):  # noqa: ARG001 - the Lambda signature
    """What the EventBridge schedule invokes.  See 1-cluster/spend-guard.tf."""
    config = config_from_environment()
    result = enforce(Aws(config["region"]), config, utc_now())
    # Printed whole into the log group, so that the figures behind a decision survive the decision.
    print(result["report"])
    return {
        "braked": bool(result["brake_after"]) and all(result["brake_after"].values()),
        "over": result["over"],
        "unknown": result["unknown"],
        "spend": {row["window"]: round(row["spend"], 4) for row in result["windows"]},
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="What this account has spent against its caps, and whether the agent pools should run.")
    parser.add_argument("--report", action="store_true",
                        help="Read and print, changing nothing. The default, and named so that a caller"
                             " says which of the two it wants rather than relying on the default.")
    parser.add_argument("--enforce", action="store_true",
                        help="Also suspend or resume the agent pools, which is what the Lambda does."
                             " Without it this reads only.")
    parser.add_argument("--no-cost-explorer", action="store_true",
                        help="Use the figures already kept. Each Cost Explorer call costs a cent.")
    parser.add_argument("--days", action="store_true",
                        help="Also print every day behind the totals: what ran, what it was billed, what it"
                             " was estimated at, and which of the two was taken.")
    parser.add_argument("--json", action="store_true",
                        help="Print the figures as JSON instead of a table.")
    args = parser.parse_args()

    try:
        config = config_from_environment()
    except ValueError as error:
        print(f"{error}", file=sys.stderr)
        return 2

    aws = Aws(config["region"])
    now = utc_now()

    if args.enforce:
        result = enforce(aws, config, now)
    else:
        result = evaluate(aws, config, now, allow_cost_explorer=not args.no_cost_explorer)
        try:
            result["brake_after"] = brake_state(aws, config["asg_names"]) if config["asg_names"] else {}
        except AwsError as error:
            result["warnings"].append(f"the agent groups could not be read, so whether the brake is on is"
                                      f" unknown: {error}")
            result["brake_after"] = {}
        braked = bool(result["brake_after"]) and all(result["brake_after"].values())
        if braked and not result["over"] and not result["unknown"]:
            result["deployed"] = deployed_guard(aws, config["function_name"])
        result["report"] = render(result, result["brake_after"], days=args.days)

    if args.json:
        print(json.dumps({
            "cluster": result["cluster"],
            "at": result["now"].isoformat(),
            "windows": [{
                "window": row["window"],
                "since": row["start"].isoformat(),
                "spend": round(row["spend"], 4),
                "cap": row["cap"],
                "over": row["over"],
            } for row in result["windows"]],
            "model": {key: value for key, value in result["model"].items()},
            "sample_interval_seconds": result["sample_interval"],
            "audit": result.get("audit"),
            # Every day any window covers, because a total is a sum of these and a sum nobody can decompose
            # is a figure nobody can argue with.
            "days": [{
                "day": day.isoformat(),
                "vcpu_hours": round(row["vcpu_hours"], 2),
                "billed": row["billed"],
                "estimated": round(row["estimated"], 4),
                "estimated_from_load": round(row["estimated"] - row["fixed_part"], 4),
                "estimated_standing_still": round(row["fixed_part"], 4),
                "settled": row["settled"],
                "taken": row["taken"],
            } for day, row in sorted(result.get("day_figures", {}).items())],
            "braked": bool(result["brake_after"]) and all(result["brake_after"].values()),
            "unknown": result["unknown"],
            "warnings": result["warnings"],
        }, indent=2, default=str))
    else:
        print(result["report"])

    if result["unknown"]:
        return 2
    braked = bool(result["brake_after"]) and all(result["brake_after"].values())
    return 1 if (result["over"] or braked) else 0


if __name__ == "__main__":
    sys.exit(main())
