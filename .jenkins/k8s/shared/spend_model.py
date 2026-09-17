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

"""Spend windows, and fitting a bill onto what ran.

No cloud appears here.  What is billed and what is measured are the caller's: it hands over a map of dates to
billed amounts and a map of dates to usage, and gets back per-day figures and per-window totals.  Every
function takes `now` rather than reading a clock, which is what makes a month boundary, a Monday and a day
that has not finished reachable by an offline test.

## Why spend is measured twice

No cloud publishes a low-latency spend feed.  A billing API is refreshed about once a day and its figure for
the current day is partial, so a daily cap enforced from the bill alone reacts a day late, which on pools
costing a few hundred dollars an hour is the whole cap and more.

So each day takes the bill once the bill has settled, and before that the larger of the bill and an estimate
from usage:

    spend(window) = sum over its days of
        billed                            if the day has settled
        max(billed, estimate)             otherwise

A settled day taking the bill is not a detail.  Taking the larger everywhere holds only while the model is
right, and when it is not, the error reaches into every past day of every window; that is how a monthly figure
once read thirty times the bill.  With this rule a model error can reach only the last two days.

## The estimate, and why its two terms are fitted

    estimate(day) = price * usage(day) + fixed_per_day * fraction of the day elapsed

`price` and `fixed_per_day` are fitted by least squares over the settled days of the last fortnight rather
than written down.  What that buys is that no price is written anywhere: root volumes, cross-zone transfer,
the control plane's hourly charge and the load balancer are all inside the days it fits, so a price change or
a new instance type moves the model on its own.

`price` is not a list price, and treating it as one broke this once.  It is dollars of bill per unit of usage
the metric reports, on this account, so a discount held anywhere and any scaling between the metric and
reality land inside it.  One account fitted $0.0010 where the list price was $0.058, and a bound around the
configured guess refused that measurement and braked the cluster on a figure forty times the truth.  Hence
CALIBRATION_MAX_RATE being absolute; see `.jenkins/k8s/eks/NOTES.md`.
"""

import math
from datetime import date, datetime, timedelta, timezone

# The three windows every caller reports on.  Shared so that a caller asking for the caps and a caller
# enforcing them cannot disagree about what a week is.
WINDOWS = ("daily", "weekly", "monthly")

# A day is used for the fit once the billing API has had this long past its end.  Two days rather than one:
# the figure for a day that closed an hour ago is still moving, and a partial figure in the fit biases the
# price downwards, which is the direction that fails to brake.
SETTLED_AFTER_HOURS = 48

# How far back the fit looks, and what it needs to be believed.  Three days is the fewest that can show a
# residual at all against two fitted terms.  The spread is the coefficient of variation of usage: days that
# are too alike cannot separate a fixed cost from a per-unit one, and least squares answers anyway.
CALIBRATION_DAYS = 14
CALIBRATION_MIN_DAYS = 3
CALIBRATION_MIN_SPREAD = 0.15

# How far apart the busiest and quietest settled day have to be, in units of usage, before the slope between
# them means anything.  192 vCPU-hours is one 8 vCPU node for one day, so this is about two and a half
# node-days.  This is the test the first fortnight of a real account failed: its days were a braked cluster
# differing by a few vCPU-hours, which is a wide relative spread over a tiny absolute one, and least squares
# answered with a price near zero.  A price near zero estimates a full-size build at nothing at all.
CALIBRATION_MIN_SPAN_HOURS = 500.0

# Absolute sanity on the fitted rate, in dollars per unit of usage.  Only the absurd is refused: a negative
# rate is arithmetic rather than a cost, and a dollar a vCPU-hour is twenty times any on-demand list price.
# Deliberately not a bound around the caller's configured figure, which is the guess the fit exists to
# replace; bounding the measurement by the guess is how a written-down number outvotes six days of evidence.
CALIBRATION_MIN_RATE = 0.0
CALIBRATION_MAX_RATE = 1.0

# How far the model may be from the bill, over the settled days where both figures exist, before the report
# says the model is not to be believed.  Two, because a model right to within a factor of two is a model whose
# error is a margin; beyond that it is a model measuring the wrong thing.  Three days, so that one day of a
# lagging billing figure cannot raise it.
MODEL_AUDIT_FACTOR = 2.0
MODEL_AUDIT_MIN_DAYS = 3


def window_start(window: str, now: datetime) -> datetime:
    """The moment the given window began, in UTC.

    UTC for all three, because that is what a cloud bills and reports in.  The week starts on Monday, which
    is ISO 8601's week and the only definition that resets on a fixed weekday.
    """
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if window == "daily":
        return midnight
    if window == "weekly":
        return midnight - timedelta(days=midnight.weekday())
    if window == "monthly":
        return midnight.replace(day=1)
    raise ValueError(f"no such window: {window}")


def days_between(start: datetime, end: datetime) -> list:
    """Every UTC date from start's day to end's day, inclusive of both."""
    first, last = start.date(), end.date()
    return [first + timedelta(days=offset) for offset in range((last - first).days + 1)]


def day_elapsed_fraction(day: date, now: datetime) -> float:
    """How much of that UTC day has passed, 0 to 1.  A day in the future is 0, a past day is 1."""
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    return min(1.0, max(0.0, (now - start).total_seconds() / 86400.0))


def is_settled(day: date, now: datetime) -> bool:
    """Has the billing API had SETTLED_AFTER_HOURS since this day ended?"""
    end = datetime(day.year, day.month, day.day, tzinfo=timezone.utc) + timedelta(days=1)
    return now - end >= timedelta(hours=SETTLED_AFTER_HOURS)


def calibrate(costs: dict, usage: dict, now: datetime,
              fallback_price: float, fallback_fixed: float) -> dict:
    """Fit `cost = fixed + price * usage` over the settled days of the last fortnight.

    `costs` is keyed by ISO date string, which is how a caller keeps it in a parameter store; `usage` is keyed
    by `date`, which is how a metric read returns it.  The asymmetry is the callers', not this module's.

    Four things refuse the fit, and three are about whether the days can identify a slope at all: fewer than
    CALIBRATION_MIN_DAYS settled days with a billed figure, a span of usage under CALIBRATION_MIN_SPAN_HOURS,
    and a relative spread under CALIBRATION_MIN_SPREAD.  The fourth is absolute sanity on the rate.

    A refused fit is still reported, under `fitted`, because a fortnight of an idle cluster measures the
    standing-still cost well even when it cannot identify a rate, and that figure is what an operator sets the
    configured standing-still figure from.

    `rms` is the root mean square residual in dollars a day, which is the error margin on every figure the fit
    produces.
    """
    usable = []
    for day, hours in sorted(usage.items()):
        if not is_settled(day, now):
            continue
        if (now.date() - day).days > CALIBRATION_DAYS:
            continue
        cost = costs.get(day.isoformat())
        if cost is None:
            continue
        usable.append((hours, float(cost)))

    fallback = {
        "mode": "fallback",
        "price": fallback_price,
        "fixed_per_day": fallback_fixed,
        "days": len(usable),
        "rms": None,
        "why": None,
        "fitted": None,
    }

    if len(usable) < CALIBRATION_MIN_DAYS:
        # "a billed figure", not "a Cost Explorer figure": this module is read by both cloud directories and
        # names no cloud anywhere else, and on GCP the bill comes from a BigQuery export or from nothing at all.
        fallback["why"] = (f"{len(usable)} settled day(s) with a billed figure,"
                           f" and the fit needs {CALIBRATION_MIN_DAYS}")
        return fallback

    count = len(usable)
    mean_hours = sum(hours for hours, _ in usable) / count
    mean_cost = sum(cost for _, cost in usable) / count
    variance = sum((hours - mean_hours) ** 2 for hours, _ in usable) / count
    spread = math.sqrt(variance) / mean_hours if mean_hours > 0 else 0.0
    span = max(hours for hours, _ in usable) - min(hours for hours, _ in usable)

    covariance = sum((hours - mean_hours) * (cost - mean_cost) for hours, cost in usable)
    scatter = sum((hours - mean_hours) ** 2 for hours, _ in usable)
    price = covariance / scatter if scatter > 0 else 0.0
    fixed = mean_cost - price * mean_hours

    # A negative fixed term is arithmetic, not a cost.  Refit through the origin rather than reporting a
    # cluster that is cheaper for existing, which would under-estimate every idle day.
    if fixed < 0:
        total_hours = sum(hours for hours, _ in usable)
        price = (sum(cost for _, cost in usable) / total_hours) if total_hours > 0 else 0.0
        fixed = 0.0

    residuals = [cost - (fixed + price * hours) for hours, cost in usable]
    fitted = {
        "mode": "calibrated",
        "price": price,
        "fixed_per_day": fixed,
        "days": count,
        "rms": math.sqrt(sum(each ** 2 for each in residuals) / count),
        "why": None,
    }

    # Three ways the fit is refused, and the figures it produced are reported either way: a refused fit over
    # a fortnight is the measurement an operator sets the standing-still figure from.
    fallback["fitted"] = fitted

    # A day of a braked or idle cluster differs from the next by a few units, which is a wide relative spread
    # over a tiny absolute one, and least squares answers anyway.  What it answers with is a price near zero,
    # which then estimates a full-size build at nothing at all.  This is the test the account's own first
    # fortnight failed.
    if span < CALIBRATION_MIN_SPAN_HOURS:
        fallback["why"] = (f"the {count} settled days span {span:,.0f} vCPU-hours, under the"
                           f" {CALIBRATION_MIN_SPAN_HOURS:,.0f} a slope needs to mean anything."
                           " Idle days cannot price a busy one")
        return fallback

    if spread < CALIBRATION_MIN_SPREAD:
        fallback["why"] = (f"the {count} settled days differ in load by {spread:.0%},"
                           f" under the {CALIBRATION_MIN_SPREAD:.0%} needed to separate"
                           " a fixed cost from a per-vCPU one")
        return fallback

    # Absolute, and not held against the configured figure.  See CALIBRATION_MIN_RATE for why: a bound around
    # the guess refuses the measurement that replaces it, and this one did, by a factor of forty.
    if not CALIBRATION_MIN_RATE < price <= CALIBRATION_MAX_RATE:
        fallback["why"] = (f"the fit gave ${price:.4f} a vCPU-hour, outside ${CALIBRATION_MIN_RATE:.4f} to"
                           f" ${CALIBRATION_MAX_RATE:.2f}, which is not a rate anything is billed at")
        return fallback

    return fitted


def estimate_day(day: date, now: datetime, usage: float, model: dict) -> float:
    """What that day cost, from what ran on it.  The fixed term is pro-rated over a day still running."""
    return model["price"] * usage + model["fixed_per_day"] * day_elapsed_fraction(day, now)


def day_figures(days: list, now: datetime, costs: dict, usage: dict, model: dict) -> dict:
    """One row per day: what ran, what it was billed, what it was estimated at, and which was taken.

    Computed once over every day any window covers, because a day's figure is a fact about the day and not
    about the window asking.  Every window total is a sum of these rows, so a caller printing them prints the
    arithmetic behind a figure rather than a second calculation of it.
    """
    rows = {}
    for day in days:
        billed = costs.get(day.isoformat())
        hours = usage.get(day, 0.0)
        estimated = estimate_day(day, now, hours, model)

        # A settled day takes the bill, and the larger of the two is only asked for on a day still moving.
        # See this module's docstring for why the rule is not the larger everywhere.
        if billed is not None and is_settled(day, now):
            taken = "billed"
        elif billed is not None and float(billed) + 0.005 >= estimated:
            # Within a cent the two are the same figure, and the billed one is the authority.  Without the
            # tolerance a day where they agree exactly reads one way or the other on a float comparison,
            # which invites a question whose answer is "they are equal".
            taken = "billed"
        else:
            taken = "estimated"

        rows[day] = {
            "day": day,
            "vcpu_hours": hours,
            "billed": None if billed is None else float(billed),
            "estimated": estimated,
            "fixed_part": model["fixed_per_day"] * day_elapsed_fraction(day, now),
            "settled": is_settled(day, now),
            "taken": taken,
        }
        rows[day]["spend"] = (rows[day]["billed"] if taken == "billed" else rows[day]["estimated"])
    return rows


def audit_model(figures: dict) -> dict:
    """What the model would have said about the days the bill has already settled.

    The estimate is checkable, and nothing was checking it.  Over settled days there are two figures for the
    same day, and their ratio is the model's error against ground truth, so this is the one measurement that
    catches a model wrong by a factor rather than by a margin.  Both faults this model has had were factors,
    and both would have shown here on the first evaluation.

    It only reports.  A guard that ignored its own cap because it distrusted its own figure would be worse
    than one that brakes early; what an operator needs is to be told which of the two to believe.
    """
    days = [row for row in figures.values() if row["settled"] and row["billed"] is not None]
    billed = sum(row["billed"] for row in days)
    estimated = sum(row["estimated"] for row in days)
    audit = {"days": len(days), "billed": billed, "estimated": estimated, "ratio": None, "off": False}
    if len(days) < MODEL_AUDIT_MIN_DAYS or billed <= 0:
        return audit
    audit["ratio"] = estimated / billed
    audit["off"] = not (1 / MODEL_AUDIT_FACTOR <= audit["ratio"] <= MODEL_AUDIT_FACTOR)
    return audit


def scale_to_bill(model: dict, figures: dict, audit: dict) -> dict:
    """Bring the configured figures onto the bill, when the fit could not be identified.

    The two-parameter fit needs days that differ in load; this needs only that the bill exists.  Over the
    settled days the total is known, so the rate is whatever makes the model's total match it:

        price = (billed - fixed * days) / total usage

    which is the same fit with its intercept held at the configured standing-still figure.  Where that
    intercept alone already exceeds the bill, both terms are divided by the ratio instead, because a floor
    above the bill is a floor that is wrong.

    This exists because a configured rate can be wrong by orders of magnitude and not by a margin; see this
    module's docstring for the run where it was, by a factor of forty.
    """
    days = [row for row in figures.values() if row["settled"] and row["billed"] is not None]
    total_hours = sum(row["vcpu_hours"] for row in days)
    billed = sum(row["billed"] for row in days)
    scaled = dict(model)
    scaled["mode"] = "scaled"
    scaled["days"] = len(days)
    scaled["ratio"] = audit["ratio"]

    headroom = billed - model["fixed_per_day"] * len(days)
    if total_hours > 0 and headroom > 0:
        scaled["price"] = headroom / total_hours
        scaled["how"] = ("the configured standing-still figure held, and the rate set so that the settled"
                         " days add up to the bill")
    else:
        scaled["price"] = model["price"] / audit["ratio"]
        scaled["fixed_per_day"] = model["fixed_per_day"] / audit["ratio"]
        scaled["how"] = ("both configured figures divided by that ratio, the standing-still figure alone"
                         " being above the bill")
    return scaled


def spend_in_window(window: str, now: datetime, figures: dict) -> dict:
    """What has been spent in this window, summed from the day rows above."""
    start = window_start(window, now)
    days = days_between(start, now)
    rows = [figures[day] for day in days if day in figures]
    return {
        "window": window,
        "start": start,
        "days": len(days),
        "billed_days": len([row for row in rows if row["taken"] == "billed"]),
        "spend": sum(row["spend"] for row in rows),
    }
