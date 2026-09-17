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
"""Configure USD spend caps and save them atomically as OpenTofu variables."""

import argparse
import os
import json
import tempfile
import re
import sys
from datetime import date
from pathlib import Path

# The three windows, from the same place ../spend-guard.py takes them, so that the script asking for a cap and
# the script enforcing it cannot disagree about what a week is.  See ../shared/README.md.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "shared"))
from spend_model import WINDOWS  # noqa: E402 - after the sys.path line above

# The default file, relative to this script.  Loaded automatically by every `tofu` command in 1-cluster.
DEFAULT_OUT = "1-cluster/spend-caps.auto.tfvars"

DEFAULT_PRICE_PER_VCPU_HOUR = 0.06
DEFAULT_FIXED_USD_PER_DAY = 5.0
DEFAULT_VCPUS_PER_NODE = 8

# What the suggestion offers, in hours of the pools at full size.  Four hours is about one post-commit run:
# ../NOTES.md records one at 3 h 30 m with the pools saturated.
SUGGESTED_DAILY_HOURS = 4.0
SUGGESTED_WEEKLY_DAYS = 3.0
SUGGESTED_MONTHLY_DAYS = 10.0

DAYS_IN = {"daily": 1.0, "weekly": 7.0, "monthly": 30.0}
PERIOD = {"daily": "day", "weekly": "week", "monthly": "month"}

CAP_VARIABLE = {window: f"spend_cap_{window}_usd" for window in WINDOWS}
EMAIL_VARIABLE = "spend_alert_email"

HEADER = """# Written by ../spend-caps.py on {today}; edit with make caps. This file is gitignored.
# A null cap disables that window.
"""


def money(amount) -> str:
    """Whole dollars, and cents only where the figure is small enough for them to matter."""
    if amount is None:
        return "-"
    return f"${amount:,.2f}" if abs(amount) < 10 else f"${amount:,.0f}"


def parse_amount(answer: str):
    """A typed cap. `none` and `0` are no cap; `1.2k` is 1200; `$900` and `1,000` are what they look like."""
    text = answer.strip().lower().replace("$", "").replace(",", "").replace(" ", "")
    if text in ("none", "no", "off", "unlimited", "0"):
        return None
    match = re.fullmatch(r"(\d+(?:\.\d+)?)(k|m)?", text)
    if not match:
        raise ValueError(f"'{answer.strip()}' is not an amount. Type a number of dollars, or `none`.")
    amount = float(match.group(1)) * {None: 1, "k": 1_000, "m": 1_000_000}[match.group(2)]
    if amount < 1:
        raise ValueError(f"${amount:g} is not a cap anything could run under. Type a larger number,"
                         " or `none` for no cap.")
    return amount


def parse_email(answer: str) -> str:
    text = answer.strip()
    if text.lower() in ("", "none", "no"):
        return ""
    if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", text):
        raise ValueError(f"'{text}' is not an email address. Type one, or `none`.")
    return text


def read_existing(path: Path) -> dict:
    """The caps already written, so that a re-run offers them back rather than starting from nothing."""
    held = {window: None for window in WINDOWS}
    held["email"] = ""
    held["exists"] = path.exists()
    if not held["exists"]:
        return held
    text = path.read_text(encoding="utf-8")
    held["valid"] = all(re.search(rf"^\s*{name}\s*=\s*(null|[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)\s*$", text, re.MULTILINE) for name in CAP_VARIABLE.values())
    for window, name in CAP_VARIABLE.items():
        match = re.search(rf"^\s*{name}\s*=\s*(\S+)", text, re.MULTILINE)
        if match and match.group(1) != "null":
            try:
                held[window] = float(match.group(1))
            except ValueError:
                pass
    match = re.search(rf'^\s*{EMAIL_VARIABLE}\s*=\s*("(?:[^"\\]|\\.)*")\s*$', text, re.MULTILINE)
    if match:
        try:
            held["email"] = json.loads(match.group(1)).replace("$${", "${").replace("%%{", "%{")
        except ValueError:
            held["valid"] = False
    else:
        held["valid"] = False
    return held


def arithmetic(price: float, fixed_per_day: float, vcpus_per_node: int, max_agent_nodes) -> dict:
    """The three figures the prompt is built on."""
    node_hour = price * vcpus_per_node
    controller_per_day = node_hour * 24
    floor_per_day = fixed_per_day + controller_per_day
    return {
        "node_hour": node_hour,
        "floor_per_day": floor_per_day,
        "floor": {window: floor_per_day * DAYS_IN[window] for window in WINDOWS},
        "burn_per_hour": None if max_agent_nodes is None else node_hour * max_agent_nodes,
        "max_agent_nodes": max_agent_nodes,
    }


def suggestions(figures: dict) -> dict:
    """A cap to offer for each window, or None when the pools' size is not known here."""
    if figures["burn_per_hour"] is None:
        return {window: None for window in WINDOWS}
    daily = figures["floor"]["daily"] + figures["burn_per_hour"] * SUGGESTED_DAILY_HOURS
    return {
        "daily": round(daily, -1),
        "weekly": round(figures["floor"]["weekly"]
                        + figures["burn_per_hour"] * SUGGESTED_DAILY_HOURS * SUGGESTED_WEEKLY_DAYS, -1),
        "monthly": round(figures["floor"]["monthly"]
                         + figures["burn_per_hour"] * SUGGESTED_DAILY_HOURS * SUGGESTED_MONTHLY_DAYS, -1),
    }


def describe(figures: dict, caps: dict, out: Path) -> str:
    """What each cap means against the floor and the burn.  Shared by --check and the prompt's summary."""
    lines = []
    lines.append(f"  Standing still, with no build running, this cluster costs about"
                 f" {money(figures['floor_per_day'])} a day:")
    lines.append(f"    the EKS control plane, the load balancer, the controller's volume and its logs, and")
    lines.append(f"    the controller's own node at about {money(figures['node_hour'])} an hour, all 24 of them.")
    if figures["burn_per_hour"] is not None:
        lines.append(f"  At full size the {figures['max_agent_nodes']} agent nodes add about"
                     f" {money(figures['burn_per_hour'])} an hour on top of that.")
    else:
        lines.append("  How many agent nodes this account allows is not known here, so no figure below")
        lines.append("  includes them. Run `make quota` first, and then `make caps` again, for that half.")
    lines.append("")
    for window in WINDOWS:
        cap = caps.get(window)
        floor = figures["floor"][window]
        if cap is None:
            lines.append(f"  {window:<8} no cap")
            continue
        share = f"{100.0 * floor / cap:.0f}% of it is the floor"
        hours = ""
        if figures["burn_per_hour"]:
            spare = max(0.0, cap - floor)
            hours = f", leaving {spare / figures['burn_per_hour']:.1f} hour(s) of the pools at full size"
        lines.append(f"  {window:<8} {money(cap):>10}   {share}{hours}")
    lines.append("")
    lines.append(f"  Written to {out}")
    return "\n".join(lines)


def refuse_reason(window: str, cap, figures: dict):
    """Why a cap cannot be used, or None."""
    if cap is None:
        return None
    floor = figures["floor"][window]
    if cap <= floor:
        return (f"{money(cap)} is under the {money(floor)} this cluster costs over a {PERIOD[window]}"
                f" standing still, so the pools would be braked for the whole window and no build could"
                f" ever run. Set at least {money(floor * 1.5)}, or `none`.")
    return None


def ask(question: str, default_text: str) -> str:
    """One question.  Blank takes the default, which is printed in the question."""
    sys.stdout.write(f"{question} [{default_text}] ")
    sys.stdout.flush()
    line = sys.stdin.readline()
    if not line:
        raise EOFError("no answer, and this needs one. Run `make caps` from a terminal.")
    return line.strip() or ""


def prompt(figures: dict, existing: dict, attempts: int = 3) -> dict:
    """Ask for the three caps and the address to tell, offering what is already set or a suggestion."""
    offered = suggestions(figures)
    caps = {}

    print("What may this cluster spend? A blank answer takes the figure in brackets, and `none` sets no")
    print("cap for that window. Every figure is US dollars, and every window is UTC: the day starts at")
    print("midnight, the week on Monday, the month on the 1st.")
    print()
    print(describe(figures, {window: None for window in WINDOWS}, Path("-")).split("\n\n")[0])
    print()

    for window in WINDOWS:
        default = existing[window] if existing["exists"] else offered[window]
        default_text = money(default) if default is not None else "none"
        for attempt in range(attempts):
            answer = ask(f"  {window:<8} cap?", default_text)
            try:
                cap = parse_amount(answer) if answer else default
            except ValueError as error:
                print(f"           {error}")
                continue
            reason = refuse_reason(window, cap, figures)
            if reason:
                print(f"           {reason}")
                continue
            caps[window] = cap
            break
        else:
            raise ValueError(f"no usable {window} cap after {attempts} attempts.")

    if all(caps[window] is None for window in WINDOWS):
        print()
        print("  Every window is `none`, so nothing will watch what this cluster spends and no guard will")
        print("  be created at all. That is a choice and not a mistake, and it is recorded so that you are")
        print("  not asked again.")
        answer = ask("  Type `yes` to confirm no caps at all:", "no")
        if answer.strip().lower() not in ("y", "yes"):
            raise ValueError("no caps were confirmed, so nothing was written. Run `make caps` again.")

    for attempt in range(attempts):
        answer = ask("  address to email when a cap is met?", existing["email"] or "none")
        try:
            caps["email"] = parse_email(answer) if answer else existing["email"]
            break
        except ValueError as error:
            print(f"           {error}")
    else:
        raise ValueError(f"no usable email address after {attempts} attempts.")

    return caps


def write(path: Path, caps: dict, interval: int) -> None:
    lines = [HEADER.format(today=date.today().isoformat(), interval=interval)]
    width = max(len(name) for name in list(CAP_VARIABLE.values()) + [EMAIL_VARIABLE])
    for window in WINDOWS:
        cap = caps.get(window)
        value = "null" if cap is None else f"{cap:g}"
        lines.append(f"{CAP_VARIABLE[window]:<{width}} = {value}")
    email = json.dumps(caps.get("email", "")).replace("${", "$${").replace("%{", "%%{")
    lines.append(f'{EMAIL_VARIABLE:<{width}} = {email}')
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, prefix="." + path.name, delete=False) as handle:
            temporary = Path(handle.name)
            handle.write("\n".join(lines) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main() -> int:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Ask what this cluster may spend, and write it where OpenTofu reads it.")
    parser.add_argument("--prompt", action="store_true", help="Ask for each cap.")
    parser.add_argument("--check", action="store_true",
                        help="Print what is configured and the arithmetic behind it, and change nothing.")
    parser.add_argument("--out", default=str(here / DEFAULT_OUT),
                        help=f"Where to write. Default {DEFAULT_OUT}.")
    for window in WINDOWS:
        parser.add_argument(f"--{window}", help=f"Set the {window} cap without asking. `none` for no cap.")
    parser.add_argument("--email", help="Set the address to email without asking. `none` for nobody.")
    parser.add_argument("--max-agent-nodes", type=int,
                        help="How many agent nodes this account allows, for the arithmetic. Defaults to"
                             " $EKS_MAX_AGENT_NODES, which `make quota` writes.")
    parser.add_argument("--price-per-vcpu-hour", type=float,
                        default=float(os.environ.get("EKS_SPEND_PRICE_PER_VCPU_HOUR",
                                                     DEFAULT_PRICE_PER_VCPU_HOUR)))
    parser.add_argument("--fixed-usd-per-day", type=float,
                        default=float(os.environ.get("EKS_SPEND_FIXED_USD_PER_DAY",
                                                     DEFAULT_FIXED_USD_PER_DAY)))
    parser.add_argument("--vcpus-per-node", type=int, default=DEFAULT_VCPUS_PER_NODE)
    parser.add_argument("--interval-minutes", type=int,
                        default=int(os.environ.get("EKS_SPEND_GUARD_INTERVAL_MINUTES", "5")),
                        help="Only for the comment written into the file.")
    args = parser.parse_args()

    out = Path(args.out)
    existing = read_existing(out)

    nodes = args.max_agent_nodes
    if nodes is None and os.environ.get("EKS_MAX_AGENT_NODES"):
        nodes = int(os.environ["EKS_MAX_AGENT_NODES"])
    figures = arithmetic(args.price_per_vcpu_hour, args.fixed_usd_per_day, args.vcpus_per_node, nodes)

    if args.check:
        if not existing["exists"]:
            print(f"No spend caps are set: {out} does not exist.")
            print("Nothing watches what this cluster spends until `make caps` writes it.")
            return 1
        if not existing.get("valid"):
            print(f"Incomplete or invalid caps file: {out}. Run make caps to repair it.", file=sys.stderr)
            return 1
        print(f"Spend caps for this cluster, from {out}:")
        print()
        print(describe(figures, existing, out))
        if existing["email"]:
            print(f"  A cap being met emails {existing['email']}.")
        else:
            print("  No address is set, so a cap being met is published to SNS and to CloudWatch only.")
        return 0

    named = {window: getattr(args, window) for window in WINDOWS}
    if any(value is not None for value in named.values()) or args.email is not None:
        caps = dict(existing)
        try:
            for window, value in named.items():
                if value is not None:
                    caps[window] = parse_amount(value)
            if args.email is not None:
                caps["email"] = parse_email(args.email)
        except ValueError as error:
            print(f"{error}", file=sys.stderr)
            return 2
        for window in WINDOWS:
            reason = refuse_reason(window, caps[window], figures)
            if reason:
                print(f"The {window} cap was refused: {reason}", file=sys.stderr)
                return 2
    elif args.prompt:
        try:
            caps = prompt(figures, existing)
        except (EOFError, ValueError) as error:
            print(f"{error}", file=sys.stderr)
            return 2
        except KeyboardInterrupt:
            print("\nNothing was written.", file=sys.stderr)
            return 2
    else:
        parser.print_help()
        return 2

    try:
        write(out, caps, args.interval_minutes)
    except OSError as error:
        print(f"Could not save caps to {out}: {error}", file=sys.stderr)
        return 2
    print()
    print(describe(figures, caps, out))
    print("  `make apply` puts these where the guard reads them, and `make spend` prints what has been")
    print("  spent against them.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
