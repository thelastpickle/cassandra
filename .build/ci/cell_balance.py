#!/usr/bin/env python3
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

"""How evenly each test target's splits divided, and which of them came near their deadline.

Every test cell runs under `timeout(time: command.timeout_hours, ...)` in ../../.jenkins/Jenkinsfile.  A cell
that reaches it is retried once and then fails the build with `Retryable interruption: Timeout has been
exceeded`, and nothing said which cell it was or how close the others came.

What makes a split long: `_split_tests` in ../run-tests.sh deals an alphabetically sorted class list
round-robin with `split -n r/K/N`, which balances the count of classes and knows nothing of their duration.
Measured on build cassandra-eks-k8s #11, `test-burn` split 3 of 4 ran 46.6 minutes against that target's
median of 19.9, `DriverBurnTest` at 20.7 and `BigVolumeDifferentialCompactionTest` at 22.0 having landed in
the same chunk.  Three targets had a worst cell over twice their median.

Round-robin is kept deliberately, so that which split holds a test is predictable from the sorted list alone
rather than from a timing file that moves under it.  The lever is therefore the split count.

This reports and does not prescribe: one build cannot tell a structurally long split from one that drew a
slow agent, so change a split count only when consecutive builds agree.

Usage:

    cell_balance.py [--input build/test] [--output build/ci_summary.html] [--budget-margin 15]

`--input` holds `cell-times/*.tsv`, one per cell from `recordCellTime` in the Jenkinsfile, and
`output/<target>/` of decompressed JUnit XML.  With no records it says so and exits 0: a build whose cells all
timed out has nothing to measure, and this is not the check to fail it.

The table names the worst cell and the breakdown says what it ran, which together decide whether a split count
should move: one suite worth most of the cell cannot be helped by more splits, several of comparable size can.
Those suites come from the cell's own `.suites` file, written while `test/output/` still held only that cell's
results; one step later `organiseTestResultFiles` merges every cell's XML and the split label is gone from
every path.  Without that file the target-wide list is printed instead, labelled as such.
"""

import argparse
import os
import re
import statistics
import sys
import xml.etree.ElementTree as ElementTree

# Columns of a cell-times record, in order.  test_seconds covers every class and not only the twelve .suites
# lists, so the setup figure is exact rather than a residue.  A seven-field record from before it still reads.
FIELDS = ("step", "arch", "jdk", "split", "splits", "timeout_hours", "duration_ms", "test_seconds")

# Enough of the worst cell's longest suites to show whether one class dominates or the whole split is heavy,
# which is the distinction that decides what to do.  More when over budget, that being the one to act on.
LONGEST_SUITES = 6
LONGEST_SUITES_OVER_BUDGET = 12

# Below this share of a cell's duration, the recorded test time makes the setup figure an upper bound rather
# than a measurement, and it says so.
#
# The case it exists for: simulator-dtest on build cassandra-eks-k8s #22 ran 58.7 minutes and recorded 3.9 of
# test time, so the setup line read 54.8 minutes at 93%, which is untrue: its long simulations never reach
# the JUnit XML.  The suite's declared time agrees with the sum of its testcase times, 2.6 seconds against
# 2.3, so the file is internally honest and the missing runs are simply absent.  Reporting the attribution
# as incomplete beats printing a figure a reader would act on.
#
# The floor keeps it quiet for cells with no setup worth reporting: fqltool-test runs 0.9 minutes, 0.02 of it
# its one test class.
UNDER_RECORDED_TEST_SHARE = 0.25
UNDER_RECORDED_MINIMUM_MINUTES = 5.0

# Name the test classes only when the worst cell's longest reaches this.  Below it no single test is why the
# cell is long, so the list answers nothing and a report holds twenty of them.  A split count can act only
# on a class large enough to move on its own.
NAME_CLASSES_ABOVE_MINUTES = 5.0


def read_records(cell_times_dir):
    """Every cell record, as a list of dicts.  A malformed file is skipped rather than fatal."""
    records = []
    if not os.path.isdir(cell_times_dir):
        return records
    for name in sorted(os.listdir(cell_times_dir)):
        path = os.path.join(cell_times_dir, name)
        try:
            with open(path, encoding="utf-8") as handle:
                fields = handle.readline().rstrip("\n").split("\t")
            if len(fields) not in (len(FIELDS) - 1, len(FIELDS)):
                continue
            record = dict(zip(FIELDS, fields))
            record["minutes"] = int(record["duration_ms"]) / 60000.0
            # None, not zero, when absent: zero reads as a cell that ran no tests, giving its whole duration
            # to setup.
            record["test_minutes"] = (float(record["test_seconds"]) / 60.0
                                      if record.get("test_seconds") else None)
            record["deadline_minutes"] = float(record["timeout_hours"]) * 60.0
            record["label"] = (f"{record['step']} jdk{record['jdk']}"
                              f" {record['split']}/{record['splits']}")
            # This cell's own longest suites, from recordCellTime.  Absent for an older build, and for a cell
            # that produced no xml.
            record["suites"] = read_suites(path[: -len(".tsv")] + ".suites")
            records.append(record)
        except (OSError, ValueError):
            continue
    return records


def read_suites(path):
    """(suite name, minutes) for one cell, longest first, or [] when the file is absent."""
    suites = []
    try:
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                seconds, _, name = line.rstrip("\n").partition("\t")
                if name:
                    suites.append((name, float(seconds) / 60.0))
    except (OSError, ValueError):
        return []
    return suites


def summarise(records, budget_margin):
    """One row per target, worst first."""
    by_step = {}
    for record in records:
        by_step.setdefault(record["step"], []).append(record)

    rows = []
    for step, cells in by_step.items():
        minutes = sorted(cell["minutes"] for cell in cells)
        worst = max(cells, key=lambda cell: cell["minutes"])
        median = statistics.median(minutes)
        deadline = max(cell["deadline_minutes"] for cell in cells)
        budget = deadline - budget_margin
        rows.append({
            "step": step,
            "cells": len(cells),
            "splits": worst["splits"],
            "min": minutes[0],
            "median": median,
            "max": minutes[-1],
            # Guarded, because a target whose median is zero is a target that ran nothing.
            "ratio": minutes[-1] / median if median > 0 else 0.0,
            "deadline": deadline,
            "budget": budget,
            "over_budget": minutes[-1] > budget,
            "worst_label": worst["label"],
            "worst_suites": worst.get("suites") or [],
            # The cell's own total where it recorded one, and the sum of what it listed otherwise.
            "worst_test_minutes": (worst["test_minutes"] if worst.get("test_minutes") is not None
                                   else sum(minutes for _, minutes in (worst.get("suites") or []))),
            "worst_test_minutes_exact": worst.get("test_minutes") is not None,
        })
    rows.sort(key=lambda row: -row["max"])
    return rows


def suite_times(target_output_dir):
    """(suite name, minutes) for every JUnit suite under one target, longest first."""
    suites = []
    for root, _, names in os.walk(target_output_dir):
        for name in names:
            if not name.endswith(".xml"):
                continue
            try:
                tree = ElementTree.parse(os.path.join(root, name))
            except (OSError, ElementTree.ParseError):
                continue
            # A file's root is <testsuite> from ant and <testsuites> from pytest, so both are searched.
            element = tree.getroot()
            found = [element] if element.tag == "testsuite" else element.findall(".//testsuite")
            for suite in found:
                try:
                    suites.append((suite.get("name") or "?", float(suite.get("time") or 0) / 60.0))
                except ValueError:
                    continue
    suites.sort(key=lambda pair: -pair[1])
    return suites


def print_report(rows, output_root, budget_margin):
    if not rows:
        print("No cell-times records were found, so there is no split balance to report.")
        return

    print()
    print("Split balance, by test target.  `worst` is one cell's whole duration, setup included, against a")
    print(f"`budget` of its deadline less {budget_margin} min.  Splitting balances the count of classes and"
          " not their")
    print("duration, so a high w/med is a split that drew slow ones.  Confirm against the previous build.")
    print()
    header = (f"{'target':34s} {'cells':>5s} {'min':>7s} {'median':>7s} {'worst':>7s}"
              f" {'w/med':>6s} {'budget':>7s}  {'':4s} worst cell")
    print(header)
    print("-" * len(header))
    for row in rows:
        flag = "OVER" if row["over_budget"] else ""
        print(f"{row['step']:34s} {row['cells']:5d} {row['min']:7.1f} {row['median']:7.1f}"
              f" {row['max']:7.1f} {row['ratio']:6.2f} {row['budget']:7.0f}  {flag:4s}"
              f" {row['worst_label']}")

    if not any(row["over_budget"] for row in rows):
        print()
        print("Every target's worst cell is inside its budget.")

    # Every target's worst cell, not only the ones over budget: the table names the cell and this says what it
    # ran, which is the pair a split-count decision needs.  The cell's own suites, from before the merge.
    print()
    print("What each worst cell ran.  One class near the cell's whole duration cannot be split further;")
    print("several of comparable size can be.  Classes are named only where the longest reaches"
          f" {NAME_CLASSES_ABOVE_MINUTES:.0f} min,")
    print("below which none of them is the reason.")
    for row in rows:
        suites = row["worst_suites"]
        flag = "  OVER BUDGET" if row["over_budget"] else ""
        print()
        print(f"{row['worst_label']}: {row['max']:.1f} min against a budget of {row['budget']:.0f}{flag}")
        if not suites:
            # No .suites file: an older build, or a cell that wrote no xml.  Labelled, because the target's
            # longest suites need not be in this cell.
            fallback = suite_times(os.path.join(output_root, row["step"]))[:LONGEST_SUITES]
            if not fallback:
                print("    no per-cell suite record, and no JUnit XML for this target either")
                continue
            print("    no per-cell suite record, so these are the longest across the whole target:")
            for name, minutes in fallback:
                print(f"    {minutes:7.1f} min  {name}")
            continue
        longest = max((minutes for _, minutes in suites), default=0.0)
        if longest < NAME_CLASSES_ABOVE_MINUTES:
            # No class is large enough to be the reason, so the whole test time is one line and the setup
            # line below is what says anything about a cell of this shape.
            print(f"    {row['worst_test_minutes']:7.1f} min"
                  f"  {100 * row['worst_test_minutes'] / row['max'] if row['max'] else 0:4.0f}%"
                  f"  every test class in this cell, the longest of them {longest:.1f} min")
        else:
            limit = LONGEST_SUITES_OVER_BUDGET if row["over_budget"] else LONGEST_SUITES
            shown = suites[:limit]
            for name, minutes in shown:
                share = 100 * minutes / row["max"] if row["max"] else 0
                print(f"    {minutes:7.1f} min  {share:4.0f}%  {name}")
            listed = sum(minutes for _, minutes in shown)
            other = row["worst_test_minutes"] - listed
            if other > 0.05:
                print(f"    {other:7.1f} min  {100 * other / row['max']:4.0f}%  every other test class in"
                      f" this cell")
        setup = row["max"] - row["worst_test_minutes"]
        if setup > 0:
            share = row["worst_test_minutes"] / row["max"] if row["max"] else 1.0
            under_recorded = (row["max"] >= UNDER_RECORDED_MINIMUM_MINUTES
                              and share < UNDER_RECORDED_TEST_SHARE)
            if under_recorded:
                print(f"    {setup:7.1f} min  {100 * setup / row['max']:4.0f}%  setup at most: only"
                      f" {row['worst_test_minutes']:.1f} min of tests was recorded,"
                      f" {100 * share:.0f}% of the cell,")
                print("                        so tests are missing from the JUnit XML and this is an upper bound")
            else:
                exact = "" if row["worst_test_minutes_exact"] else ", or a test class this cell did not record"
                print(f"    {setup:7.1f} min  {100 * setup / row['max']:4.0f}%  not running tests: the node,"
                      f" the image pulls, the compile and the virtualenv{exact}")


def html_report(rows, budget_margin):
    parts = ["<h2>Split balance</h2>"]
    if not rows:
        parts.append("<p>No cell-times records were found.</p>")
        return "".join(parts)
    parts.append(
        "<p><code>worst</code> is one cell's whole duration, setup included, which is what the cell deadline"
        f" governs; <code>budget</code> is that deadline less {budget_margin} minutes of margin."
        " Round-robin splitting balances the number of classes in a split and not their duration, so a high"
        " <code>worst/median</code> is a split that drew slow classes. Confirm against the previous build"
        " before changing a split count.</p>")
    parts.append("<table border='1' cellpadding='4'><tr><th>target</th><th>cells</th><th>min</th>"
                 "<th>median</th><th>worst</th><th>worst/median</th><th>budget</th><th>worst cell</th></tr>")
    for row in rows:
        style = " bgcolor='#ffdddd'" if row["over_budget"] else ""
        parts.append(
            f"<tr{style}><td>{row['step']}</td><td>{row['cells']}</td><td>{row['min']:.1f}</td>"
            f"<td>{row['median']:.1f}</td><td>{row['max']:.1f}</td><td>{row['ratio']:.2f}</td>"
            f"<td>{row['budget']:.0f}</td><td>{row['worst_label']}</td></tr>")
    parts.append("</table>")
    return "".join(parts)


def main():
    parser = argparse.ArgumentParser(description="Report how evenly each test target's splits divided.")
    parser.add_argument("--input", default="build/test",
                       help="directory holding cell-times/ and output/, default build/test")
    parser.add_argument("--output", default=None,
                       help="existing .html file to append the table to; omitted prints only")
    parser.add_argument("--budget-margin", type=int, default=15,
                       help="minutes of margin to keep under a cell's deadline, default 15")
    args = parser.parse_args()

    records = read_records(os.path.join(args.input, "cell-times"))
    rows = summarise(records, args.budget_margin)
    print_report(rows, os.path.join(args.input, "output"), args.budget_margin)

    if args.output and os.path.isfile(args.output):
        try:
            with open(args.output, "a", encoding="utf-8") as handle:
                handle.write(html_report(rows, args.budget_margin))
        except OSError as error:
            print(f"could not append to {args.output}: {error}", file=sys.stderr)

    # Always 0.  An over-budget split is a thing to plan, not a reason to fail a build that passed.
    return 0


if __name__ == "__main__":
    sys.exit(main())
