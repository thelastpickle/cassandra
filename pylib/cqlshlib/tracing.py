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

from datetime import datetime
import time

from cassandra.query import QueryTrace, TraceUnavailable
from cqlshlib.displaying import colorme, MAGENTA


def print_trace_session(shell, tracing_mode, session, session_id, partial_session=False):
    """
    Lookup a trace by session and trace session ID, then print it.
    """
    trace = QueryTrace(session_id, session)
    try:
        wait_for_complete = not partial_session
        trace.populate(wait_for_complete=wait_for_complete)
    except TraceUnavailable:
        shell.printerr("Session %s wasn't found." % session_id)
    else:
        print_trace(shell, tracing_mode, trace)


def print_trace(shell, tracing_mode, trace):
    assert tracing_mode in {'full', 'compact'}
    trace_fn = print_trace_full if tracing_mode == 'full' else print_trace_compact
    trace_fn(shell, trace)


def format_trace(activity):
    """
    Format a trace activity, without the control-character mangling we use for other strings.
    (Trace activity can contain newlines.)
    """
    return colorme(activity, None, 'text')


def print_trace_full(shell, trace):
    """
    Print a populated cassandra.query.QueryTrace instance.
    """
    rows = make_trace_rows(trace, 'full')
    if not rows:
        shell.printerr("No rows for session %s found." % (trace.trace_id,))
        return
    names = ['activity', 'timestamp', 'source', 'source_elapsed', 'client']

    formatted_names = list(map(shell.myformat_colname, names))
    formatted_values = [[format_trace(row[0])] + list(map(shell.myformat_value, row[1:])) for row in rows]

    shell.writeresult('')
    shell.writeresult('Tracing session: ', color=MAGENTA, newline=False)
    shell.writeresult(trace.trace_id)
    shell.writeresult('')
    shell.print_formatted_result(formatted_names, formatted_values, True, shell.tty)
    shell.writeresult('')


def print_trace_compact(shell, trace):
    """
    Print a populated cassandra.query.QueryTrace instance.
    """
    rows = make_trace_rows(trace, 'compact')
    if not rows:
        shell.printerr("No rows for session %s found." % (trace.trace_id,))
        return

    # Transform the full trace into 3 compact columns: src, elapsed, activity
    # The original source column is a string version of an ipv4 address, e.g. 192.168.1.121;
    # We want to generate a "key" mapping those addresses to "A", "B", "C", etc.
    unique_sources = sorted(set(row[2] for row in rows))
    if len(unique_sources) > 26:
        shell.writeresult('Too many sources for compact mode; showing full instead')
        print_trace_full(shell, trace)
        return
    source_mapping = {src: chr(65 + idx) for idx, src in enumerate(unique_sources)}

    names = ['src', 'elapsed', 'activity']
    formatted_names = list(map(shell.myformat_colname, names))
    formatted_values = [[shell.myformat_value(source_mapping[row[2]]),
                         shell.myformat_value(row[3]),
                         format_trace(row[0])] for row in rows]

    shell.writeresult('')
    shell.writeresult('Tracing session: ', color=MAGENTA, newline=False)
    shell.writeresult(trace.trace_id)
    shell.writeresult('')

    # Write mapping of sources to keys
    shell.writeresult('Source mapping:', color=MAGENTA)
    for src, key in source_mapping.items():
        shell.writeresult(f"  {key} -> {src}")
    shell.writeresult('')

    shell.print_formatted_result(formatted_names, formatted_values, True, shell.tty, justify_last=False)
    shell.writeresult('')


def make_trace_rows(trace, tracing_mode):
    if not trace.events:
        return []

    rows = [[trace.request_type, str(datetime_from_utc_to_local(trace.started_at)), trace.coordinator, 0, trace.client]]

    # append main rows (from events table).
    for event in trace.events:
        rows.append(["%s [%s]" % (event.description, event.thread_name) if tracing_mode == 'full' else event.description,
                     str(datetime_from_utc_to_local(event.datetime)),
                     event.source,
                     total_micro_seconds(event.source_elapsed),
                     trace.client])
    # append footer row (from sessions table).
    if trace.duration:
        finished_at = (datetime_from_utc_to_local(trace.started_at) + trace.duration)
        rows.append(['Request complete', str(finished_at), trace.coordinator, total_micro_seconds(trace.duration), trace.client])

    return rows


def total_micro_seconds(td):
    """
    Convert a timedelta into total microseconds
    """
    return int((td.microseconds + (td.seconds + td.days * 24 * 3600) * 10 ** 6)) if td else "--"


def datetime_from_utc_to_local(utc_datetime):
    now_timestamp = time.time()
    offset = datetime.fromtimestamp(now_timestamp) - datetime.utcfromtimestamp(now_timestamp)
    return utc_datetime + offset
