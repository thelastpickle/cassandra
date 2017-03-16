/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.hints;

import java.io.Serializable;
import java.util.HashMap;
import java.util.UUID;

import org.apache.cassandra.diag.DiagnosticEvent;
import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * DiagnosticEvent implementation for hinted handoff.
 */
public class HintEvent extends DiagnosticEvent
{
    public enum HintEventType
    {
        DISPATCHING_STARTED,
        DISPATCHING_PAUSED,
        DISPATCHING_RESUMED,
        DISPATCHING_SHUTDOWN,

        DISPATCHER_CREATED,
        DISPATCHER_CLOSED,

        DISPATCHER_PAGE,
        DISPATCHER_HINT_RESULT,

        ABORT_REQUESTED
    }

    public enum HintResult
    {
        PAGE_SUCCESS, PAGE_FAILURE
    }

    public HintEventType type;
    HintsDispatcher dispatcher;
    public UUID targetHostId;
    public InetAddressAndPort targetAddress;
    public Boolean encoded;
    public Hint hint;
    public HintResult dispatchResult;
    public InputPosition position;
    public long pageHintsSuccessful;
    public long pageHintsFailed;
    public long pageHintsTimeout;

    private HintEvent(HintEventType type, HintsDispatcher dispatcher, UUID targetHostId,
                      InetAddressAndPort targetAddress, Boolean encoded, Hint hint, HintResult dispatchResult,
                      InputPosition position, long pageHintsSuccessful, long pageHintsFailed, long pageHintsTimeout)
    {
        this.type = type;
        this.dispatcher = dispatcher;
        this.targetHostId = targetHostId;
        this.targetAddress = targetAddress;
        this.encoded = encoded;
        this.hint = hint;
        this.dispatchResult = dispatchResult;
        this.position = position;
        this.pageHintsSuccessful = pageHintsSuccessful;
        this.pageHintsFailed = pageHintsFailed;
        this.pageHintsTimeout = pageHintsTimeout;
    }

    public static void dispatcherCreated(HintsDispatcher dispatcher)
    {
        if (isEnabled(HintEventType.DISPATCHER_CREATED))
            DiagnosticEventService.publish(new HintEvent(HintEventType.DISPATCHER_CREATED, dispatcher,
                    dispatcher.hostId, dispatcher.address,
                    null, null, null, null,
        0, 0, 0));
    }

    public static void dispatcherClosed(HintsDispatcher dispatcher)
    {
        if (isEnabled(HintEventType.DISPATCHER_CLOSED))
            DiagnosticEventService.publish(new HintEvent(HintEventType.DISPATCHER_CLOSED, dispatcher,
                    dispatcher.hostId, dispatcher.address,
                    null, null, null, null,
        0, 0, 0));
    }

    public static void dispatchPage(HintsDispatcher dispatcher, InputPosition pagePosition)
    {
        if (isEnabled(HintEventType.DISPATCHER_PAGE))
            DiagnosticEventService.publish(new HintEvent(HintEventType.DISPATCHER_PAGE, dispatcher,
                    dispatcher.hostId, dispatcher.address,
                    null, null, null, pagePosition,
        0, 0, 0));
    }

    public static void abortRequested(HintsDispatcher dispatcher)
    {
        if (isEnabled(HintEventType.ABORT_REQUESTED))
            DiagnosticEventService.publish(new HintEvent(HintEventType.ABORT_REQUESTED, dispatcher,
                    dispatcher.hostId, dispatcher.address,
                    null, null, null, null,
        0, 0, 0));
    }

    public static void pageSuccessResult(HintsDispatcher dispatcher, long success, long failures, long timeouts)
    {
        if (isEnabled(HintEventType.DISPATCHER_HINT_RESULT))
            DiagnosticEventService.publish(new HintEvent(HintEventType.DISPATCHER_HINT_RESULT, dispatcher,
                    dispatcher.hostId, dispatcher.address,
                    null, null, HintResult.PAGE_SUCCESS, null,
                              success, failures, timeouts));
    }

    public static void pageFailureResult(HintsDispatcher dispatcher, long success, long failures, long timeouts)
    {
        if (isEnabled(HintEventType.DISPATCHER_HINT_RESULT))
            DiagnosticEventService.publish(new HintEvent(HintEventType.DISPATCHER_HINT_RESULT, dispatcher,
                    dispatcher.hostId, dispatcher.address,
                    null, null, HintResult.PAGE_FAILURE, null,
                              success, failures, timeouts));
    }

    private static boolean isEnabled(HintEventType type)
    {
        return DiagnosticEventService.isEnabled(HintEvent.class, type);
    }

    @Override
    public Enum<HintEventType> getType()
    {
        return type;
    }

    @Override
    public Object getSource()
    {
        return dispatcher;
    }

    @Override
    public HashMap<String, Serializable> toMap()
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        ret.put("targetHostId", targetHostId);
        ret.put("targetAddress", targetAddress.getHostAddress(true));
        ret.put("encoded", encoded);
        if (hint != null)
        {
            ret.put("hint.creationTime", hint.creationTime);
            ret.put("hint.mutation.keyspace", hint.mutation.getKeyspaceName());
            ret.put("hint.mutation.isLive", hint.isLive());
            ret.put("hint.mutation.timeout", hint.mutation.getTimeout());
        }
        if (pageHintsSuccessful > 0 || pageHintsFailed > 0 || pageHintsTimeout > 0)
        {
            ret.put("hint.page.hints_succeeded", pageHintsSuccessful);
            ret.put("hint.page.hints_failed", pageHintsFailed);
            ret.put("hint.page.hints_timed_out", pageHintsTimeout);
        }
        return ret;
    }
}