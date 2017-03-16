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

import org.apache.cassandra.diag.DiagnosticEvent;
import org.apache.cassandra.diag.DiagnosticEventService;

/**
 * DiagnosticEvent implementation for HintService.
 */
public class HintsServiceEvent extends DiagnosticEvent
{
    public enum HintsServiceEventType
    {
        DISPATCHING_STARTED,
        DISPATCHING_PAUSED,
        DISPATCHING_RESUMED,
        DISPATCHING_SHUTDOWN
    }

    public HintsServiceEventType type;

    public HintsService service;
    public boolean isDispatchPaused;
    public boolean isShutdown;
    public boolean dispatchExecutorIsPaused;
    public boolean dispatchExecutorHasScheduledDispatches;

    // TODO: add metrics

    private HintsServiceEvent(HintsServiceEventType type, HintsService service)
    {
        this.type = type;
        this.service = service;
        this.isDispatchPaused = service.isDispatchPaused.get();
        this.isShutdown = service.isShutDown();
        this.dispatchExecutorIsPaused = service.dispatchExecutor.isPaused();
        this.dispatchExecutorHasScheduledDispatches = service.dispatchExecutor.hasScheduledDispatches();
    }

    static void dispatchingStarted(HintsService service)
    {
        if (isEnabled(HintsServiceEventType.DISPATCHING_STARTED))
            DiagnosticEventService.publish(new HintsServiceEvent(HintsServiceEventType.DISPATCHING_STARTED, service));
    }

    static void dispatchingShutdown(HintsService service)
    {
        if (isEnabled(HintsServiceEventType.DISPATCHING_SHUTDOWN))
            DiagnosticEventService.publish(new HintsServiceEvent(HintsServiceEventType.DISPATCHING_SHUTDOWN, service));
    }

    static void dispatchingPaused(HintsService service)
    {
        if (isEnabled(HintsServiceEventType.DISPATCHING_PAUSED))
            DiagnosticEventService.publish(new HintsServiceEvent(HintsServiceEventType.DISPATCHING_PAUSED, service));
    }

    static void dispatchingResumed(HintsService service)
    {
        if (isEnabled(HintsServiceEventType.DISPATCHING_RESUMED))
            DiagnosticEventService.publish(new HintsServiceEvent(HintsServiceEventType.DISPATCHING_RESUMED, service));
    }

    private static boolean isEnabled(HintsServiceEventType type)
    {
        return DiagnosticEventService.isEnabled(HintsServiceEvent.class, type);
    }

    @Override
    public Enum<HintsServiceEventType> getType()
    {
        return type;
    }

    public Object getSource()
    {
        return service;
    }

    @Override
    public HashMap<String, Serializable> toMap()
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        ret.put("isDispatchPaused", isDispatchPaused);
        ret.put("isShutdown", isShutdown);
        ret.put("dispatchExecutorIsPaused", dispatchExecutorIsPaused);
        ret.put("dispatchExecutorHasScheduledDispatches", dispatchExecutorHasScheduledDispatches);
        return ret;
    }
}
