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

package org.apache.cassandra.service;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.diag.DiagnosticEvent;
import org.apache.cassandra.diag.DiagnosticEventService;

/**
 * Events related to {@link PendingRangeCalculatorService}.
 */
public class PendingRangeCalculatorServiceEvent extends DiagnosticEvent
{
    private final PendingRangeCalculatorServiceEventType type;
    private final PendingRangeCalculatorService source;
    private final int taskCount;

    public enum PendingRangeCalculatorServiceEventType
    {
        TASK_STARTED,
        TASK_FINISHED_SUCCESSFULLY,
        TASK_EXECUTION_REJECTED,
        TASK_COUNT_CHANGED
    }

    private PendingRangeCalculatorServiceEvent(PendingRangeCalculatorServiceEventType type,
                                               PendingRangeCalculatorService service,
                                               int taskCount)
    {
        this.type = type;
        this.source = service;
        this.taskCount = taskCount;
    }

    static void taskStarted(PendingRangeCalculatorService service,
                                   AtomicInteger taskCount)
    {
        if (DiagnosticEventService.instance().isEnabled(PendingRangeCalculatorServiceEvent.class,
                                             PendingRangeCalculatorServiceEventType.TASK_STARTED))
            DiagnosticEventService.instance().publish(new PendingRangeCalculatorServiceEvent(PendingRangeCalculatorServiceEventType.TASK_STARTED,
                                                                                  service,
                                                                                  taskCount.get()));
    }

    static void taskFinished(PendingRangeCalculatorService service,
                                    AtomicInteger taskCount)
    {
        if (DiagnosticEventService.instance().isEnabled(PendingRangeCalculatorServiceEvent.class,
                                             PendingRangeCalculatorServiceEventType.TASK_FINISHED_SUCCESSFULLY))
            DiagnosticEventService.instance().publish(new PendingRangeCalculatorServiceEvent(PendingRangeCalculatorServiceEventType.TASK_FINISHED_SUCCESSFULLY,
                                                                                  service,
                                                                                  taskCount.get()));
    }

    static void taskRejected(PendingRangeCalculatorService service,
                                    AtomicInteger taskCount)
    {
        if (DiagnosticEventService.instance().isEnabled(PendingRangeCalculatorServiceEvent.class,
                                             PendingRangeCalculatorServiceEventType.TASK_EXECUTION_REJECTED))
            DiagnosticEventService.instance().publish(new PendingRangeCalculatorServiceEvent(PendingRangeCalculatorServiceEventType.TASK_EXECUTION_REJECTED,
                                                                                  service,
                                                                                  taskCount.get()));
    }

    static void taskCountChanged(PendingRangeCalculatorService service,
                                    int taskCount)
    {
        if (DiagnosticEventService.instance().isEnabled(PendingRangeCalculatorServiceEvent.class,
                                             PendingRangeCalculatorServiceEventType.TASK_COUNT_CHANGED))
            DiagnosticEventService.instance().publish(new PendingRangeCalculatorServiceEvent(PendingRangeCalculatorServiceEventType.TASK_COUNT_CHANGED,
                                                                                  service,
                                                                                  taskCount));
    }

    public int getTaskCount()
    {
        return taskCount;
    }

    @Override
    public Enum<?> getType()
    {
        return type;
    }

    @Override
    public Object getSource()
    {
        return source;
    }

    @Override
    public HashMap<String, Serializable> toMap()
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        ret.put("taskCount", taskCount);
        return ret;
    }
}
