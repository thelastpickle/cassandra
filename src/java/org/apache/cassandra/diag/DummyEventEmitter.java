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

package org.apache.cassandra.diag;

import java.lang.management.ManagementFactory;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.concurrent.ScheduledExecutors;

/**
 * Event emitter that can be used for (manual) testing.
 */
public class DummyEventEmitter implements DummyEventEmitterMBean
{
    private static final DummyEventEmitter instance = new DummyEventEmitter();

    private ScheduledFuture<?> scheduledFuture;

    private DummyEventEmitter()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            ObjectName jmxObjectName = new ObjectName("org.apache.cassandra.diag:type=DummyEventEmitter");
            mbs.registerMBean(this, jmxObjectName);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static DummyEventEmitter instance()
    {
        return instance;
    }

    @Override
    public void dummyEventEmitIntervalMillis(int intervalMillis)
    {
        stop();
        scheduledFuture = ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(
            () -> DiagnosticEventService.instance().publish(DummyEvent.create()),
            intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop()
    {
        if (scheduledFuture != null && !scheduledFuture.isDone())
            scheduledFuture.cancel(false);
    }
}
