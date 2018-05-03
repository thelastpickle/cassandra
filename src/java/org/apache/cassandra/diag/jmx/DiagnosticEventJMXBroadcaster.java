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

package org.apache.cassandra.diag.jmx;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanServer;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.diag.DiagnosticEvent;
import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.utils.progress.jmx.JMXBroadcastExecutor;

/**
 * Allows to subscribe to diagnostic events, while acting as JMX event broadcaster.
 *
 * @see JMXEventSupport
 */
public class DiagnosticEventJMXBroadcaster extends NotificationBroadcasterSupport implements DiagnosticEventJMXBroadcasterMBean
{
    private static final Logger logger = LoggerFactory.getLogger(DiagnosticEventJMXBroadcaster.class);

    private static DiagnosticEventJMXBroadcaster instance;

    private final JMXEventSupport eventSupport = new JMXEventSupport(this);

    private AtomicInteger subscribers = new AtomicInteger(0);

    private DiagnosticEventJMXBroadcaster()
    {
        // use dedicated executor for handling JMX notifications
        super(JMXBroadcastExecutor.executor);

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            ObjectName jmxObjectName = new ObjectName("org.apache.cassandra.diag:type=DiagnosticEvents");
            mbs.registerMBean(this, jmxObjectName);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void start()
    {
        instance = new DiagnosticEventJMXBroadcaster();
    }

    public static DiagnosticEventJMXBroadcaster instance()
    {
        return instance;
    }

    @Override
    public void enableEvents(String eventClazz)
    {
        enableEvents(eventClazz, null, true);
    }

    @Override
    public void enableEvents(String eventClazz, String eventType)
    {
        enableEvents(eventClazz, eventType, true);
    }

    @Override
    public void disableEvents(String eventClazz)
    {
        enableEvents(eventClazz, null, false);
    }

    private void enableEvents(String eventClazz, String eventType, boolean enabled)
    {
        // get class by eventClazz argument name
        Class<DiagnosticEvent> clazz;
        try
        {
            // restrict class loading for security reasons
            if (!eventClazz.startsWith("org.apache.cassandra."))
                throw new RuntimeException("Not a Cassandra event class");

            clazz = (Class<DiagnosticEvent>) Class.forName(eventClazz);
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }

        if (!(DiagnosticEvent.class.isAssignableFrom(clazz)))
            throw new RuntimeException("Event class must be of type DiagnosticEvent");

        // unsubscribe and exit if disabled
        if (!enabled)
        {
            DiagnosticEventService.instance().unsubscribe(eventSupport);
            return;
        }

        // subscribe and exit in case of missing eventType
        if (eventType == null)
        {
            DiagnosticEventService.instance().subscribe(clazz, eventSupport);
            return;
        }

        // determine event type by eventType argument
        // XXX: must be public inner enum of event class - is this fine, or should type be a class or string?
        boolean subscribed = false;
        for (Class inner : clazz.getClasses())
        {
            if (!inner.isEnum()) continue;

            try
            {
                Enum en = Enum.valueOf(inner, eventType);
                DiagnosticEventService.instance().subscribe(clazz, en, eventSupport);
                subscribed = true;
                break;
            }
            catch (IllegalArgumentException e)
            {
                // thrown by Enum.valueOf
            }
        }
        if (!subscribed)
            throw new RuntimeException("Invalid eventType: " + eventType);
    }

    @Override
    public void addNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback)
    {
        super.addNotificationListener(listener, filter, handback);
        logger.debug("Listener added");
        subscribers.incrementAndGet();
    }

    @Override
    public void removeNotificationListener(NotificationListener listener) throws ListenerNotFoundException
    {
        super.removeNotificationListener(listener);
        logger.debug("Listener removed");
        subscribers.decrementAndGet();
    }

    @Override
    public void removeNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback) throws ListenerNotFoundException
    {
        super.removeNotificationListener(listener, filter, handback);
        logger.debug("Listener removed");
        subscribers.decrementAndGet();

    }
}
