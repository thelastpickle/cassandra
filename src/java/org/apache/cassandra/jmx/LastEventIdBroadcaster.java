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

package org.apache.cassandra.jmx;

import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.management.MBeanServer;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.utils.progress.jmx.JMXBroadcastExecutor;

/**
 * Broadcaster for notifying JMX clients on newly available data. Periodically sends {@link Notification}s
 * containing a list of event types and greatest event IDs. Consumers may use this information to
 * query or poll events based on this data.
 */
public class LastEventIdBroadcaster extends NotificationBroadcasterSupport implements LastEventIdBroadcasterMBean
{

    private final static LastEventIdBroadcaster instance = new LastEventIdBroadcaster();

    private final static int PERIODIC_BROADCAST_INTERVAL_MILLIS = 30000;
    private final static int SHORT_TERM_BROADCAST_DELAY_MILLIS = 1000;

    private final Collection<LastEventIdCollector<String, ?>> collectors = new HashSet<>();
    private final AtomicLong notificationSerialNumber = new AtomicLong();
    private final AtomicReference<ScheduledFuture<?>> scheduledPeriodicalBroadcast = new AtomicReference<>();
    private final AtomicReference<ScheduledFuture<?>> scheduledShortTermBroadcast = new AtomicReference<>();

    private Map<String, Comparable> lastSummary = Collections.emptyMap();


    private LastEventIdBroadcaster()
    {
        // use dedicated executor for handling JMX notifications
        super(JMXBroadcastExecutor.executor);

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            ObjectName jmxObjectName = new ObjectName("org.apache.cassandra.jmx:type=LastEventIdBroadcaster");
            mbs.registerMBean(this, jmxObjectName);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static LastEventIdBroadcaster instance()
    {
        return instance;
    }

    @Override
    public Map<String, Comparable> getLastEventIds()
    {
        Map<String, Comparable> summary = new HashMap<>(collectors.stream()
                                                  .flatMap((s) -> s.lastEventIds().entrySet().stream())
                                                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        summary.put("last_updated_at", System.currentTimeMillis());
        lastSummary = Collections.unmodifiableMap(summary);
        return lastSummary;
    }

    @Override
    public Map<String, Comparable> getLastEventIdsIfModified(long lastUpdate)
    {
        if (lastUpdate >= (long)lastSummary.get("last_updated_at")) return lastSummary;
        else return getLastEventIds();
    }

    @Override
    public synchronized void addNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback)
    {
        super.addNotificationListener(listener, filter, handback);

        // lazily schedule periodical broadcast once we got our first subscriber
        if (scheduledPeriodicalBroadcast.get() == null)
        {
            ScheduledFuture<?> scheduledFuture = ScheduledExecutors.scheduledTasks
                                                 .scheduleAtFixedRate(this::broadcastEventIds,
                                                                      PERIODIC_BROADCAST_INTERVAL_MILLIS,
                                                                      PERIODIC_BROADCAST_INTERVAL_MILLIS,
                                                                      TimeUnit.MILLISECONDS);
            if (!this.scheduledPeriodicalBroadcast.compareAndSet(null, scheduledFuture))
                scheduledFuture.cancel(false);
        }
    }

    public void addCollector(LastEventIdCollector<String, ?> collector)
    {
        collectors.add(collector);
        collector.addEventIdsUpdateListener(this::onLastIdsChanged);
    }

    private void onLastIdsChanged(Map<String, ? extends Comparable> updatedLastIds)
    {
        HashMap<String, Comparable> map = new HashMap<>(lastSummary);
        map.putAll(updatedLastIds);
        map.put("last_updated_at", System.currentTimeMillis());
        lastSummary = Collections.unmodifiableMap(map);

        // schedule broadcast for timely announcing new events before next periodical broadcast
        // this should allow us to buffer new updates for a while, while keeping broadcasts near-time
        ScheduledFuture<?> running = scheduledShortTermBroadcast.get();
        if (running == null || running.isDone())
        {
            ScheduledFuture<?> scheduledFuture = ScheduledExecutors.scheduledTasks
                                                 .schedule((Runnable)this::broadcastEventIds,
                                                           SHORT_TERM_BROADCAST_DELAY_MILLIS,
                                                           TimeUnit.MILLISECONDS);
            if (!this.scheduledShortTermBroadcast.compareAndSet(running, scheduledFuture))
                scheduledFuture.cancel(false);
        }
    }

    private void broadcastEventIds()
    {
        if (!lastSummary.isEmpty())
            broadcastEventIds(lastSummary);
    }

    private void broadcastEventIds(Map<String, Comparable> summary)
    {
        Notification notification = new Notification("event_last_id_summary",
                                                     "LastEventIdBroadcaster",
                                                     notificationSerialNumber.incrementAndGet(),
                                                     System.currentTimeMillis(),
                                                     "Event last IDs summary");
        notification.setUserData(summary);
        sendNotification(notification);
    }
}
