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

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;

import org.apache.cassandra.diag.DiagnosticEvent;

/**
 * ProgressListener that translates {@link org.apache.cassandra.diag.DiagnosticEvent} to JMX Notification message.
 */
public class JMXEventSupport implements Consumer<DiagnosticEvent>
{
    private final AtomicLong notificationSerialNumber = new AtomicLong();

    private final NotificationBroadcasterSupport broadcaster;

    public JMXEventSupport(NotificationBroadcasterSupport broadcaster)
    {
        this.broadcaster = broadcaster;
    }

    @Override
    public void accept(DiagnosticEvent event)
    {
        String source = "n/a";
        if (event.getSource() != null)
            source = event.getSource().getClass().getSimpleName();

        Notification notification = new Notification("diag_event",
                                                     source,
                                                     notificationSerialNumber.getAndIncrement(),
                                                     event.timestamp,
                                                     event.toString());
        Map<String, Serializable> payload = event.toMap();
        payload.put("class", event.getClass().getSimpleName());
        payload.put("type", event.getType().name());
        notification.setUserData(payload);
        broadcaster.sendNotification(notification);
    }
}
