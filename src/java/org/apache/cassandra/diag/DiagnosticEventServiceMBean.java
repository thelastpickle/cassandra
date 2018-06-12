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

import java.io.Serializable;
import java.util.Map;
import java.util.SortedMap;

/**
 * Provides JMX enabled attributes and operations implemented by {@link DiagnosticEventService}.
 */
public interface DiagnosticEventServiceMBean
{
    /*
     * Indicates if any events will be published.
     */
    boolean isEnabled();

    /**
     * Disables all events.
     */
    void stopPublishing();

    /**
     * Enables event publishing.
     */
    void resumePublishing();

    /**
     * Retrieved all events of specified type starting with provided key. Result will be sorted chronologically.
     *
     * @param eventClazz fqn of event class
     * @param key ID of first event to retrieve
     * @param limit number of results to return
     * @param includeKey should specified key be returned in result
     */
    SortedMap<Long, Map<String, Serializable>> getEvents(String eventClazz, Long key, int limit, boolean includeKey);

    /**
     * Start storing events to make them available via {@link #getEvents(String, Long, int, boolean)}.
     */
    void enableEventPersistence(String eventClazz);

    /**
     * Stop storing events.
     */
    void disableEventPersistence(String eventClazz);
}
