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

import java.util.Map;
import java.util.function.Consumer;

/**
 * Provides a map of event types and corresponding greatest event IDs. Returned data will be merged
 * by {@link LastEventIdBroadcaster} with data returned by collectors. Event IDs can be any kind of
 * comparable classes, e.g. {@link Long} or {@link java.util.UUID}, but it's highly recommended that
 * these also implement {@link java.io.Serializable}.
 */
public interface LastEventIdCollector<K, V extends Comparable<V>>
{
    /**
     * Return all current events and event IDs
     */
    Map<K, V> lastEventIds();

    /**
     * Add consumer that must be called with all current events and event IDs upon updates.
     */
    void addEventIdsUpdateListener(Consumer<Map<K, V>> listener);

    /**
     * Removed added consumer.
     */
    void removeEventIdsUpdateListener(Consumer<Map<K, V>> listener);
}
