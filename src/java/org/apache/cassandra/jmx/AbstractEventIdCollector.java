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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;


/**
 * Helper implementation for creating sub classes of {@link LastEventIdCollector}.
 * @param <E> Event type
 * @param <K> event ID map key type
 * @param <V> event ID value type
 */
public abstract class AbstractEventIdCollector<E, K, V extends Comparable<V>> implements LastEventIdCollector<K, V>
{
    private final Map<K, V> lastEventIds = new ConcurrentHashMap<>();

    private final Set<Consumer<Map<K, V>>> listeners = new HashSet<>();

    public void onEvent(E event)
    {
        K key = eventType(event);
        V id = eventId(event);
        if (lastEventIds.compute(key, (k, v) -> v == null || id.compareTo(v) > 0 ? id : v) != null)
            listeners.forEach((l) -> l.accept(lastEventIds));
    }

    @Override
    public Map<K, V> lastEventIds()
    {
        return lastEventIds;
    }

    @Override
    public void addEventIdsUpdateListener(Consumer<Map<K, V>> listener)
    {
        listeners.add(listener);
    }

    @Override
    public void removeEventIdsUpdateListener(Consumer<Map<K, V>> listener)
    {
        listeners.remove(listener);
    }

    /**
     * Returns the event ID value for the provided event instance.
     */
    public abstract V eventId(E event);

    /**
     * Returns the name of the event type, e.g. "AuditEvent".
     */
    public abstract K eventType(E event);
}
