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

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service for publishing and consuming {@link DiagnosticEvent}s.
 */
public class DiagnosticEventService
{
    private static final Logger logger = LoggerFactory.getLogger(DiagnosticEventService.class);

    // Subscribers interested in consuming all kind of events
    private static ImmutableSet<Consumer<DiagnosticEvent>> subscribersAll = ImmutableSet.of();

    // Subscribers for particular event class, e.g. BootstrapEvent
    private static ImmutableSetMultimap<Class<? extends DiagnosticEvent>, Consumer<DiagnosticEvent>> subscribersByClass = ImmutableSetMultimap.of();

    // Subscribers for event class and type, e.g. BootstrapEvent#TOKENS_ALLOCATED
    private static ImmutableMap<Class, ImmutableSetMultimap<Enum<?>, Consumer<DiagnosticEvent>>> subscribersByClassAndType = ImmutableMap.of();

    /**
     * Makes provided event available to all subscribers.
     */
    public static void publish(DiagnosticEvent event)
    {
        logger.trace("Publishing: {}", event);

        // event class + type
        ImmutableMultimap<Enum<?>, Consumer<DiagnosticEvent>> consumersByType = subscribersByClassAndType.get(event.getClass());
        if (consumersByType != null && consumersByType.containsKey(event.getType()))
        {
            for (Consumer<DiagnosticEvent> consumer : consumersByType.get(event.getType()))
                consumer.accept(event);
        }

        // event class
        Set<Consumer<DiagnosticEvent>> consumersByEvents = subscribersByClass.get(event.getClass());
        if (consumersByEvents != null)
        {
            for (Consumer<DiagnosticEvent> consumer : consumersByEvents)
                consumer.accept(event);
        }

        // all events
        for (Consumer<DiagnosticEvent> consumer : subscribersAll)
            consumer.accept(event);
    }

    /**
     * Registers event handler for specified class of events.
     * @param event DiagnosticEvent class implementation
     * @param consumer Consumer for received events
     */
    public static synchronized <E extends DiagnosticEvent> void subscribe(Class<E> event, Consumer<E> consumer)
    {
        subscribersByClass = ImmutableSetMultimap.<Class<? extends DiagnosticEvent>, Consumer<DiagnosticEvent>>builder()
                              .putAll(subscribersByClass)
                              .put(event, new TypedConsumerWrapper<>(consumer))
                              .build();
    }

    /**
     * Registers event handler for specified class of events.
     * @param event DiagnosticEvent class implementation
     * @param consumer Consumer for received events
     */
    public static synchronized <E extends DiagnosticEvent, T extends Enum<T>> void subscribe(Class<E> event,
                                                                                             T eventType,
                                                                                             Consumer<E> consumer)
    {
        ImmutableSetMultimap.Builder<Enum<?>, Consumer<DiagnosticEvent>> byTypeBuilder = ImmutableSetMultimap.builder();
        if (subscribersByClassAndType.containsKey(event))
            byTypeBuilder.putAll(subscribersByClassAndType.get(event));
        byTypeBuilder.put(eventType, new TypedConsumerWrapper<>(consumer));

        ImmutableMap.Builder<Class, ImmutableSetMultimap<Enum<?>, Consumer<DiagnosticEvent>>> byClassBuilder = ImmutableMap.builder();
        for (Class clazz : subscribersByClassAndType.keySet())
        {
            if (!clazz.equals(event))
                byClassBuilder.put(clazz, subscribersByClassAndType.get(clazz));
        }

        subscribersByClassAndType = byClassBuilder
                                    .put(event, byTypeBuilder.build())
                                    .build();
    }

    /**
     * Registers event handler for all DiagnosticEvents published from this point.
     * @param consumer Consumer for received events
     */
    public static synchronized void subscribeAll(Consumer<DiagnosticEvent> consumer)
    {
        subscribersAll = ImmutableSet.<Consumer<DiagnosticEvent>>builder()
                         .addAll(subscribersAll)
                         .add(consumer)
                         .build();
    }

    /**
     * De-registers event handler from receiving any further events.
     * @param consumer Consumer registered for receiving events
     */
    public static synchronized <E extends DiagnosticEvent> void unsubscribe(Consumer<E> consumer)
    {
        // all events
        subscribersAll = ImmutableSet.copyOf(Iterables.filter(subscribersAll, (c) -> c != consumer));

        // event class
        ImmutableSetMultimap.Builder<Class<? extends DiagnosticEvent>, Consumer<DiagnosticEvent>> byClassBuilder = ImmutableSetMultimap.builder();
        Collection<Map.Entry<Class<? extends DiagnosticEvent>, Consumer<DiagnosticEvent>>> entries = subscribersByClass.entries();
        for (Map.Entry<Class<? extends DiagnosticEvent>, Consumer<DiagnosticEvent>> entry : entries)
        {
            Consumer<DiagnosticEvent> subscriber = entry.getValue();
            if (subscriber instanceof TypedConsumerWrapper)
                subscriber = ((TypedConsumerWrapper)subscriber).wrapped;

            if (subscriber != consumer)
            {
                byClassBuilder = byClassBuilder.put(entry);
            }
        }
        subscribersByClass = byClassBuilder.build();


        // event class + type
        ImmutableMap.Builder<Class, ImmutableSetMultimap<Enum<?>, Consumer<DiagnosticEvent>>> byClassAndTypeBuilder = ImmutableMap.builder();
        for (Map.Entry<Class, ImmutableSetMultimap<Enum<?>, Consumer<DiagnosticEvent>>> byClassEntry : subscribersByClassAndType.entrySet())
        {
            ImmutableSetMultimap.Builder<Enum<?>, Consumer<DiagnosticEvent>> byTypeBuilder = ImmutableSetMultimap.builder();
            ImmutableSetMultimap<Enum<?>, Consumer<DiagnosticEvent>> byTypeConsumers = byClassEntry.getValue();
            Iterables.filter(byTypeConsumers.entries(), (e) ->
            {
                if (e == null || e.getValue() == null) return false;
                Consumer<DiagnosticEvent> subscriber = e.getValue();
                if (subscriber instanceof TypedConsumerWrapper)
                    subscriber = ((TypedConsumerWrapper) subscriber).wrapped;
                return subscriber != consumer;
            }).forEach(byTypeBuilder::put);

            ImmutableSetMultimap<Enum<?>, Consumer<DiagnosticEvent>> byType = byTypeBuilder.build();
            if (!byType.isEmpty())
                byClassAndTypeBuilder.put(byClassEntry.getKey(), byType);
        }

        subscribersByClassAndType = byClassAndTypeBuilder.build();
    }

    /**
     * Indicates if any {@link Consumer} has been registered for the specified class of events.
     * @param event DiagnosticEvent class implementation
     */
    public static <E extends DiagnosticEvent> boolean hasSubscribers(Class<E> event)
    {
        return !subscribersAll.isEmpty() || subscribersByClass.containsKey(event) || subscribersByClassAndType.containsKey(event);
    }

    /**
     * Indicates if any {@link Consumer} has been registered for the specified class of events.
     * @param event DiagnosticEvent class implementation
     * @param eventType Subscribed event type matched against {@link DiagnosticEvent#getType()}
     */
    public static <E extends DiagnosticEvent, T extends Enum<T>> boolean hasSubscribers(Class<E> event, T eventType)
    {
        if (!subscribersAll.isEmpty())
            return true;

        if (subscribersByClass.containsKey(event) && !subscribersByClass.get(event).isEmpty())
            return true;

        ImmutableSetMultimap<Enum<?>, Consumer<DiagnosticEvent>> byType = subscribersByClassAndType.get(event);
        if (byType == null || byType.isEmpty()) return false;

        Set<Consumer<DiagnosticEvent>> consumers = byType.get(eventType);
        return consumers != null && !consumers.isEmpty();
    }

    /**
     * Returns a yet to be emitted single event represented by a {@link CompletableFuture}.
     *
     * @param event DiagnosticEvent class implementation
     * @param type Matching event type as returned by {@link DiagnosticEvent#getType()}
     */
    public static <E extends DiagnosticEvent, T extends Enum<T>>  CompletableFuture<E> once(Class<E> event, T type)
    {
        final CompletableFuture<E> ret = new CompletableFuture<>();
        final Consumer<E> handler = new Consumer<E>()
        {
            public void accept(E e)
            {
                synchronized (ret)
                {
                    if (!ret.isDone())
                        ret.complete(e);
                }
                unsubscribe(this);
            }
        };
        subscribe(event, type, handler);
        return ret;
    }

    /**
     * Removes all active subscribers. Should only be called from testing.
     */
    public static synchronized void cleanup()
    {
        subscribersByClass = ImmutableSetMultimap.of();
        subscribersAll = ImmutableSet.of();
        subscribersByClassAndType = ImmutableMap.of();
    }

    /**
     * Wrapper class for supporting typed event handling for consumers.
     */
    private static class TypedConsumerWrapper<E> implements Consumer<DiagnosticEvent>
    {
        private final Consumer<E> wrapped;

        private TypedConsumerWrapper(Consumer<E> wrapped)
        {
            this.wrapped = wrapped;
        }

        public void accept(DiagnosticEvent e)
        {
            wrapped.accept((E)e);
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TypedConsumerWrapper<?> that = (TypedConsumerWrapper<?>) o;
            return Objects.equals(wrapped, that.wrapped);
        }

        public int hashCode()
        {
            return Objects.hash(wrapped);
        }
    }
}