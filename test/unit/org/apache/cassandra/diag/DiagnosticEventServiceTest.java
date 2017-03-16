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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class DiagnosticEventServiceTest
{

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void cleanup()
    {
        DiagnosticEventService.cleanup();
    }

    @Test
    public void testSubscribe()
    {
        assertFalse(DiagnosticEventService.hasSubscribers(TestEvent1.class));
        assertFalse(DiagnosticEventService.hasSubscribers(TestEvent2.class));
        Consumer<TestEvent1> consumer1 = (event) ->
        {
        };
        Consumer<TestEvent1> consumer2 = (event) ->
        {
        };
        Consumer<TestEvent1> consumer3 = (event) ->
        {
        };
        DiagnosticEventService.subscribe(TestEvent1.class, consumer1);
        DiagnosticEventService.subscribe(TestEvent1.class, consumer2);
        DiagnosticEventService.subscribe(TestEvent1.class, consumer3);
        assertTrue(DiagnosticEventService.hasSubscribers(TestEvent1.class));
        assertFalse(DiagnosticEventService.hasSubscribers(TestEvent2.class));
        DiagnosticEventService.unsubscribe(consumer1);
        assertTrue(DiagnosticEventService.hasSubscribers(TestEvent1.class));
        assertFalse(DiagnosticEventService.hasSubscribers(TestEvent2.class));
        DiagnosticEventService.unsubscribe(consumer2);
        DiagnosticEventService.unsubscribe(consumer3);
        assertFalse(DiagnosticEventService.hasSubscribers(TestEvent1.class));
        assertFalse(DiagnosticEventService.hasSubscribers(TestEvent2.class));
    }

    @Test
    public void testSubscribeByType()
    {
        assertFalse(DiagnosticEventService.hasSubscribers(TestEvent1.class));
        assertFalse(DiagnosticEventService.hasSubscribers(TestEvent2.class));
        Consumer<TestEvent1> consumer1 = (event) ->
        {
        };
        Consumer<TestEvent1> consumer2 = (event) ->
        {
        };
        Consumer<TestEvent1> consumer3 = (event) ->
        {
        };

        assertFalse(DiagnosticEventService.hasSubscribers(TestEvent1.class, TestEventType.TEST1));
        DiagnosticEventService.subscribe(TestEvent1.class, TestEventType.TEST1, consumer1);
        assertTrue(DiagnosticEventService.hasSubscribers(TestEvent1.class, TestEventType.TEST1));
        assertFalse(DiagnosticEventService.hasSubscribers(TestEvent1.class, TestEventType.TEST2));

        DiagnosticEventService.subscribe(TestEvent1.class, TestEventType.TEST2, consumer2);
        DiagnosticEventService.subscribe(TestEvent1.class, TestEventType.TEST2, consumer2);
        DiagnosticEventService.subscribe(TestEvent1.class, TestEventType.TEST2, consumer2);
        assertTrue(DiagnosticEventService.hasSubscribers(TestEvent1.class, TestEventType.TEST2));

        assertFalse(DiagnosticEventService.hasSubscribers(TestEvent2.class));

        DiagnosticEventService.subscribe(TestEvent1.class, consumer3);
        assertTrue(DiagnosticEventService.hasSubscribers(TestEvent1.class));
        assertTrue(DiagnosticEventService.hasSubscribers(TestEvent1.class, TestEventType.TEST1));
        assertTrue(DiagnosticEventService.hasSubscribers(TestEvent1.class, TestEventType.TEST2));
        assertTrue(DiagnosticEventService.hasSubscribers(TestEvent1.class, TestEventType.TEST3));

        DiagnosticEventService.unsubscribe(consumer1);
        assertTrue(DiagnosticEventService.hasSubscribers(TestEvent1.class));
        assertFalse(DiagnosticEventService.hasSubscribers(TestEvent2.class));
        DiagnosticEventService.unsubscribe(consumer2);
        DiagnosticEventService.unsubscribe(consumer3);
        assertFalse(DiagnosticEventService.hasSubscribers(TestEvent1.class));
        assertFalse(DiagnosticEventService.hasSubscribers(TestEvent2.class));
    }

    @Test
    public void testSubscribeAll()
    {
        assertFalse(DiagnosticEventService.hasSubscribers(TestEvent1.class));
        assertFalse(DiagnosticEventService.hasSubscribers(TestEvent2.class));
        Consumer<DiagnosticEvent> consumerAll1 = (event) ->
        {
        };
        Consumer<DiagnosticEvent> consumerAll2 = (event) ->
        {
        };
        Consumer<DiagnosticEvent> consumerAll3 = (event) ->
        {
        };
        DiagnosticEventService.subscribeAll(consumerAll1);
        DiagnosticEventService.subscribeAll(consumerAll2);
        DiagnosticEventService.subscribeAll(consumerAll3);
        assertTrue(DiagnosticEventService.hasSubscribers(TestEvent1.class));
        assertTrue(DiagnosticEventService.hasSubscribers(TestEvent2.class));
        DiagnosticEventService.unsubscribe(consumerAll1);
        assertTrue(DiagnosticEventService.hasSubscribers(TestEvent1.class));
        assertTrue(DiagnosticEventService.hasSubscribers(TestEvent2.class));
        DiagnosticEventService.unsubscribe(consumerAll2);
        DiagnosticEventService.unsubscribe(consumerAll3);
        assertFalse(DiagnosticEventService.hasSubscribers(TestEvent1.class));
        assertFalse(DiagnosticEventService.hasSubscribers(TestEvent2.class));
    }

    @Test
    public void testCleanup()
    {
        assertFalse(DiagnosticEventService.hasSubscribers(TestEvent1.class));
        assertFalse(DiagnosticEventService.hasSubscribers(TestEvent2.class));
        Consumer<TestEvent1> consumer = (event) ->
        {
        };
        DiagnosticEventService.subscribe(TestEvent1.class, consumer);
        Consumer<DiagnosticEvent> consumerAll = (event) ->
        {
        };
        DiagnosticEventService.subscribeAll(consumerAll);
        assertTrue(DiagnosticEventService.hasSubscribers(TestEvent1.class));
        assertTrue(DiagnosticEventService.hasSubscribers(TestEvent2.class));
        DiagnosticEventService.cleanup();
        assertFalse(DiagnosticEventService.hasSubscribers(TestEvent1.class));
        assertFalse(DiagnosticEventService.hasSubscribers(TestEvent2.class));
    }

    @Test
    public void testPublish()
    {
        TestEvent1 a = new TestEvent1();
        TestEvent1 b = new TestEvent1();
        TestEvent1 c = new TestEvent1();
        List<TestEvent1> events = ImmutableList.of(a, b, c, c, c);

        List<DiagnosticEvent> consumed = new LinkedList<>();
        Consumer<TestEvent1> consumer = consumed::add;
        Consumer<DiagnosticEvent> consumerAll = consumed::add;

        DiagnosticEventService.publish(c);
        DiagnosticEventService.subscribe(TestEvent1.class, consumer);
        DiagnosticEventService.publish(a);
        DiagnosticEventService.unsubscribe(consumer);
        DiagnosticEventService.publish(c);
        DiagnosticEventService.subscribeAll(consumerAll);
        DiagnosticEventService.publish(b);
        DiagnosticEventService.subscribe(TestEvent1.class, TestEventType.TEST3, consumer);
        DiagnosticEventService.publish(c);
        DiagnosticEventService.subscribe(TestEvent1.class, TestEventType.TEST1, consumer);
        DiagnosticEventService.publish(c);

        assertEquals(events, consumed);
    }

    private static class TestEvent1 extends DiagnosticEvent
    {
        public TestEventType getType()
        {
            return TestEventType.TEST1;
        }

        public Object getSource()
        {
            return null;
        }

        public HashMap<String, Serializable> toMap()
        {
            return null;
        }
    }

    private static class TestEvent2 extends DiagnosticEvent
    {
        public TestEventType getType()
        {
            return TestEventType.TEST2;
        }

        public Object getSource()
        {
            return null;
        }

        public HashMap<String, Serializable> toMap()
        {
            return null;
        }
    }

    private enum TestEventType { TEST1, TEST2, TEST3 };
}
