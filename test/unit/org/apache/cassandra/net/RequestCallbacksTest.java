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
package org.apache.cassandra.net;

import java.net.UnknownHostException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RequestCallbacksTest
{
    private static InetAddressAndPort peer;

    private static final RequestCallback<NoPayload> callback = message -> {};

    @BeforeClass
    public static void beforeClass() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();
        peer = InetAddressAndPort.getByName("127.0.0.1");
    }

    @Test
    public void testNoCallbackIsRegisteredAfterShutdownNow()
    {
        RequestCallbacks callbacks = new RequestCallbacks(MessagingService.instance());
        callbacks.shutdownNow(true);
        assertTrue(callbacks.isShutdown());

        // the reaper is gone, so a callback registered now would stay in the map for the life of the process
        Message<NoPayload> message = echoRequest();
        callbacks.addWithExpiration(callback, message, peer);
        assertNull(callbacks.get(message.id(), peer));
    }

    @Test
    public void testGracefulShutdownTerminatesWhenARequestArrivesLate() throws Exception
    {
        RequestCallbacks callbacks = new RequestCallbacks(MessagingService.instance());

        // one outstanding callback, far from its expiry, so the graceful shutdown waits for it
        Message<NoPayload> outstanding = echoRequest();
        callbacks.addWithExpiration(callback, outstanding, peer);
        callbacks.shutdownGracefully();
        assertTrue(callbacks.isShutdown());
        assertNotNull(callbacks.get(outstanding.id(), peer));

        // a request that reaches the coordinator after the shutdown started must not extend the wait
        Message<NoPayload> late = echoRequest();
        callbacks.addWithExpiration(callback, late, peer);
        assertNull(callbacks.get(late.id(), peer));

        // the response to the outstanding request empties the map, so the next pass shuts the reaper down
        assertNotNull(callbacks.remove(outstanding.id(), peer));
        callbacks.awaitTerminationUntil(nanoTime() + SECONDS.toNanos(30));
    }

    private static Message<NoPayload> echoRequest()
    {
        return Message.out(Verb.ECHO_REQ, NoPayload.noPayload, nanoTime() + SECONDS.toNanos(300));
    }
}
