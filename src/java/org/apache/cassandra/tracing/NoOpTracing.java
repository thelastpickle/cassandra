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
package org.apache.cassandra.tracing;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;

/** Starting Cassandra with `-Dcassandra.custom_tracing_class=org.apache.cassandra.tracing.NoOpTracing`
 * will forcibly disable all tracing. See `jvm.options` 
 *
 * This can be useful in defensive environments.
 */
public final class NoOpTracing extends Tracing
{
    public TraceState begin(String request, InetAddress client, Map<String, String> parameters)
    {
        return NoOpTraceState.instance;
    }

    protected TraceState newTraceState(InetAddressAndPort coordinator, UUID sessionId, TraceType traceType)
    {
        return NoOpTraceState.instance;
    }

    protected void stopSessionImpl() {}

    public void trace(ByteBuffer sessionId, String message, int ttl) {}

    private static class NoOpTraceState extends TraceState
    {
        private static final NoOpTraceState instance = new NoOpTraceState();
        
        private NoOpTraceState()
        {
            super(FBUtilities.getBroadcastNativeAddressAndPort(), UUID.randomUUID(), TraceType.NONE);
        }

        protected void traceImpl(String message) {}
    }
}
