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

import java.nio.ByteBuffer;

import org.apache.cassandra.sensors.Sensor;

/**
 * A utility class that contains the definition of custom params added to the {@link Message} header to propagate {@link Sensor} values from
 * writer to coordinator and necessary methods to encode sensor values as appropriate for the internode message format.
 */
public final class SensorsCustomParams
{
    /**
     * The per-request read bytes value for a given keyspace and table.
     */
    public static final String READ_BYTES_REQUEST = "READ_BYTES_REQUEST";
    /**
     * The total read bytes value for a given keyspace and table, across all requests. This is a monotonically increasing value.
     */
    public static final String READ_BYTES_TABLE = "READ_BYTES_TABLE";

    private SensorsCustomParams()
    {
    }

    /**
     * Utility method to encode senors value as byte buffer in the big endian order.
     */
    public static byte[] sensorValueAsBytes(double value)
    {
        ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
        buffer.putDouble(value);

        return buffer.array();
    }
}
