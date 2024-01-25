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
package org.apache.cassandra.sensors;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.config.CassandraRelevantProperties.BF_RECREATE_ON_FP_CHANCE_CHANGE;
import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.assertj.core.api.Assertions.assertThat;

public class SensorsReadTest
{
    public static final String KEYSPACE1 = "SensorsReadTest";
    public static final String CF_STANDARD = "Standard";
    public static final String CF_STANDARD_CLUSTERING = "StandardClustering";

    private static final String READ_BYTES_REQUEST = "READ_BYTES_REQUEST";
    private static final String READ_BYTES_TABLE = "READ_BYTES_TABLE";

    private ColumnFamilyStore store;
    private CopyOnWriteArrayList<Message> capturedOutboundMessages;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD,
                                                              1, AsciiType.instance, AsciiType.instance, null),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD_CLUSTERING,
                                                              1, AsciiType.instance, AsciiType.instance, AsciiType.instance));

        CompactionManager.instance.disableAutoCompaction();
    }

    @Before
    public void beforeTest()
    {
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE1).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD).metadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD_CLUSTERING).metadata());

        capturedOutboundMessages = new CopyOnWriteArrayList<>();
        MessagingService.instance().outboundSink.add((message, to) ->
        {
            capturedOutboundMessages.add(message);
            return false;
        });
    }

    @After
    public void afterTest()
    {
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD_CLUSTERING).truncateBlocking();

        RequestTracker.instance.set(null);
        SensorsRegistry.instance.clear();

        BF_RECREATE_ON_FP_CHANCE_CHANGE.setBoolean(false);
    }

    @Test
    public void testMemtableRead()
    {
        store = discardSSTables(KEYSPACE1, CF_STANDARD);
        Context context = new Context(KEYSPACE1, CF_STANDARD, store.metadata.id.toString());

        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }

        DecoratedKey key = store.getPartitioner().decorateKey(ByteBufferUtil.bytes("4"));
        ReadCommand command = Util.cmd(store, key).build();
        handleReadCommand(command);

        assertRequestAndRegistrySensorsEquality(context);
        Sensor requestSensor = getThreadLocalRequestSensor();
        assertResponseSensors(requestSensor.getValue(), requestSensor.getValue());
    }

    @Test
    public void testSinglePartitionReadCommand_ByPartitionKey()
    {
        store = discardSSTables(KEYSPACE1, CF_STANDARD);
        Context context = new Context(KEYSPACE1, CF_STANDARD, store.metadata.id.toString());

        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }

        store.forceBlockingFlush(UNIT_TESTS);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();

        DecoratedKey key = sstable.decorateKey(ByteBufferUtil.bytes("4"));
        ReadCommand command = Util.cmd(store, key).build();
        handleReadCommand(command);

        assertRequestAndRegistrySensorsEquality(context);
        Sensor requestSensor = getThreadLocalRequestSensor();
        assertResponseSensors(requestSensor.getValue(), requestSensor.getValue());
    }

    @Test
    public void testSinglePartitionReadCommand_ByClustering()
    {
        store = discardSSTables(KEYSPACE1, CF_STANDARD_CLUSTERING);
        Context context = new Context(KEYSPACE1, CF_STANDARD_CLUSTERING, store.metadata.id.toString());

        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, "0")
            .clustering(String.valueOf(j))
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }

        store.forceBlockingFlush(UNIT_TESTS);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();

        DecoratedKey key = sstable.decorateKey(ByteBufferUtil.bytes("0"));
        ReadCommand command = Util.cmd(store, key).includeRow("0").build();
        handleReadCommand(command);

        assertRequestAndRegistrySensorsEquality(context);

        Sensor requestSensor = getThreadLocalRequestSensor();
        assertResponseSensors(requestSensor.getValue(), requestSensor.getValue());
    }

    @Test
    public void testSinglePartitionReadCommand_AllowFiltering()
    {
        store = discardSSTables(KEYSPACE1, CF_STANDARD_CLUSTERING);
        Context context = new Context(KEYSPACE1, CF_STANDARD_CLUSTERING, store.metadata.id.toString());

        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, "0")
            .clustering(String.valueOf(j))
            .add("val", String.valueOf(j))
            .build()
            .applyUnsafe();
        }

        store.forceBlockingFlush(UNIT_TESTS);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();

        DecoratedKey key = sstable.decorateKey(ByteBufferUtil.bytes("0"));
        ReadCommand command1 = Util.cmd(store, key).includeRow("0").build();
        handleReadCommand(command1);

        Sensor request1Sensor = getThreadLocalRequestSensor();
        // Extract the value as later we will reset the thread local and the sensor value will be lost
        long request1Bytes = (long) request1Sensor.getValue();

        assertThat(request1Sensor.getValue()).isGreaterThan(0);
        assertThat(request1Sensor).isEqualTo(getRegistrySensor(context));
        assertResponseSensors(request1Sensor.getValue(), request1Sensor.getValue());

        getThreadLocalRequestSensor().reset();
        capturedOutboundMessages.clear();

        ReadCommand command2 = Util.cmd(store, key).filterOn("val", Operator.EQ, "9").build();
        handleReadCommand(command2);

        Sensor request2Sensor = getThreadLocalRequestSensor();
        assertThat(request2Sensor.getValue()).isEqualTo(request1Bytes * 10);
        assertThat(getRegistrySensor(context).getValue()).isEqualTo(request1Bytes + request2Sensor.getValue());
        assertResponseSensors(request2Sensor.getValue(), request1Bytes + request2Sensor.getValue());
    }

    @Test
    public void testPartitionRangeReadCommand_ByPartitionKey()
    {
        store = discardSSTables(KEYSPACE1, CF_STANDARD);
        Context context = new Context(KEYSPACE1, CF_STANDARD, store.metadata.id.toString());

        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }

        store.forceBlockingFlush(UNIT_TESTS);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();

        DecoratedKey key = sstable.decorateKey(ByteBufferUtil.bytes("0"));
        ReadCommand command1 = Util.cmd(store, key).build();
        handleReadCommand(command1);

        Sensor request1Sensor = getThreadLocalRequestSensor();
        // Extract the value as later we will reset the thread local and the sensor value will be lost
        long request1Bytes = (long) request1Sensor.getValue();

        assertThat(request1Sensor.getValue()).isGreaterThan(0);
        assertThat(request1Sensor).isEqualTo(getRegistrySensor(context));
        assertResponseSensors(request1Bytes, request1Bytes);

        getThreadLocalRequestSensor().reset();
        capturedOutboundMessages.clear();

        ReadCommand command2 = Util.cmd(store).fromKeyIncl("0").toKeyIncl("9").build();
        handleReadCommand(command2);

        Sensor request2Sensor = getThreadLocalRequestSensor();
        assertThat(request2Sensor.getValue()).isEqualTo(request1Bytes * 10);
        assertThat(getRegistrySensor(context).getValue()).isEqualTo(request1Bytes + request2Sensor.getValue());
        assertResponseSensors(request2Sensor.getValue(), request1Bytes + request2Sensor.getValue());
    }

    private ColumnFamilyStore discardSSTables(String ks, String cf)
    {
        Keyspace keyspace = Keyspace.open(ks);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cf);
        cfs.discardSSTables(System.currentTimeMillis());
        return cfs;
    }

    private static void handleReadCommand(ReadCommand command)
    {
        ReadCommandVerbHandler.instance.doVerb(Message.builder(Verb.READ_REQ, command).build());
    }

    private void assertRequestAndRegistrySensorsEquality(Context context)
    {
        Sensor localSensor = getThreadLocalRequestSensor();
        assertThat(localSensor.getValue()).isGreaterThan(0);

        Sensor registrySensor = getRegistrySensor(context);
        assertThat(registrySensor).isEqualTo(localSensor);
    }

    private void assertResponseSensors(double requestValue, double registryValue)
    {
        assertThat(capturedOutboundMessages).hasSize(1);
        Message message = capturedOutboundMessages.get(0);
        assertResponseSensors(message, requestValue, registryValue);

        // make sure messages with sensor values can be deserialized on the receiving node
        DataOutputBuffer out = serialize(message);
        Message deserializedMessage = deserialize(out, message.from());
        assertResponseSensors(deserializedMessage, requestValue, registryValue);
    }

    private void assertResponseSensors(Message message, double requestValue, double registryValue)
    {
        assertThat(message.header.customParams()).isNotNull();
        assertThat(message.header.customParams()).containsKey(READ_BYTES_REQUEST);
        assertThat(message.header.customParams()).containsKey(READ_BYTES_TABLE);

        double requestReadBytes = bytesToDouble(message.header.customParams().get(READ_BYTES_REQUEST));
        double tableReadBytes = bytesToDouble(message.header.customParams().get(READ_BYTES_TABLE));
        assertThat(requestReadBytes).isEqualTo(requestValue);
        assertThat(tableReadBytes).isEqualTo(registryValue);
    }

    private static double bytesToDouble(byte[] bytes)
    {
        ByteBuffer readBytesBuffer = ByteBuffer.allocate(Double.BYTES);
        readBytesBuffer.put(bytes);
        readBytesBuffer.flip();
        return readBytesBuffer.getDouble();
    }

    private static DataOutputBuffer serialize(Message message)
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            int messagingVersion = MessagingService.current_version;
            Message.serializer.serialize(message, out, messagingVersion);
            return out;
        }
        catch (IOException e)
        {
            throw new RuntimeException("Cannot serialize message " + message, e);
        }
    }

    private static Message deserialize(DataOutputBuffer out, InetAddressAndPort peer)
    {
        try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
        {
            int messagingVersion = MessagingService.current_version;
            return Message.serializer.deserialize(in, peer, messagingVersion);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Cannot deserialize message from " + peer, e);
        }
    }

    /**
     * Returns the read sensor with the given context from the global registry
     * @param context the sensor context
     * @return the requested read sensor from the global registry
     */
    private static Sensor getRegistrySensor(Context context)
    {
        return SensorsRegistry.instance.getOrCreateSensor(context, Type.READ_BYTES).get();
    }

    /**
     * Returns the read sensor registered in the thread local {@link RequestSensors}
     * @return the thread local read sensor
     */
    private static Sensor getThreadLocalRequestSensor()
    {
        return RequestTracker.instance.get().getSensor(Type.READ_BYTES).get();
    }
}
