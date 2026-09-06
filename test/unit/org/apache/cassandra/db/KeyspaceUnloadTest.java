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

package org.apache.cassandra.db;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import com.google.common.base.Throwables;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.schema.IndexMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KeyspaceUnloadTest extends CQLTester
{
    private static final String FAILURE_MESSAGE = "Simulated failure to invalidate the index";

    private static final String METRIC_PREFIX = "org.apache.cassandra.metrics.keyspace.";

    /**
     * An unload runs while a keyspace is dropped, so it must finish its work even when one table fails.
     * A table can fail on the flush that precedes the unload, or on the invalidation of one of its
     * indexes. This test uses the second of the two, because an index makes the failure deterministic.
     */
    @Test
    public void testAFailingTableLeavesTheRestOfTheKeyspaceUnloaded() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

        createTable(keyspace, "CREATE TABLE %s (k int PRIMARY KEY, v int)");
        createIndex(keyspace, String.format("CREATE CUSTOM INDEX ON %%s(v) USING '%s'", ThrowingIndex.class.getName()));
        createTable(keyspace, "CREATE TABLE %s (k int PRIMARY KEY, v int)");

        Keyspace ks = Keyspace.open(keyspace);
        List<ColumnFamilyStore> tables = new ArrayList<>(ks.getColumnFamilyStores());
        assertEquals(2, tables.size());
        assertFalse("The keyspace registered no metrics, so this test cannot detect a leak.",
                    registeredMetricNames(keyspace).isEmpty());

        try
        {
            ks.unload(true);
            fail("The failing index must make the unload report the error.");
        }
        catch (RuntimeException e)
        {
            assertEquals(FAILURE_MESSAGE, Throwables.getRootCause(e).getMessage());
        }

        for (ColumnFamilyStore cfs : tables)
            assertTrue("Table " + cfs.name + " is still loaded.", !cfs.isValid());

        assertTrue("The keyspace metrics are still registered: " + registeredMetricNames(keyspace),
                   registeredMetricNames(keyspace).isEmpty());
    }

    private static Set<String> registeredMetricNames(String keyspace)
    {
        return CassandraMetricsRegistry.Metrics.getNames()
                                               .stream()
                                               .filter(name -> name.startsWith(METRIC_PREFIX) && name.endsWith('.' + keyspace))
                                               .collect(Collectors.toSet());
    }

    public static class ThrowingIndex extends StubIndex
    {
        public ThrowingIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        @Override
        public Callable<?> getInvalidateTask()
        {
            return () -> { throw new RuntimeException(FAILURE_MESSAGE); };
        }
    }
}
