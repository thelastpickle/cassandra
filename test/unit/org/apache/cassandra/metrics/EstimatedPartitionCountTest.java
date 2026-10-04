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

package org.apache.cassandra.metrics;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.mockito.MockedStatic;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

public class EstimatedPartitionCountTest extends CQLTester
{
    @Test
    public void testViewChangeDuringCount() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("INSERT INTO %s (id) VALUES (1)");
        flush();

        // HARNESS: count real SSTables, with a deterministic flush during the first scan.
        AtomicInteger scans = new AtomicInteger();
        try (MockedStatic<SSTableReader> readers = mockStatic(SSTableReader.class))
        {
            readers.when(() -> SSTableReader.getApproximateKeyCount(any())).thenAnswer(invocation -> {
                long count = (long) invocation.callRealMethod();
                if (scans.incrementAndGet() == 1)
                {
                    // TRIGGER: replace the view after counting its referenced SSTables.
                    execute("INSERT INTO %s (id) VALUES (2)");
                    flush();
                }
                return count;
            });

            // ORACLE: CASSANDRA-21615 must return the completed estimate without rescanning.
            // Retrying a scan on every view change can prevent the call from returning.
            assertEquals(1L, cfs.metric.estimatedPartitionCountInSSTables.getAsLong());
            assertEquals("A view change must not retry the completed scan", 1, scans.get());

            // A later call must count the new view, then reuse that count while it is unchanged.
            assertEquals(2L, cfs.metric.estimatedPartitionCountInSSTables.getAsLong());
            assertEquals(2, scans.get());
            assertEquals(2L, cfs.metric.estimatedPartitionCountInSSTables.getAsLong());
            assertEquals(2, scans.get());
        }
    }
}
