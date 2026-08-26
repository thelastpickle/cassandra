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
package org.apache.cassandra.index.sai.metrics;

import javax.management.InstanceNotFoundException;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.datastax.driver.core.ResultSet;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics.QueryKind;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.index.sai.metrics.TableQueryMetrics.AbstractQueryMetrics.makeName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class QueryMetricsTest extends AbstractMetricsTest
{
    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s.%s (id1 TEXT PRIMARY KEY, v1 INT, v2 TEXT) WITH compaction = " +
                                                        "{'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";
    private static final String CREATE_INDEX_TEMPLATE = "CREATE CUSTOM INDEX IF NOT EXISTS %s ON %s.%s(%s) USING 'StorageAttachedIndex'";

    private static final String TABLE_QUERY_METRIC_TYPE = TableQueryMetrics.PerTable.METRIC_TYPE;
    private static final String PER_QUERY_METRIC_TYPE = TableQueryMetrics.PerQuery.METRIC_TYPE;

    /** The kinds of query that the general metrics of all queries are split into. */
    private static final QueryKind[] QUERY_KINDS = { QueryKind.SP_FILTER_ONLY, QueryKind.MP_FILTER_ONLY,
                                                     QueryKind.SP_TOPK_ONLY, QueryKind.MP_TOPK_ONLY,
                                                     QueryKind.SP_HYBRID, QueryKind.MP_HYBRID };

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @After
    public void resetQueryKindMetrics()
    {
        CassandraRelevantProperties.SAI_QUERY_KIND_PER_TABLE_METRICS_ENABLED.setBoolean(false);
        CassandraRelevantProperties.SAI_QUERY_KIND_PER_QUERY_METRICS_ENABLED.setBoolean(false);
    }

    @Test
    public void testSameIndexNameAcrossKeyspaces()
    {
        String table = "test_same_index_name_across_keyspaces";
        String index = "test_same_index_name_across_keyspaces_index";

        String keyspace1 = createKeyspace(CREATE_KEYSPACE_TEMPLATE);
        String keyspace2 = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace1, table));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, index, keyspace1, table, "v1"));

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace2, table));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, index, keyspace2, table, "v1"));

        execute("INSERT INTO " + keyspace1 + '.' + table + " (id1, v1, v2) VALUES ('0', 0, '0')");

        ResultSet rows = executeNet("SELECT id1 FROM " + keyspace1 + '.' + table + " WHERE v1 = 0");
        assertEquals(1, rows.all().size());

        assertEquals(1L, getTableQueryMetrics(keyspace1, table, "TotalQueriesCompleted"));
        assertEquals(1L, getTableQueryMetrics(keyspace1, table, "PostFilteringReadLatency"));
        assertEquals(0L, getTableQueryMetrics(keyspace2, table, "TotalQueriesCompleted"));
        assertEquals(0L, getTableQueryMetrics(keyspace2, table, "PostFilteringReadLatency"));

        execute("INSERT INTO " + keyspace2 + '.' + table + " (id1, v1, v2) VALUES ('0', 0, '0')");
        execute("INSERT INTO " + keyspace2 + '.' + table + " (id1, v1, v2) VALUES ('1', 1, '1')");

        rows = executeNet("SELECT id1 FROM " + keyspace1 + '.' + table + " WHERE v1 = 0");
        assertEquals(1, rows.all().size());

        rows = executeNet("SELECT id1 FROM " + keyspace2 + '.' + table + " WHERE v1 = 1");
        assertEquals(1, rows.all().size());

        assertEquals(2L, getTableQueryMetrics(keyspace1, table, "TotalQueriesCompleted"));
        assertEquals(1L, getTableQueryMetrics(keyspace2, table, "TotalQueriesCompleted"));
        assertEquals(2L, getTableQueryMetrics(keyspace1, table, "PostFilteringReadLatency"));
        assertEquals(1L, getTableQueryMetrics(keyspace2, table, "PostFilteringReadLatency"));
    }

    @Test
    public void testMetricRelease() throws Throwable
    {
        String table = "test_metric_release";
        String index = "test_metric_release_index";

        String keyspace = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace, table));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, index, keyspace, table, "v1"));

        execute("INSERT INTO " + keyspace + '.' + table + " (id1, v1, v2) VALUES ('0', 0, '0')");

        ResultSet rows = executeNet("SELECT id1 FROM " + keyspace + '.' + table + " WHERE v1 = 0");
        assertEquals(1, rows.all().size());

        assertEquals(1L, getTableQueryMetrics(keyspace, table, "TotalQueriesCompleted"));

        // If we drop the last index on the table we should no longer see the table-level state metrics:
        dropIndex(String.format("DROP INDEX %s." + index, keyspace));
        assertThatThrownBy(() -> getTableQueryMetrics(keyspace, table, "TotalQueriesCompleted")).hasCauseInstanceOf(InstanceNotFoundException.class);
    }

    /**
     * Test the {@link ReadCommand} flags that decide the kind of query (top-k only, filtering only, hybrid,
     * single partition and multipartition) in metrics.
     */
    @Test
    public void testQueryKindFlags() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, n int, s text, v vector<float, 2>, PRIMARY KEY(k, c))");

        // test without indexes
        assertQueryKindFlags("SELECT * FROM %s", false, false);
        assertQueryKindFlags("SELECT * FROM %s WHERE n = 1 ALLOW FILTERING", false, false);

        // test with legacy indexes
        String idx = createIndex("CREATE INDEX ON %s(n)");
        assertQueryKindFlags("SELECT * FROM %s", false, false);
        assertQueryKindFlags("SELECT * FROM %s WHERE n = 1", false, true);
        assertQueryKindFlags("SELECT * FROM %s WHERE n = 1 AND s = 'a' ALLOW FILTERING", false, true);

        // test with SAI indexes
        dropIndex("DROP INDEX %s." + idx);
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        assertQueryKindFlags("SELECT * FROM %s", false, false);
        assertQueryKindFlags("SELECT * FROM %s WHERE n = 1", false, true);
        assertQueryKindFlags("SELECT * FROM %s WHERE n = 1 AND s = 'a' ALLOW FILTERING", false, true);
        assertQueryKindFlags("SELECT * FROM %s ORDER BY v ANN OF [1, 1] LIMIT 10", true, false);
        assertQueryKindFlags("SELECT * FROM %s WHERE n = 1 ORDER BY v ANN OF [1, 1] LIMIT 10", true, true);
    }

    private void assertQueryKindFlags(String query, boolean expectedIsTopK, boolean expectedUsesIndexFiltering)
    {
        ReadCommand command = parseReadCommand(query);
        assertEquals(query, expectedIsTopK, command.isTopK());
        assertEquals(query, expectedUsesIndexFiltering, command.usesIndexFiltering());
    }

    private ReadCommand parseReadCommand(String query)
    {
        SelectStatement select = (SelectStatement) QueryProcessor.parseStatement(formatQuery(query), ClientState.forInternalCalls());
        ReadQuery readQuery = select.getQuery(QueryOptions.DEFAULT, FBUtilities.nowInSeconds());
        assertThat(readQuery).isInstanceOf(ReadCommand.class);
        return (ReadCommand) readQuery;
    }

    /**
     * Test that the metrics of the different kinds of query are separated, and that they are only created if the
     * properties that enable them are set.
     */
    @Test
    public void testQueryKindMetrics()
    {
        testQueryKindMetrics(false, false);
        testQueryKindMetrics(false, true);
        testQueryKindMetrics(true, false);
        testQueryKindMetrics(true, true);
    }

    private void testQueryKindMetrics(boolean perTable, boolean perQuery)
    {
        CassandraRelevantProperties.SAI_QUERY_KIND_PER_TABLE_METRICS_ENABLED.setBoolean(perTable);
        CassandraRelevantProperties.SAI_QUERY_KIND_PER_QUERY_METRICS_ENABLED.setBoolean(perQuery);

        // create a table with indexes for a numeric column and a vector column
        createTable("CREATE TABLE %s (k int, c int, n int, v vector<float, 2>, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        int numPartitions = 11;
        int numRowsPerPartition = 13;
        int numRows = numPartitions * numRowsPerPartition;
        for (int k = 0; k < numPartitions; k++)
            for (int c = 0; c < numRowsPerPartition; c++)
                execute("INSERT INTO %s (k, c, n, v) VALUES (?, ?, 1, [1, 1])", k, c);

        // multi-partition filter query
        UntypedResultSet rows = execute("SELECT k, c FROM %s WHERE n = 1");
        assertEquals(numRows, rows.size());

        // multi-partition top-k query
        rows = execute("SELECT k, c FROM %s ORDER BY v ANN OF [1, 1] LIMIT 1000");
        assertEquals(numRows, rows.size());

        // single-partition top-k query
        rows = execute("SELECT k, c FROM %s WHERE k = 0 ORDER BY v ANN OF [1, 1] LIMIT 1000");
        assertEquals(numRowsPerPartition, rows.size());

        // single-partition filter query
        rows = execute("SELECT k, c FROM %s WHERE k = 0 AND n = 1");
        assertEquals(numRowsPerPartition, rows.size());

        // multi-partition hybrid query
        rows = execute("SELECT k, c FROM %s WHERE n = 1 ORDER BY v ANN OF [1, 1] LIMIT 1000");
        assertEquals(numRows, rows.size());

        // single-partition hybrid query
        rows = execute("SELECT k, c FROM %s WHERE k = 0 AND n = 1 ORDER BY v ANN OF [1, 1] LIMIT 1000");
        assertEquals(numRowsPerPartition, rows.size());

        // Each of the six queries belongs to exactly one kind, so the general counter is six and each kind is one.
        assertCountersPerKind(perTable, "TotalQueriesCompleted", 6, 1);

        // No query times out, so all the timeout counters are zero.
        assertCountersPerKind(perTable, "TotalQueryTimeouts", 0, 0);

        // The kinds are disjoint, so the general counters are the sum of the counters of all the kinds.
        assertCountersAddUp(perTable, "TotalPartitionReads");
        assertCountersAddUp(perTable, "TotalRowsFiltered");

        // Every query updates one histogram of its kind, and one general histogram.
        assertHistogramsPerKind(perQuery, "PartitionReads");
        assertHistogramsPerKind(perQuery, "RowsFiltered");
    }

    private void assertCountersPerKind(boolean perTable, String name, long expectedForAllKinds, long expectedPerKind)
    {
        waitForEquals(objectName(name, TABLE_QUERY_METRIC_TYPE), expectedForAllKinds);
        for (QueryKind kind : QUERY_KINDS)
            assertEqualsIfExists(perTable, objectName(name, makeName(TABLE_QUERY_METRIC_TYPE, kind)), expectedPerKind);
    }

    private void assertCountersAddUp(boolean perTable, String name)
    {
        long all = counter(objectName(name, TABLE_QUERY_METRIC_TYPE));
        if (!perTable)
        {
            for (QueryKind kind : QUERY_KINDS)
                assertMetricDoesNotExist(objectName(name, makeName(TABLE_QUERY_METRIC_TYPE, kind)));
            return;
        }

        long sum = 0;
        for (QueryKind kind : QUERY_KINDS)
            sum += counter(objectName(name, makeName(TABLE_QUERY_METRIC_TYPE, kind)));
        assertEquals(name, all, sum);
    }

    private void assertHistogramsPerKind(boolean perQuery, String name)
    {
        waitForEquals(objectName(name, PER_QUERY_METRIC_TYPE), 6);
        for (QueryKind kind : QUERY_KINDS)
            assertEqualsIfExists(perQuery, objectName(name, makeName(PER_QUERY_METRIC_TYPE, kind)), 1);
    }

    private void assertEqualsIfExists(boolean shouldExist, ObjectName name, long value)
    {
        if (shouldExist)
            waitForEquals(name, value);
        else
            assertMetricDoesNotExist(name);
    }

    private long counter(ObjectName name)
    {
        return ((Number) getMetricValue(name)).longValue();
    }

    private ObjectName objectName(String name, String type)
    {
        return objectNameNoIndex(name, KEYSPACE, currentTable(), type);
    }
}
