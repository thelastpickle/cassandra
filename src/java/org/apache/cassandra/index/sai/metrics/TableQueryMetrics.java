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

import java.util.EnumMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Table query metrics for different kinds of query. The metrics for each kind of query are divided into two groups:
 * <ul>
 *    <li>Per table counters and timers ({@link PerTable}).</li>
 *    <li>Per query histograms ({@link PerQuery}).</li>
 * </ul>
 * The following kinds of query are tracked:
 * <ul>
 *    <li>All SAI queries.</li>
 *    <li>Single-partition filter queries (filtering only, no top-k).</li>
 *    <li>Multi-partition filter queries (filtering only, no top-k).</li>
 *    <li>Single-partition top-k queries (top-k only, no filtering).</li>
 *    <li>Multi-partition top-k queries (top-k only, no filtering).</li>
 *    <li>Single-partition hybrid queries (both filtering and top-k).</li>
 *    <li>Multi-partition hybrid queries (both filtering and top-k).</li>
 * </ul>
 * The kinds are disjoint, so any combination of them is meaningful.
 * <p>
 * The general metrics for all SAI queries are always recorded. The other kinds of query are recorded only if they are
 * enabled with the {@link CassandraRelevantProperties#SAI_QUERY_KIND_PER_TABLE_METRICS_ENABLED} and
 * {@link CassandraRelevantProperties#SAI_QUERY_KIND_PER_QUERY_METRICS_ENABLED} system properties.
 */
public class TableQueryMetrics
{
    /** Per table metrics for all kinds of query. */
    public final EnumMap<QueryKind, PerTable> perTableMetrics = new EnumMap<>(QueryKind.class);

    /** Per query metrics for all kinds of query. */
    public final EnumMap<QueryKind, PerQuery> perQueryMetrics = new EnumMap<>(QueryKind.class);

    public TableQueryMetrics(TableMetadata table)
    {
        addMetrics(table, QueryKind.ALL, command -> true);
        addMetrics(table, QueryKind.SP_FILTER_ONLY, command -> !command.isTopK() && command.usesIndexFiltering() && command.isSinglePartition());
        addMetrics(table, QueryKind.MP_FILTER_ONLY, command -> !command.isTopK() && command.usesIndexFiltering() && !command.isSinglePartition());
        addMetrics(table, QueryKind.SP_TOPK_ONLY, command -> command.isTopK() && !command.usesIndexFiltering() && command.isSinglePartition());
        addMetrics(table, QueryKind.MP_TOPK_ONLY, command -> command.isTopK() && !command.usesIndexFiltering() && !command.isSinglePartition());
        addMetrics(table, QueryKind.SP_HYBRID, command -> command.isTopK() && command.usesIndexFiltering() && command.isSinglePartition());
        addMetrics(table, QueryKind.MP_HYBRID, command -> command.isTopK() && command.usesIndexFiltering() && !command.isSinglePartition());
    }

    public enum QueryKind
    {
        ALL(""),
        SP_FILTER_ONLY("SinglePartitionFilterOnly"),
        MP_FILTER_ONLY("MultiPartitionFilterOnly"),
        SP_TOPK_ONLY("SinglePartitionTopKOnly"),
        MP_TOPK_ONLY("MultiPartitionTopKOnly"),
        SP_HYBRID("SinglePartitionHybrid"),
        MP_HYBRID("MultiPartitionHybrid");

        private final String name;

        QueryKind(String name)
        {
            this.name = name;
        }
    }

    private void addMetrics(TableMetadata table, QueryKind queryKind, Predicate<ReadCommand> filter)
    {
        if (queryKind == QueryKind.ALL || CassandraRelevantProperties.SAI_QUERY_KIND_PER_TABLE_METRICS_ENABLED.getBoolean())
            perTableMetrics.put(queryKind, new PerTable(table, queryKind, filter));

        if (queryKind == QueryKind.ALL || CassandraRelevantProperties.SAI_QUERY_KIND_PER_QUERY_METRICS_ENABLED.getBoolean())
            perQueryMetrics.put(queryKind, new PerQuery(table, queryKind, filter));
    }

    /**
     * Records the metrics of a single query.
     *
     * @param context the state of the execution of a single query
     * @param command the query command
     */
    public void record(QueryContext context, ReadCommand command)
    {
        Snapshot snapshot = new Snapshot(context);
        perTableMetrics.values().forEach(metrics -> metrics.record(snapshot, command));
        perQueryMetrics.values().forEach(metrics -> metrics.record(snapshot, command));

        if (Tracing.isTracing())
        {
            Tracing.trace("Index query accessed memtable indexes, {}, and {}, post-filtered {} in {}, and took {} microseconds.",
                          pluralize(snapshot.sstablesHit, "SSTable index", "es"), pluralize(snapshot.segmentsHit, "segment", "s"),
                          pluralize(snapshot.rowsFiltered, "row", "s"), pluralize(snapshot.partitionsRead, "partition", "s"),
                          TimeUnit.NANOSECONDS.toMicros(snapshot.totalQueryTimeNs));
        }
    }

    /**
     * Releases all the metrics of all the kinds of query.
     */
    public void release()
    {
        perTableMetrics.values().forEach(PerTable::release);
        perQueryMetrics.values().forEach(PerQuery::release);
    }

    private static String pluralize(long count, String root, String plural)
    {
        return count == 1 ? String.format("1 %s", root) : String.format("%d %s%s", count, root, plural);
    }

    /**
     * Family of metrics for a single kind of query.
     */
    public abstract static class AbstractQueryMetrics extends AbstractMetrics
    {
        private static final Pattern QUERY = Pattern.compile("Query");

        private final Predicate<ReadCommand> filter;

        private AbstractQueryMetrics(String keyspace, String table, String scope, QueryKind queryKind, Predicate<ReadCommand> filter)
        {
            super(keyspace, table, makeName(scope, queryKind));
            this.filter = filter;
        }

        public final void record(Snapshot snapshot, ReadCommand command)
        {
            if (filter.test(command))
                record(snapshot);
        }

        protected abstract void record(Snapshot snapshot);

        public static String makeName(String scope, QueryKind queryKind)
        {
            return QUERY.matcher(scope).replaceFirst(queryKind.name + "Query");
        }
    }

    /**
     * Per table metrics for a single kind of query.
     */
    public static class PerTable extends AbstractQueryMetrics
    {
        public static final String METRIC_TYPE = "TableQueryMetrics";

        public final Timer postFilteringReadLatency;

        public final Counter totalQueryTimeouts;
        public final Counter totalPartitionReads;
        public final Counter totalRowsFiltered;
        public final Counter totalQueriesCompleted;

        /**
         * @param table the table to measure metrics for
         * @param queryKind the kind of query that metrics are recorded for
         * @param filter a predicate that decides whether a query is recorded
         */
        public PerTable(TableMetadata table, QueryKind queryKind, Predicate<ReadCommand> filter)
        {
            super(table.keyspace, table.name, METRIC_TYPE, queryKind, filter);

            postFilteringReadLatency = Metrics.timer(createMetricName("PostFilteringReadLatency"));

            totalPartitionReads = Metrics.counter(createMetricName("TotalPartitionReads"));
            totalRowsFiltered = Metrics.counter(createMetricName("TotalRowsFiltered"));
            totalQueriesCompleted = Metrics.counter(createMetricName("TotalQueriesCompleted"));
            totalQueryTimeouts = Metrics.counter(createMetricName("TotalQueryTimeouts"));
        }

        @Override
        protected void record(Snapshot snapshot)
        {
            if (snapshot.queryTimedOut)
                totalQueryTimeouts.inc();

            totalQueriesCompleted.inc();
            totalPartitionReads.inc(snapshot.partitionsRead);
            totalRowsFiltered.inc(snapshot.rowsFiltered);

            // Top-k queries do not post-filter the rows that they read, so they have no latency to record.
            if (snapshot.postFilteringReadLatency > 0)
                postFilteringReadLatency.update(snapshot.postFilteringReadLatency, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Per query metrics for a single kind of query.
     */
    public static class PerQuery extends AbstractQueryMetrics
    {
        public static final String METRIC_TYPE = "PerQuery";

        public final Timer queryLatency;

        /**
         * Global metrics for all indices hit during the query.
         */
        public final Histogram sstablesHit;
        public final Histogram segmentsHit;
        public final Histogram partitionReads;
        public final Histogram rowsFiltered;

        /**
         * Balanced tree index metrics.
         */
        public final Histogram balancedTreePostingsNumPostings;
        /**
         * Balanced tree index posting lists metrics.
         */
        public final Histogram balancedTreePostingsSkips;
        public final Histogram balancedTreePostingsDecodes;

        /**
         * Trie index posting lists metrics.
         */
        public final Histogram postingsSkips;
        public final Histogram postingsDecodes;

        /**
         * @param table the table to measure metrics for
         * @param queryKind the kind of query that metrics are recorded for
         * @param filter a predicate that decides whether a query is recorded
         */
        public PerQuery(TableMetadata table, QueryKind queryKind, Predicate<ReadCommand> filter)
        {
            super(table.keyspace, table.name, METRIC_TYPE, queryKind, filter);

            queryLatency = Metrics.timer(createMetricName("QueryLatency"));

            sstablesHit = Metrics.histogram(createMetricName("SSTableIndexesHit"), false);
            segmentsHit = Metrics.histogram(createMetricName("IndexSegmentsHit"), false);

            balancedTreePostingsSkips = Metrics.histogram(createMetricName("BalancedTreePostingsSkips"), false);

            balancedTreePostingsNumPostings = Metrics.histogram(createMetricName("BalancedTreePostingsNumPostings"), false);
            balancedTreePostingsDecodes = Metrics.histogram(createMetricName("BalancedTreePostingsDecodes"), false);

            postingsSkips = Metrics.histogram(createMetricName("PostingsSkips"), false);
            postingsDecodes = Metrics.histogram(createMetricName("PostingsDecodes"), false);

            partitionReads = Metrics.histogram(createMetricName("PartitionReads"), false);
            rowsFiltered = Metrics.histogram(createMetricName("RowsFiltered"), false);
        }

        @Override
        protected void record(Snapshot snapshot)
        {
            queryLatency.update(snapshot.totalQueryTimeNs, TimeUnit.NANOSECONDS);

            sstablesHit.update(snapshot.sstablesHit);
            segmentsHit.update(snapshot.segmentsHit);

            partitionReads.update(snapshot.partitionsRead);
            rowsFiltered.update(snapshot.rowsFiltered);

            // Record trie index cache metrics.
            if (snapshot.trieSegmentsHit > 0)
            {
                postingsSkips.update(snapshot.triePostingsSkips);
                postingsDecodes.update(snapshot.triePostingsDecodes);
            }

            // Record balanced tree index cache metrics.
            if (snapshot.balancedTreeSegmentsHit > 0)
            {
                balancedTreePostingsNumPostings.update(snapshot.balancedTreePostingListsHit);
                balancedTreePostingsSkips.update(snapshot.balancedTreePostingsSkips);
                balancedTreePostingsDecodes.update(snapshot.balancedTreePostingsDecodes);
            }
        }
    }

    /**
     * A snapshot of the metrics of a {@link QueryContext} at a single point in time. The snapshot memoises those
     * values, so that {@link AbstractQueryMetrics#record(Snapshot)} can record them for as many
     * {@link AbstractQueryMetrics} as needed without reading the context once and again.
     */
    public static class Snapshot
    {
        private final long totalQueryTimeNs;
        private final long sstablesHit;
        private final long segmentsHit;
        private final long partitionsRead;
        private final long rowsFiltered;
        private final long trieSegmentsHit;
        private final long triePostingsSkips;
        private final long triePostingsDecodes;
        private final long balancedTreePostingListsHit;
        private final long balancedTreeSegmentsHit;
        private final long balancedTreePostingsSkips;
        private final long balancedTreePostingsDecodes;
        private final long postFilteringReadLatency;
        private final boolean queryTimedOut;

        public Snapshot(QueryContext context)
        {
            totalQueryTimeNs = context.totalQueryTimeNs();
            sstablesHit = context.sstablesHit;
            segmentsHit = context.segmentsHit;
            partitionsRead = context.partitionsRead;
            rowsFiltered = context.rowsFiltered;
            trieSegmentsHit = context.trieSegmentsHit;
            triePostingsSkips = context.triePostingsSkips;
            triePostingsDecodes = context.triePostingsDecodes;
            balancedTreePostingListsHit = context.balancedTreePostingListsHit;
            balancedTreeSegmentsHit = context.balancedTreeSegmentsHit;
            balancedTreePostingsSkips = context.balancedTreePostingsSkips;
            balancedTreePostingsDecodes = context.balancedTreePostingsDecodes;
            postFilteringReadLatency = context.getPostFilteringReadLatency();
            queryTimedOut = context.queryTimedOut;
        }
    }
}
