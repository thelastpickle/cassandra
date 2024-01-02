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

package org.apache.cassandra.index.sai.disk.vector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.util.Bits;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.CollectionRangeIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.RangeUtil;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static java.lang.Math.log;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.pow;

public class VectorMemtableIndex implements MemtableIndex
{
    private final Logger logger = LoggerFactory.getLogger(VectorMemtableIndex.class);

    private final IndexContext indexContext;
    private final CassandraOnHeapGraph<PrimaryKey> graph;
    private final LongAdder writeCount = new LongAdder();

    private PrimaryKey minimumKey;
    private PrimaryKey maximumKey;

    private final NavigableSet<PrimaryKey> primaryKeys = new ConcurrentSkipListSet<>();

    public VectorMemtableIndex(IndexContext indexContext)
    {
        this.indexContext = indexContext;
        this.graph = new CassandraOnHeapGraph<>(indexContext.getValidator(), indexContext.getIndexWriterConfig());
    }

    @Override
    public void index(DecoratedKey key, Clustering clustering, ByteBuffer value, Memtable memtable, OpOrder.Group opGroup)
    {
        if (value == null || value.remaining() == 0)
            return;

        var primaryKey = indexContext.keyFactory().create(key, clustering);
        long allocatedBytes = index(primaryKey, value);
        memtable.markExtraOnHeapUsed(allocatedBytes, opGroup);
    }

    private long index(PrimaryKey primaryKey, ByteBuffer value)
    {
        if (value == null || value.remaining() == 0)
            return 0;

        updateKeyBounds(primaryKey);

        writeCount.increment();
        primaryKeys.add(primaryKey);
        return graph.add(value, primaryKey, CassandraOnHeapGraph.InvalidVectorBehavior.FAIL);
    }

    @Override
    public void update(DecoratedKey key, Clustering clustering, ByteBuffer oldValue, ByteBuffer newValue, Memtable memtable, OpOrder.Group opGroup)
    {
        int oldRemaining = oldValue == null ? 0 : oldValue.remaining();
        int newRemaining = newValue == null ? 0 : newValue.remaining();
        if (oldRemaining == 0 && newRemaining == 0)
            return;

        boolean different;
        if (oldRemaining != newRemaining)
        {
            assert oldRemaining == 0 || newRemaining == 0; // one of them is null
            different = true;
        }
        else
        {
            different = IntStream.range(0, oldRemaining).anyMatch(i -> oldValue.get(i) != newValue.get(i));
        }

        if (different)
        {
            var primaryKey = indexContext.keyFactory().create(key, clustering);
            // update bounds because only rows with vectors are included in the key bounds,
            // so if the vector was null before, we won't have included it
            updateKeyBounds(primaryKey);

            // make the changes in this order so we don't have a window where the row is not in the index at all
            if (newRemaining > 0)
                graph.add(newValue, primaryKey, CassandraOnHeapGraph.InvalidVectorBehavior.FAIL);
            if (oldRemaining > 0)
                graph.remove(oldValue, primaryKey);

            // remove primary key if it's no longer indexed
            if (newRemaining <= 0 && oldRemaining > 0)
                primaryKeys.remove(primaryKey);
        }
    }

    private void updateKeyBounds(PrimaryKey primaryKey) {
        if (minimumKey == null)
            minimumKey = primaryKey;
        else if (primaryKey.compareTo(minimumKey) < 0)
            minimumKey = primaryKey;
        if (maximumKey == null)
            maximumKey = primaryKey;
        else if (primaryKey.compareTo(maximumKey) > 0)
            maximumKey = primaryKey;
    }

    @Override
    public RangeIterator search(QueryContext queryContext, Expression expr, AbstractBounds<PartitionPosition> keyRange, int limit)
    {
        assert expr.getOp() == Expression.Op.ANN || expr.getOp() == Expression.Op.BOUNDED_ANN : "Only ANN is supported for vector search, received " + expr.getOp();

        float[] qv = expr.lower.value.vector;
        if (expr.getEuclideanSearchThreshold() > 0)
            limit = 100000;

        Bits bits;
        if (RangeUtil.coversFullRing(keyRange))
        {
            // partition/range deletion won't trigger index update, so we have to filter shadow primary keys in memtable index
            bits = queryContext.bitsetForShadowedPrimaryKeys(graph);
        }
        else
        {
            // if left bound is MIN_BOUND or KEY_BOUND, we need to include all token-only PrimaryKeys with same token
            boolean leftInclusive = keyRange.left.kind() != PartitionPosition.Kind.MAX_BOUND;
            // if right bound is MAX_BOUND or KEY_BOUND, we need to include all token-only PrimaryKeys with same token
            boolean rightInclusive = keyRange.right.kind() != PartitionPosition.Kind.MIN_BOUND;
            // if right token is MAX (Long.MIN_VALUE), there is no upper bound
            boolean isMaxToken = keyRange.right.getToken().isMinimum(); // max token

            PrimaryKey left = indexContext.keyFactory().createTokenOnly(keyRange.left.getToken()); // lower bound
            PrimaryKey right = isMaxToken ? null : indexContext.keyFactory().createTokenOnly(keyRange.right.getToken()); // upper bound

            Set<PrimaryKey> resultKeys = isMaxToken ? primaryKeys.tailSet(left, leftInclusive) : primaryKeys.subSet(left, leftInclusive, right, rightInclusive);
            if (!queryContext.getShadowedPrimaryKeys().isEmpty())
                resultKeys = resultKeys.stream().filter(queryContext::shouldInclude).collect(Collectors.toSet());

            if (resultKeys.isEmpty())
                return RangeIterator.empty();

            int bruteForceRows = maxBruteForceRows(limit, resultKeys.size(), graph.size());
            logger.trace("Search range covers {} rows; max brute force rows is {} for memtable index with {} nodes, LIMIT {}",
                         resultKeys.size(), bruteForceRows, graph.size(), limit);
            Tracing.trace("Search range covers {} rows; max brute force rows is {} for memtable index with {} nodes, LIMIT {}",
                          resultKeys.size(), bruteForceRows, graph.size(), limit);
            if (resultKeys.size() <= bruteForceRows)
                return new ReorderingRangeIterator(new PriorityQueue<>(resultKeys));
            else
                bits = new KeyRangeFilteringBits(keyRange, queryContext.bitsetForShadowedPrimaryKeys(graph));
        }

        var keyQueue = graph.search(qv, limit, expr.getEuclideanSearchThreshold(), bits);
        if (keyQueue.isEmpty())
            return RangeIterator.empty();
        return new ReorderingRangeIterator(keyQueue);
    }

    @Override
    public RangeIterator limitToTopResults(QueryContext context, List<PrimaryKey> keys, Expression exp, int limit)
    {
        if (minimumKey == null)
            // This case implies maximumKey is empty too.
            return RangeIterator.empty();

        List<PrimaryKey> results = keys.stream()
                                      .dropWhile(k -> k.compareTo(minimumKey) < 0)
                                      .takeWhile(k -> k.compareTo(maximumKey) <= 0)
                                      .collect(Collectors.toList());

        int maxBruteForceRows = maxBruteForceRows(limit, results.size(), graph.size());
        logger.trace("SAI materialized {} rows; max brute force rows is {} for memtable index with {} nodes, LIMIT {}",
                     results.size(), maxBruteForceRows, graph.size(), limit);
        Tracing.trace("SAI materialized {} rows; max brute force rows is {} for memtable index with {} nodes, LIMIT {}",
                      results.size(), maxBruteForceRows, graph.size(), limit);
        if (results.size() <= maxBruteForceRows)
        {
            if (results.isEmpty())
                return RangeIterator.empty();
            return new CollectionRangeIterator(minimumKey, maximumKey, results);
        }

        float[] qv = exp.lower.value.vector;
        var bits = new KeyFilteringBits(results);
        var keyQueue = graph.search(qv, limit, 0.0f, bits);
        if (keyQueue.isEmpty())
            return RangeIterator.empty();
        return new ReorderingRangeIterator(keyQueue);
    }

    private int maxBruteForceRows(int limit, int nPermittedOrdinals, int graphSize)
    {
        int expectedNodesVisited = expectedNodesVisited(limit, nPermittedOrdinals, graphSize);
        int expectedComparisons = indexContext.getIndexWriterConfig().getMaximumNodeConnections() * expectedNodesVisited;
        // in-memory comparisons are cheaper than pulling a row off disk and then comparing
        // VSTODO this is dramatically oversimplified
        // larger dimension should increase this, because comparisons are more expensive
        // lower chunk cache hit ratio should decrease this, because loading rows is more expensive
        double memoryToDiskFactor = 0.25;
        return (int) max(limit, memoryToDiskFactor * expectedComparisons);
    }

    /**
     * All parameters must be greater than zero.  nPermittedOrdinals may be larger than graphSize.
     */
    public static int expectedNodesVisited(int limit, int nPermittedOrdinals, int graphSize)
    {
        var K = limit;
        var B = min(nPermittedOrdinals, graphSize);
        var N = graphSize;
        // These constants come from running many searches on a variety of datasets and graph sizes.
        // * It is very consistent that the visited count is slightly less than linear wrt K, for both
        //   unconstrained (B = N) and constrained (B < N) searches.
        // * The behavior wrt B is hard to characterize.  Graphing the result F vs N/B shows ranges of
        //   growth very close to linear, interspersed with sharp jumps up to a higher visit count.  Overall,
        //   approximating it as linear is in the right ballpark.
        // * For unconstrained searches, the visited count is closest to log(N) but for constrained searches
        //   it is closer to log(N)**2 (or a higher exponent), perhaps as a result of N/B being too small.
        //
        // If we need to make this even more accurate, the relationship to B and to log(N) may be the best
        // places to start.
        var raw = (int) (100 + 0.025 * pow(log(N), 2) * pow(K, 0.95) * ((double) N / B));
        return ensureSaneEstimate(raw, limit, graphSize);
    }

    public static int ensureSaneEstimate(int rawEstimate, int limit, int graphSize)
    {
        // we will always visit at least min(limit, graphSize) nodes, and we can't visit more nodes than exist in the graph
        return min(max(rawEstimate, min(limit, graphSize)), graphSize);
    }

    @Override
    public Iterator<Pair<ByteComparable, Iterator<PrimaryKey>>> iterator(DecoratedKey min, DecoratedKey max)
    {
        // This method is only used when merging an in-memory index with a RowMapping. This is done a different
        // way with the graph using the writeData method below.
        throw new UnsupportedOperationException();
    }

    public SegmentMetadata.ComponentMetadataMap writeData(IndexDescriptor indexDescriptor, IndexContext indexContext, Function<PrimaryKey, Integer> postingTransformer) throws IOException
    {
        return graph.writeData(indexDescriptor, indexContext, postingTransformer);
    }

    @Override
    public long writeCount()
    {
        return writeCount.longValue();
    }

    @Override
    public long estimatedOnHeapMemoryUsed()
    {
        return graph.ramBytesUsed();
    }

    @Override
    public long estimatedOffHeapMemoryUsed()
    {
        return 0;
    }

    @Override
    public boolean isEmpty()
    {
        return graph.isEmpty();
    }

    @Nullable
    @Override
    public ByteBuffer getMinTerm()
    {
        return null;
    }

    @Nullable
    @Override
    public ByteBuffer getMaxTerm()
    {
        return null;
    }

    private class KeyRangeFilteringBits implements Bits
    {
        private final AbstractBounds<PartitionPosition> keyRange;
        @Nullable
        private final Bits bits;

        public KeyRangeFilteringBits(AbstractBounds<PartitionPosition> keyRange, @Nullable Bits bits)
        {
            this.keyRange = keyRange;
            this.bits = bits;
        }

        @Override
        public boolean get(int ordinal)
        {
            if (bits != null && !bits.get(ordinal))
                return false;

            var keys = graph.keysFromOrdinal(ordinal);
            return keys.stream().anyMatch(k -> keyRange.contains(k.partitionKey()));
        }

        @Override
        public int length()
        {
            return graph.size();
        }
    }

    private class ReorderingRangeIterator extends RangeIterator
    {
        private final PriorityQueue<PrimaryKey> keyQueue;

        ReorderingRangeIterator(PriorityQueue<PrimaryKey> keyQueue)
        {
            super(minimumKey, maximumKey, keyQueue.size());
            this.keyQueue = keyQueue;
        }

        @Override
        protected void performSkipTo(PrimaryKey nextKey)
        {
            while (!keyQueue.isEmpty() && keyQueue.peek().compareTo(nextKey) < 0)
                keyQueue.poll();
        }

        @Override
        public void close() {}

        @Override
        protected PrimaryKey computeNext()
        {
            if (keyQueue.isEmpty())
                return endOfData();
            return keyQueue.poll();
        }
    }

    private class KeyFilteringBits implements Bits
    {
        private final Set<PrimaryKey> results;

        /**
         * A {@link Bits} implementation that filters out all ordinals that do not correspond to a {@link PrimaryKey}
         * in the provided list.
         * @param results - an ordered list of {@link PrimaryKey}s
         */
        public KeyFilteringBits(List<PrimaryKey> results)
        {
            this.results = new HashSet<>(results);
        }

        @Override
        public boolean get(int i)
        {
            var pks = graph.keysFromOrdinal(i);
            for (var pk : pks)
                if (results.contains(pk))
                    return true;
            return false;
        }

        @Override
        public int length()
        {
            return results.size();
        }
    }
}