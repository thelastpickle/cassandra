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

package org.apache.cassandra.index.sai.plan;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Triple;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.BasePartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.utils.InMemoryPartitionIterator;
import org.apache.cassandra.index.sai.utils.InMemoryUnfilteredPartitionIterator;
import org.apache.cassandra.index.sai.utils.PartitionInfo;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.ToFloatFunction;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * Processor that scans all rows from given partitions and selects rows with top-k scores based on vector indexes.
 *
 * This processor performs the following steps:
 * - collect rows with score into PriorityQueue that sorts rows based on score. If there are multiple vector indexes,
 *   the final score is the sum of all vector index scores.
 * - remove rows with the lowest scores from PQ if PQ size exceeds limit
 * - return rows from PQ in primary key order to client
 *
 * Note that recall will be lower with paging, because:
 * - page size is used as limit
 * - for the first query, coordinator returns global top page-size rows within entire ring
 * - for the subsequent queries, coordinators returns global top page-size rows withom range from last-returned-row to max token
 */
public class VectorTopKProcessor
{
    private final ReadCommand command;
    private final IndexContext indexContext;
    private final float[] queryVector;

    private final int limit;

    private int rowCount = 0;

    // Nullable because it is an optimization to know there is no chance of cache hits.
    // Currently, the coordinator doesn't support this optimization, so it is always null there.
    @Nullable
    private final ToFloatFunction<PrimaryKey> scoreCache;

    public VectorTopKProcessor(ReadCommand command, ToFloatFunction<PrimaryKey> scoreCache)
    {
        this.command = command;
        this.scoreCache = scoreCache;

        Pair<IndexContext, float[]> annIndexAndExpression = findTopKIndexContext();
        Preconditions.checkNotNull(annIndexAndExpression);

        this.indexContext = annIndexAndExpression.left;
        this.queryVector = annIndexAndExpression.right;
        this.limit = command.limits().count();
    }

    /**
     * Filter given partitions and keep the rows with highest scores. In case of {@link UnfilteredPartitionIterator},
     * all tombstones will be kept.
     */
    public <U extends Unfiltered, R extends BaseRowIterator<U>, P extends BasePartitionIterator<R>> BasePartitionIterator<?> filter(P partitions)
    {
        // priority queue ordered by score in ascending order
        PriorityQueue<Triple<PartitionInfo, Row, Float>> topK = new PriorityQueue<>(limit, Comparator.comparing(Triple::getRight));
        // to store top-k results in primary key order
        TreeMap<PartitionInfo, TreeSet<Unfiltered>> unfilteredByPartition = new TreeMap<>(Comparator.comparing(p -> p.key));

        while (partitions.hasNext())
        {
            try (R partition = partitions.next())
            {
                DecoratedKey key = partition.partitionKey();
                Row staticRow = partition.staticRow();
                PartitionInfo partitionInfo = PartitionInfo.create(partition);
                // compute key and static row score once per partition
                float keyAndStaticScore = getScoreForRow(key, staticRow, true);

                while (partition.hasNext())
                {
                    Unfiltered unfiltered = partition.next();
                    // Always include tombstones for coordinator. It relies on ReadCommand#withMetricsRecording to throw
                    // TombstoneOverwhelmingException to prevent OOM.
                    if (!unfiltered.isRow())
                    {
                        unfilteredByPartition.computeIfAbsent(partitionInfo, k -> new TreeSet<>(command.metadata().comparator))
                                             .add(unfiltered);
                        continue;
                    }

                    Row row = (Row) unfiltered;
                    float rowScore = getScoreForRow(key, row, false) + keyAndStaticScore;
                    topK.add(Triple.of(partitionInfo, row, rowScore));

                    // when exceeding limit, remove row with low score
                    while (topK.size() > limit)
                        topK.poll();
                }
            }
        }
        partitions.close();

        // reorder rows in partition/clustering order
        for (Triple<PartitionInfo, Row, Float> triple : topK)
            unfilteredByPartition.computeIfAbsent(triple.getLeft(), k -> new TreeSet<>(command.metadata().comparator))
                                 .add(triple.getMiddle());

        rowCount = topK.size();

        if (partitions instanceof PartitionIterator)
            return new InMemoryPartitionIterator(command, unfilteredByPartition);
        return new InMemoryUnfilteredPartitionIterator(command, unfilteredByPartition);
    }

    /**
     * @return num of collected rows
     */
    public int rowCount()
    {
        return rowCount;
    }

    /**
     * Sum the scores from different vector indexes for the row
     */
    private float getScoreForRow(DecoratedKey key, Row row, boolean partitionLevel)
    {
        ColumnMetadata column = indexContext.getDefinition();

        if (column.isPrimaryKeyColumn() && key == null)
            return 0;

        if (column.isStatic() && !row.isStatic())
            return 0;

        if ((column.isClusteringColumn() || column.isRegular()) && row.isStatic())
            return 0;

        // if the query context has score, e.g. at replica side
        if (scoreCache != null && key != null && row != null)
        {
            PrimaryKey primaryKey = indexContext.keyFactory().create(key, row.clustering());
            float score = scoreCache.applyAsFloat(primaryKey);
            if (score >= 0)
                return score;
        }

        ByteBuffer value = indexContext.getValueOf(partitionLevel ? key : null, row, FBUtilities.nowInSeconds());
        if (value != null)
        {
            float[] vector = TypeUtil.decomposeVector(indexContext, value);
            return indexContext.getIndexWriterConfig().getSimilarityFunction().compare(vector, queryVector);
        }
        return 0;
    }


    private Pair<IndexContext, float[]> findTopKIndexContext()
    {
        ColumnFamilyStore cfs = Keyspace.openAndGetStore(command.metadata());

        for (RowFilter.Expression expression : command.rowFilter().getExpressions())
        {
            StorageAttachedIndex sai = findVectorIndexFor(cfs.indexManager, expression);
            if (sai != null)
            {

                float[] qv = TypeUtil.decomposeVector(sai.getIndexContext(), expression.getIndexValue().duplicate());
                return Pair.create(sai.getIndexContext(), qv);
            }
        }

        return null;
    }

    @Nullable
    private StorageAttachedIndex findVectorIndexFor(SecondaryIndexManager sim, RowFilter.Expression e)
    {
        if (e.operator() != Operator.ANN)
            return null;

        Optional<Index> index = sim.getBestIndexFor(e);
        return (StorageAttachedIndex) index.filter(i -> i instanceof StorageAttachedIndex).orElse(null);
    }
}
