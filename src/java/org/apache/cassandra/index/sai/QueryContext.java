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

package org.apache.cassandra.index.sai;

import java.io.IOException;
import java.util.HashSet;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import io.github.jbellis.jvector.util.Bits;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.disk.vector.JVectorLuceneOnDiskGraph;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Tracks state relevant to the execution of a single query, including metrics and timeout monitoring.
 */
@NotThreadSafe
public class QueryContext
{
    private static final boolean DISABLE_TIMEOUT = CassandraRelevantProperties.TEST_SAI_DISABLE_TIMEOUT.getBoolean();

    protected final long queryStartTimeNanos;

    public final long executionQuotaNano;

    private final LongAdder sstablesHit = new LongAdder();
    private final LongAdder segmentsHit = new LongAdder();
    private final LongAdder partitionsRead = new LongAdder();
    private final LongAdder rowsFiltered = new LongAdder();

    private final LongAdder trieSegmentsHit = new LongAdder();

    private final LongAdder bkdPostingListsHit = new LongAdder();
    private final LongAdder bkdSegmentsHit = new LongAdder();

    private final LongAdder bkdPostingsSkips = new LongAdder();
    private final LongAdder bkdPostingsDecodes = new LongAdder();

    private final LongAdder triePostingsSkips = new LongAdder();
    private final LongAdder triePostingsDecodes = new LongAdder();

    private final LongAdder tokenSkippingCacheHits = new LongAdder();
    private final LongAdder tokenSkippingLookups = new LongAdder();

    private final LongAdder queryTimeouts = new LongAdder();

    private final LongAdder hnswVectorsAccessed = new LongAdder();
    private final LongAdder hnswVectorCacheHits = new LongAdder();

    private final LongAdder shadowedKeysLoopCount = new LongAdder();
    private final NavigableSet<PrimaryKey> shadowedPrimaryKeys = new ConcurrentSkipListSet<>();

    @VisibleForTesting
    public QueryContext()
    {
        this(DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS));
    }

    public QueryContext(long executionQuotaMs)
    {
        this.executionQuotaNano = TimeUnit.MILLISECONDS.toNanos(executionQuotaMs);
        queryStartTimeNanos = nanoTime();
    }

    public long totalQueryTimeNs()
    {
        return nanoTime() - queryStartTimeNanos;
    }

    // setters
    public void addSstablesHit(long val)
    {
        sstablesHit.add(val);
    }
    public void addSegmentsHit(long val) {
        segmentsHit.add(val);
    }
    public void addPartitionsRead(long val)
    {
        partitionsRead.add(val);
    }
    public void addRowsFiltered(long val)
    {
        rowsFiltered.add(val);
    }
    public void addTrieSegmentsHit(long val)
    {
        trieSegmentsHit.add(val);
    }
    public void addBkdPostingListsHit(long val)
    {
        bkdPostingListsHit.add(val);
    }
    public void addBkdSegmentsHit(long val)
    {
        bkdSegmentsHit.add(val);
    }
    public void addBkdPostingsSkips(long val)
    {
        bkdPostingsSkips.add(val);
    }
    public void addBkdPostingsDecodes(long val)
    {
        bkdPostingsDecodes.add(val);
    }
    public void addTriePostingsSkips(long val)
    {
        triePostingsSkips.add(val);
    }
    public void addTriePostingsDecodes(long val)
    {
        triePostingsDecodes.add(val);
    }
    public void addTokenSkippingCacheHits(long val)
    {
        tokenSkippingCacheHits.add(val);
    }
    public void addTokenSkippingLookups(long val)
    {
        tokenSkippingLookups.add(val);
    }
    public void addQueryTimeouts(long val)
    {
        queryTimeouts.add(val);
    }
    public void addHnswVectorsAccessed(long val)
    {
        hnswVectorsAccessed.add(val);
    }
    public void addHnswVectorCacheHits(long val)
    {
        hnswVectorCacheHits.add(val);
    }

    public void addShadowedKeysLoopCount(long val)
    {
        shadowedKeysLoopCount.add(val);
    }

    // getters

    public long sstablesHit()
    {
        return sstablesHit.longValue();
    }
    public long segmentsHit() {
        return segmentsHit.longValue();
    }
    public long partitionsRead()
    {
        return partitionsRead.longValue();
    }
    public long rowsFiltered()
    {
        return rowsFiltered.longValue();
    }
    public long trieSegmentsHit()
    {
        return trieSegmentsHit.longValue();
    }
    public long bkdPostingListsHit()
    {
        return bkdPostingListsHit.longValue();
    }
    public long bkdSegmentsHit()
    {
        return bkdSegmentsHit.longValue();
    }
    public long bkdPostingsSkips()
    {
        return bkdPostingsSkips.longValue();
    }
    public long bkdPostingsDecodes()
    {
        return bkdPostingsDecodes.longValue();
    }
    public long triePostingsSkips()
    {
        return triePostingsSkips.longValue();
    }
    public long triePostingsDecodes()
    {
        return triePostingsDecodes.longValue();
    }
    public long tokenSkippingCacheHits()
    {
        return tokenSkippingCacheHits.longValue();
    }
    public long tokenSkippingLookups()
    {
        return tokenSkippingLookups.longValue();
    }
    public long queryTimeouts()
    {
        return queryTimeouts.longValue();
    }
    public long hnswVectorsAccessed()
    {
        return hnswVectorsAccessed.longValue();
    }
    public long hnswVectorCacheHits()
    {
        return hnswVectorCacheHits.longValue();
    }
    
    public void checkpoint()
    {
        if (totalQueryTimeNs() >= executionQuotaNano && !DISABLE_TIMEOUT)
        {
            addQueryTimeouts(1);
            throw new AbortedOperationException();
        }
    }

    public long shadowedKeysLoopCount()
    {
        return shadowedKeysLoopCount.longValue();
    }

    public void recordShadowedPrimaryKey(PrimaryKey primaryKey)
    {
        boolean isNewKey = shadowedPrimaryKeys.add(primaryKey);
        assert isNewKey : "Duplicate shadowed primary key added. Key should have been filtered out earlier in query.";
    }

    // Returns true if the row ID will be included or false if the row ID will be shadowed
    public boolean shouldInclude(long sstableRowId, PrimaryKeyMap primaryKeyMap)
    {
        return !shadowedPrimaryKeys.contains(primaryKeyMap.primaryKeyFromRowId(sstableRowId));
    }

    public boolean shouldInclude(PrimaryKey pk)
    {
        return !shadowedPrimaryKeys.contains(pk);
    }

    /**
     * @return shadowed primary keys, in ascending order
     */
    public NavigableSet<PrimaryKey> getShadowedPrimaryKeys()
    {
        return shadowedPrimaryKeys;
    }

    public Bits bitsetForShadowedPrimaryKeys(CassandraOnHeapGraph<PrimaryKey> graph)
    {
        if (getShadowedPrimaryKeys().isEmpty())
            return null;

        return new IgnoredKeysBits(graph, getShadowedPrimaryKeys());
    }

    public Bits bitsetForShadowedPrimaryKeys(SegmentMetadata metadata, PrimaryKeyMap primaryKeyMap, JVectorLuceneOnDiskGraph graph) throws IOException
    {
        Set<Integer> ignoredOrdinals = null;
        try (var ordinalsView = graph.getOrdinalsView())
        {
            for (PrimaryKey primaryKey : getShadowedPrimaryKeys())
            {
                // not in current segment
                if (primaryKey.compareTo(metadata.minKey) < 0 || primaryKey.compareTo(metadata.maxKey) > 0)
                    continue;

                long sstableRowId = primaryKeyMap.exactRowIdForPrimaryKey(primaryKey);
                if (sstableRowId < 0) // not found
                    continue;

                int segmentRowId = metadata.toSegmentRowId(sstableRowId);
                // not in segment yet
                if (segmentRowId < 0)
                    continue;
                // end of segment
                if (segmentRowId > metadata.maxSSTableRowId)
                    break;

                int ordinal = ordinalsView.getOrdinalForRowId(segmentRowId);
                if (ordinal >= 0)
                {
                    if (ignoredOrdinals == null)
                        ignoredOrdinals = new HashSet<>();
                    ignoredOrdinals.add(ordinal);
                }
            }
        }

        if (ignoredOrdinals == null)
            return null;

        return new IgnoringBits(ignoredOrdinals, graph.size());
    }

    private static class IgnoringBits implements Bits
    {
        private final Set<Integer> ignoredOrdinals;
        private final int maxOrdinal;

        public IgnoringBits(Set<Integer> ignoredOrdinals, int maxOrdinal)
        {
            this.ignoredOrdinals = ignoredOrdinals;
            this.maxOrdinal = maxOrdinal;
        }

        @Override
        public boolean get(int index)
        {
            return !ignoredOrdinals.contains(index);
        }

        @Override
        public int length()
        {
            return maxOrdinal;
        }
    }

    private static class IgnoredKeysBits implements Bits
    {
        private final CassandraOnHeapGraph<PrimaryKey> graph;
        private final NavigableSet<PrimaryKey> ignored;

        public IgnoredKeysBits(CassandraOnHeapGraph<PrimaryKey> graph, NavigableSet<PrimaryKey> ignored)
        {
            this.graph = graph;
            this.ignored = ignored;
        }

        @Override
        public boolean get(int ordinal)
        {
            var keys = graph.keysFromOrdinal(ordinal);
            return keys.stream().anyMatch(k -> !ignored.contains(k));
        }

        @Override
        public int length()
        {
            return graph.size();
        }
    }
}
