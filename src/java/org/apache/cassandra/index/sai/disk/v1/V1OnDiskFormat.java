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

package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteOrder;
import java.util.EnumSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.PerIndexWriter;
import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.SearchableIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.format.OnDiskFormat;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.metrics.AbstractMetrics;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.lucene.store.IndexInput;

import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemory;

/**
 * The original SAI OnDiskFormat, found in DSE.  Because it has a simple token -> offsets map, queries
 * against "wide partitions" are slow in proportion to the partition size, since we have to read
 * the whole partition and post-filter the rows
 */
public class V1OnDiskFormat implements OnDiskFormat
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final Set<IndexComponent> PER_SSTABLE_COMPONENTS = EnumSet.of(IndexComponent.GROUP_COMPLETION_MARKER,
                                                                                 IndexComponent.GROUP_META,
                                                                                 IndexComponent.TOKEN_VALUES,
                                                                                 IndexComponent.OFFSETS_VALUES);

    private static final Set<IndexComponent> LITERAL_COMPONENTS = EnumSet.of(IndexComponent.COLUMN_COMPLETION_MARKER,
                                                                             IndexComponent.META,
                                                                             IndexComponent.TERMS_DATA,
                                                                             IndexComponent.POSTING_LISTS);
    private static final Set<IndexComponent> NUMERIC_COMPONENTS = EnumSet.of(IndexComponent.COLUMN_COMPLETION_MARKER,
                                                                             IndexComponent.META,
                                                                             IndexComponent.KD_TREE,
                                                                             IndexComponent.KD_TREE_POSTING_LISTS);

    /**
     * Global limit on heap consumed by all index segment building that occurs outside the context of Memtable flush.
     *
     * Note that to avoid flushing extremely small index segments, a segment is only flushed when
     * both the global size of all building segments has breached the limit and the size of the
     * segment in question reaches (segment_write_buffer_space_mb / # currently building column indexes).
     *
     * ex. If there is only one column index building, it can buffer up to segment_write_buffer_space_mb.
     *
     * ex. If there is one column index building per table across 8 compactors, each index will be
     *     eligible to flush once it reaches (segment_write_buffer_space_mb / 8) MBs.
     */
    public static final long SEGMENT_BUILD_MEMORY_LIMIT = 1024L * 1024L * DatabaseDescriptor.getSAISegmentWriteBufferSpace();

    public static final NamedMemoryLimiter SEGMENT_BUILD_MEMORY_LIMITER =
    new NamedMemoryLimiter(SEGMENT_BUILD_MEMORY_LIMIT, "SSTable-attached Index Segment Builder");

    static
    {
        CassandraMetricsRegistry.MetricName bufferSpaceUsed = DefaultNameFactory.createMetricName(AbstractMetrics.TYPE, "SegmentBufferSpaceUsedBytes", null);
        CassandraMetricsRegistry.Metrics.register(bufferSpaceUsed, (Gauge<Long>) SEGMENT_BUILD_MEMORY_LIMITER::currentBytesUsed);

        CassandraMetricsRegistry.MetricName bufferSpaceLimit = DefaultNameFactory.createMetricName(AbstractMetrics.TYPE, "SegmentBufferSpaceLimitBytes", null);
        CassandraMetricsRegistry.Metrics.register(bufferSpaceLimit, (Gauge<Long>) () -> SEGMENT_BUILD_MEMORY_LIMIT);

        // Note: The active builder count starts at 1 to avoid dividing by zero.
        CassandraMetricsRegistry.MetricName buildsInProgress = DefaultNameFactory.createMetricName(AbstractMetrics.TYPE, "ColumnIndexBuildsInProgress", null);
        CassandraMetricsRegistry.Metrics.register(buildsInProgress, (Gauge<Long>) () -> SegmentBuilder.ACTIVE_BUILDER_COUNT.get() - 1);
    }

    public static final V1OnDiskFormat instance = new V1OnDiskFormat();

    private static final IndexFeatureSet v1IndexFeatureSet = new IndexFeatureSet()
    {
        @Override
        public boolean isRowAware()
        {
            return false;
        }

        @Override
        public boolean hasVectorIndexChecksum()
        {
            return false;
        }
    };

    protected V1OnDiskFormat()
    {}

    @Override
    public IndexFeatureSet indexFeatureSet()
    {
        return v1IndexFeatureSet;
    }

    @Override
    public PrimaryKey.Factory primaryKeyFactory(ClusteringComparator comparator)
    {
        return new PartitionAwarePrimaryKeyFactory();
    }

    @Override
    public PrimaryKeyMap.Factory newPrimaryKeyMapFactory(IndexDescriptor indexDescriptor, SSTableReader sstable) throws IOException
    {
        return new PartitionAwarePrimaryKeyMap.PartitionAwarePrimaryKeyMapFactory(indexDescriptor, sstable);
    }

    @Override
    public SearchableIndex newSearchableIndex(SSTableContext sstableContext, IndexContext indexContext)
    {
        return new V1SearchableIndex(sstableContext, indexContext);
    }

    @Override
    public IndexSearcher newIndexSearcher(SSTableContext sstableContext,
                                          IndexContext indexContext,
                                          PerIndexFiles indexFiles,
                                          SegmentMetadata segmentMetadata) throws IOException
    {
        if (indexContext.isLiteral())
            return new InvertedIndexSearcher(sstableContext.primaryKeyMapFactory(), indexFiles, segmentMetadata, sstableContext.indexDescriptor, indexContext);
        return new KDTreeIndexSearcher(sstableContext.primaryKeyMapFactory(), indexFiles, segmentMetadata, sstableContext.indexDescriptor, indexContext);
    }

    @Override
    public PerSSTableWriter newPerSSTableWriter(IndexDescriptor indexDescriptor) throws IOException
    {
        return new SSTableComponentsWriter(indexDescriptor);
    }

    @Override
    public PerIndexWriter newPerIndexWriter(StorageAttachedIndex index,
                                            IndexDescriptor indexDescriptor,
                                            LifecycleNewTracker tracker,
                                            RowMapping rowMapping,
                                            long keyCount)
    {
        // If we're not flushing or we haven't yet started the initialization build, flush from SSTable contents.
        if (tracker.opType() != OperationType.FLUSH || !index.canFlushFromMemtableIndex())
        {
            NamedMemoryLimiter limiter = SEGMENT_BUILD_MEMORY_LIMITER;
            logger.debug(index.getIndexContext().logMessage("Starting a compaction index build. Global segment memory usage: {}"),
                         prettyPrintMemory(limiter.currentBytesUsed()));

            return new SSTableIndexWriter(indexDescriptor, index.getIndexContext(), limiter, index.isIndexValid(), keyCount);
        }

        return new MemtableIndexWriter(index.getIndexContext().getPendingMemtableIndex(tracker),
                                       indexDescriptor,
                                       index.getIndexContext(),
                                       rowMapping);
    }

    @Override
    public boolean validatePerSSTableComponents(IndexDescriptor indexDescriptor, boolean checksum)
    {
        for (IndexComponent indexComponent : perSSTableComponents())
        {
            if (isBuildCompletionMarker(indexComponent))
                continue;

            try (IndexInput input = indexDescriptor.openPerSSTableInput(indexComponent))
            {
                Version earliest = getExpectedEarliestVersion(indexComponent);
                if (checksum)
                    SAICodecUtils.validateChecksum(input);
                else
                    SAICodecUtils.validate(input, earliest);
            }
            catch (Throwable e)
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug(indexDescriptor.logMessage("{} failed for index component {} on SSTable {}"),
                                 (checksum ? "Checksum validation" : "Validation"),
                                 indexComponent,
                                 indexDescriptor.descriptor);
                }
                return false;
            }
        }
        return true;
    }

    protected Version getExpectedEarliestVersion(IndexComponent indexComponent)
    {
        Version earliest = Version.EARLIEST;
        if (isVectorDataComponent(indexComponent))
        {
            if (!Version.latest().onOrAfter(Version.VECTOR_EARLIEST))
                throw new IllegalStateException("Configured latest version " + Version.latest() + " is not compatible with vector index");
            earliest = Version.VECTOR_EARLIEST;
        }
        return earliest;
    }

    @Override
    public boolean validateOneIndexComponent(IndexComponent component, IndexDescriptor descriptor, IndexContext context, boolean checksum)
    {
        if (isBuildCompletionMarker(component))
            return true;
        // starting with v3, vector components include proper headers and checksum; skip for earlier versions
        if (context.isVector()
            && isVectorDataComponent(component)
            && !descriptor.getVersion(context).onDiskFormat().indexFeatureSet().hasVectorIndexChecksum())
        {
            return true;
        }

        try (IndexInput input = descriptor.openPerIndexInput(component, context))
        {
            if (checksum)
                SAICodecUtils.validateChecksum(input);
            else
                SAICodecUtils.validate(input);
        }
        catch (Throwable e)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug(descriptor.logMessage("{} failed for index component {} on SSTable {}"),
                             (checksum ? "Checksum validation" : "Validation"),
                             component,
                             descriptor.descriptor,
                             e);
            }
            return false;
        }
        return true;
    }

    @Override
    public Set<IndexComponent> perSSTableComponents()
    {
        return PER_SSTABLE_COMPONENTS;
    }

    @Override
    public Set<IndexComponent> perIndexComponents(IndexContext indexContext)
    {
        if (TypeUtil.isLiteral(indexContext.getValidator()))
            return LITERAL_COMPONENTS;
        return NUMERIC_COMPONENTS;
    }

    @Override
    public int openFilesPerSSTable()
    {
        return 2;
    }

    @Override
    public int openFilesPerIndex(IndexContext indexContext)
    {
        // For the V1 format there are always 2 open files per index - index (kdtree or terms) + postings
        return 2;
    }

    @Override
    public ByteOrder byteOrderFor(IndexComponent indexComponent, IndexContext context)
    {
        return ByteOrder.BIG_ENDIAN;
    }

    protected boolean isBuildCompletionMarker(IndexComponent indexComponent)
    {
        return indexComponent == IndexComponent.GROUP_COMPLETION_MARKER ||
               indexComponent == IndexComponent.COLUMN_COMPLETION_MARKER;
    }

    /** vector data components (that did not have checksums before v3) */
    private boolean isVectorDataComponent(IndexComponent indexComponent)
    {
        return indexComponent == IndexComponent.VECTOR ||
               indexComponent == IndexComponent.PQ ||
               indexComponent == IndexComponent.TERMS_DATA ||
               indexComponent == IndexComponent.POSTING_LISTS;
    }
}
