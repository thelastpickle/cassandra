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

package org.apache.cassandra.io.sstable.format;

import java.io.IOException;
<<<<<<< HEAD
=======
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
<<<<<<< HEAD
import java.util.UUID;
=======
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc

import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.cache.InstrumentingCache;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.metrics.RestorableMeter;
<<<<<<< HEAD
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.IFilter;
=======
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.Pair;
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
import org.apache.cassandra.utils.concurrent.Ref;

public abstract class ForwardingSSTableReader extends SSTableReader
{
<<<<<<< HEAD
    private final SSTableReader delegate;
    private final Ref<SSTableReader> selfRef;
=======
    // This method is only accessiable via extension and not for calling directly;
    // to work around this, rely on reflection if the method gets called
    private static final Method ESTIMATE_ROWS_FROM_INDEX;

    static
    {
        try
        {
            Method m = SSTable.class.getDeclaredMethod("estimateRowsFromIndex", RandomAccessReader.class);
            m.setAccessible(true);
            ESTIMATE_ROWS_FROM_INDEX = m;
        }
        catch (NoSuchMethodException e)
        {
            throw new AssertionError(e);
        }
    }

    private final SSTableReader delegate;
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc

    public ForwardingSSTableReader(SSTableReader delegate)
    {
        super(delegate.descriptor, SSTable.componentsFor(delegate.descriptor),
<<<<<<< HEAD
              TableMetadataRef.forOfflineTools(delegate.metadata()), delegate.maxDataAge, delegate.getSSTableMetadata(),
=======
              delegate.metadata, delegate.maxDataAge, delegate.getSSTableMetadata(),
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
              delegate.openReason, delegate.header);
        this.delegate = delegate;
        this.first = delegate.first;
        this.last = delegate.last;
<<<<<<< HEAD
        this.selfRef = new Ref<>(this, new Tidy()
        {
            public void tidy() throws Exception
            {
                Ref<SSTableReader> ref = delegate.tryRef();
                if (ref != null)
                    ref.release();
            }

            public String name()
            {
                return descriptor.toString();
            }
        });
    }

    protected RowIndexEntry getPosition(PartitionPosition key, Operator op, boolean updateCacheAndStats, boolean permitMatchPastLast, SSTableReadsListener listener)
    {
        return delegate.getPosition(key, op, updateCacheAndStats, permitMatchPastLast, listener);
    }

    public UnfilteredRowIterator iterator(DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reversed, SSTableReadsListener listener)
    {
        return delegate.iterator(key, slices, selectedColumns, reversed, listener);
    }

    public UnfilteredRowIterator iterator(FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry, Slices slices, ColumnFilter selectedColumns, boolean reversed)
    {
        return delegate.iterator(file, key, indexEntry, slices, selectedColumns, reversed);
    }

    public UnfilteredRowIterator simpleIterator(FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry, boolean tombstoneOnly)
    {
        return delegate.simpleIterator(file, key, indexEntry, tombstoneOnly);
    }

    public ISSTableScanner getScanner()
    {
        return delegate.getScanner();
    }

    public ISSTableScanner getScanner(Collection<Range<Token>> ranges)
    {
        return delegate.getScanner(ranges);
    }

    public ISSTableScanner getScanner(Iterator<AbstractBounds<PartitionPosition>> rangeIterator)
    {
        return delegate.getScanner(rangeIterator);
    }

    public ISSTableScanner getScanner(ColumnFilter columns, DataRange dataRange, SSTableReadsListener listener)
    {
        return delegate.getScanner(columns, dataRange, listener);
    }

    public void setupOnline()
    {
        delegate.setupOnline();
    }

=======
    }

    @Override
    public boolean equals(Object that)
    {
        return delegate.equals(that);
    }

    @Override
    public int hashCode()
    {
        return delegate.hashCode();
    }

    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public String getFilename()
    {
        return delegate.getFilename();
    }

<<<<<<< HEAD
    public boolean equals(Object that)
    {
        return delegate.equals(that);
    }

    public int hashCode()
    {
        return delegate.hashCode();
    }

=======
    @Override
    public void setupOnline()
    {
        delegate.setupOnline();
    }

    @Override
    public boolean isKeyCacheSetup()
    {
        return delegate.isKeyCacheSetup();
    }

    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public boolean loadSummary()
    {
        return delegate.loadSummary();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public void saveSummary()
    {
        delegate.saveSummary();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public void saveBloomFilter()
    {
        delegate.saveBloomFilter();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public void setReplaced()
    {
        delegate.setReplaced();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public boolean isReplaced()
    {
        return delegate.isReplaced();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public void runOnClose(Runnable runOnClose)
    {
        delegate.runOnClose(runOnClose);
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public SSTableReader cloneWithRestoredStart(DecoratedKey restoredStart)
    {
        return delegate.cloneWithRestoredStart(restoredStart);
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public SSTableReader cloneWithNewStart(DecoratedKey newStart, Runnable runOnClose)
    {
        return delegate.cloneWithNewStart(newStart, runOnClose);
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public SSTableReader cloneWithNewSummarySamplingLevel(ColumnFamilyStore parent, int samplingLevel) throws IOException
    {
        return delegate.cloneWithNewSummarySamplingLevel(parent, samplingLevel);
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public RestorableMeter getReadMeter()
    {
        return delegate.getReadMeter();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public int getIndexSummarySamplingLevel()
    {
        return delegate.getIndexSummarySamplingLevel();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public long getIndexSummaryOffHeapSize()
    {
        return delegate.getIndexSummaryOffHeapSize();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public int getMinIndexInterval()
    {
        return delegate.getMinIndexInterval();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public double getEffectiveIndexInterval()
    {
        return delegate.getEffectiveIndexInterval();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public void releaseSummary()
    {
        delegate.releaseSummary();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public long getIndexScanPosition(PartitionPosition key)
    {
        return delegate.getIndexScanPosition(key);
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public CompressionMetadata getCompressionMetadata()
    {
        return delegate.getCompressionMetadata();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public long getCompressionMetadataOffHeapSize()
    {
        return delegate.getCompressionMetadataOffHeapSize();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public void forceFilterFailures()
    {
        delegate.forceFilterFailures();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public IFilter getBloomFilter()
    {
        return delegate.getBloomFilter();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public long getBloomFilterSerializedSize()
    {
        return delegate.getBloomFilterSerializedSize();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public long getBloomFilterOffHeapSize()
    {
        return delegate.getBloomFilterOffHeapSize();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public long estimatedKeys()
    {
        return delegate.estimatedKeys();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public long estimatedKeysForRanges(Collection<Range<Token>> ranges)
    {
        return delegate.estimatedKeysForRanges(ranges);
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public int getIndexSummarySize()
    {
        return delegate.getIndexSummarySize();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public int getMaxIndexSummarySize()
    {
        return delegate.getMaxIndexSummarySize();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public byte[] getIndexSummaryKey(int index)
    {
        return delegate.getIndexSummaryKey(index);
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public Iterable<DecoratedKey> getKeySamples(Range<Token> range)
    {
        return delegate.getKeySamples(range);
    }

<<<<<<< HEAD
    public List<PartitionPositionBounds> getPositionsForRanges(Collection<Range<Token>> ranges)
=======
    @Override
    public List<Pair<Long, Long>> getPositionsForRanges(Collection<Range<Token>> ranges)
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    {
        return delegate.getPositionsForRanges(ranges);
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public KeyCacheKey getCacheKey(DecoratedKey key)
    {
        return delegate.getCacheKey(key);
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public void cacheKey(DecoratedKey key, RowIndexEntry info)
    {
        delegate.cacheKey(key, info);
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public RowIndexEntry getCachedPosition(DecoratedKey key, boolean updateStats)
    {
        return delegate.getCachedPosition(key, updateStats);
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    protected RowIndexEntry getCachedPosition(KeyCacheKey unifiedKey, boolean updateStats)
    {
        return delegate.getCachedPosition(unifiedKey, updateStats);
    }

<<<<<<< HEAD
    public boolean isKeyCacheEnabled()
    {
        return delegate.isKeyCacheEnabled();
    }

=======
    @Override
    protected RowIndexEntry getPosition(PartitionPosition key, Operator op, boolean updateCacheAndStats, boolean permitMatchPastLast, SSTableReadsListener listener)
    {
        return delegate.getPosition(key, op, updateCacheAndStats, permitMatchPastLast, listener);
    }

    @Override
    public UnfilteredRowIterator iterator(DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reversed, boolean isForThrift, SSTableReadsListener listener)
    {
        return delegate.iterator(key, slices, selectedColumns, reversed, isForThrift, listener);
    }

    @Override
    public UnfilteredRowIterator iterator(FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry, Slices slices, ColumnFilter selectedColumns, boolean reversed, boolean isForThrift)
    {
        return delegate.iterator(file, key, indexEntry, slices, selectedColumns, reversed, isForThrift);
    }

    @Override
    public UnfilteredRowIterator simpleIterator(FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry, boolean tombstoneOnly)
    {
        return delegate.simpleIterator(file, key, indexEntry, tombstoneOnly);
    }

    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public DecoratedKey firstKeyBeyond(PartitionPosition token)
    {
        return delegate.firstKeyBeyond(token);
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public long uncompressedLength()
    {
        return delegate.uncompressedLength();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public long onDiskLength()
    {
        return delegate.onDiskLength();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public double getCrcCheckChance()
    {
        return delegate.getCrcCheckChance();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public void setCrcCheckChance(double crcCheckChance)
    {
        delegate.setCrcCheckChance(crcCheckChance);
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public void markObsolete(Runnable tidier)
    {
        delegate.markObsolete(tidier);
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public boolean isMarkedCompacted()
    {
        return delegate.isMarkedCompacted();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public void markSuspect()
    {
        delegate.markSuspect();
    }

<<<<<<< HEAD
    public void unmarkSuspect()
    {
        delegate.unmarkSuspect();
    }

    public boolean isMarkedSuspect()
    {
        return delegate.isMarkedSuspect();
    }

    public ISSTableScanner getScanner(Range<Token> range)
    {
        return delegate.getScanner(range);
    }

    public FileDataInput getFileDataInput(long position)
    {
        return delegate.getFileDataInput(position);
    }

    public boolean newSince(long age)
    {
        return delegate.newSince(age);
    }

    public void createLinks(String snapshotDirectoryPath)
    {
        delegate.createLinks(snapshotDirectoryPath);
    }

    public boolean isRepaired()
    {
        return delegate.isRepaired();
    }

    public DecoratedKey keyAt(long indexPosition) throws IOException
    {
        return delegate.keyAt(indexPosition);
    }

    public boolean isPendingRepair()
    {
        return delegate.isPendingRepair();
    }

    public UUID getPendingRepair()
    {
        return delegate.getPendingRepair();
    }

    public long getRepairedAt()
    {
        return delegate.getRepairedAt();
    }

    public boolean isTransient()
    {
        return delegate.isTransient();
    }

    public boolean intersects(Collection<Range<Token>> ranges)
    {
        return delegate.intersects(ranges);
    }

=======
    @Override
    public boolean isMarkedSuspect()
    {
        return delegate.isMarkedSuspect();
    }

    @Override
    public ISSTableScanner getScanner()
    {
        return delegate.getScanner();
    }

    @Override
    public ISSTableScanner getScanner(ColumnFilter columns, DataRange dataRange, boolean isForThrift, SSTableReadsListener listener)
    {
        return delegate.getScanner(columns, dataRange, isForThrift, listener);
    }

    @Override
    public ISSTableScanner getScanner(Range<Token> range, RateLimiter limiter)
    {
        return delegate.getScanner(range, limiter);
    }

    @Override
    public ISSTableScanner getScanner(RateLimiter limiter)
    {
        return delegate.getScanner(limiter);
    }

    @Override
    public ISSTableScanner getScanner(Collection<Range<Token>> ranges, RateLimiter limiter)
    {
        return delegate.getScanner(ranges, limiter);
    }

    @Override
    public ISSTableScanner getScanner(Iterator<AbstractBounds<PartitionPosition>> rangeIterator)
    {
        return delegate.getScanner(rangeIterator);
    }

    @Override
    public ISSTableScanner getScanner(ColumnFilter columns, DataRange dataRange, RateLimiter limiter, boolean isForThrift, SSTableReadsListener listener)
    {
        return delegate.getScanner(columns, dataRange, limiter, isForThrift, listener);
    }

    @Override
    public FileDataInput getFileDataInput(long position)
    {
        return delegate.getFileDataInput(position);
    }

    @Override
    public boolean newSince(long age)
    {
        return delegate.newSince(age);
    }

    @Override
    public void createLinks(String snapshotDirectoryPath)
    {
        delegate.createLinks(snapshotDirectoryPath);
    }

    @Override
    public boolean isRepaired()
    {
        return delegate.isRepaired();
    }

    @Override
    public DecoratedKey keyAt(long indexPosition) throws IOException
    {
        return delegate.keyAt(indexPosition);
    }

    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public long getBloomFilterFalsePositiveCount()
    {
        return delegate.getBloomFilterFalsePositiveCount();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public long getRecentBloomFilterFalsePositiveCount()
    {
        return delegate.getRecentBloomFilterFalsePositiveCount();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public long getBloomFilterTruePositiveCount()
    {
        return delegate.getBloomFilterTruePositiveCount();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public long getRecentBloomFilterTruePositiveCount()
    {
        return delegate.getRecentBloomFilterTruePositiveCount();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public InstrumentingCache<KeyCacheKey, RowIndexEntry> getKeyCache()
    {
        return delegate.getKeyCache();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public EstimatedHistogram getEstimatedPartitionSize()
    {
        return delegate.getEstimatedPartitionSize();
    }

<<<<<<< HEAD
    public EstimatedHistogram getEstimatedCellPerPartitionCount()
    {
        return delegate.getEstimatedCellPerPartitionCount();
    }

=======
    @Override
    public EstimatedHistogram getEstimatedColumnCount()
    {
        return delegate.getEstimatedColumnCount();
    }

    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public double getEstimatedDroppableTombstoneRatio(int gcBefore)
    {
        return delegate.getEstimatedDroppableTombstoneRatio(gcBefore);
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public double getDroppableTombstonesBefore(int gcBefore)
    {
        return delegate.getDroppableTombstonesBefore(gcBefore);
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public double getCompressionRatio()
    {
        return delegate.getCompressionRatio();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public long getMinTimestamp()
    {
        return delegate.getMinTimestamp();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public long getMaxTimestamp()
    {
        return delegate.getMaxTimestamp();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public int getMinLocalDeletionTime()
    {
        return delegate.getMinLocalDeletionTime();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public int getMaxLocalDeletionTime()
    {
        return delegate.getMaxLocalDeletionTime();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public boolean mayHaveTombstones()
    {
        return delegate.mayHaveTombstones();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public int getMinTTL()
    {
        return delegate.getMinTTL();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public int getMaxTTL()
    {
        return delegate.getMaxTTL();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public long getTotalColumnsSet()
    {
        return delegate.getTotalColumnsSet();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public long getTotalRows()
    {
        return delegate.getTotalRows();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public int getAvgColumnSetPerRow()
    {
        return delegate.getAvgColumnSetPerRow();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public int getSSTableLevel()
    {
        return delegate.getSSTableLevel();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public void reloadSSTableMetadata() throws IOException
    {
        delegate.reloadSSTableMetadata();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public StatsMetadata getSSTableMetadata()
    {
        return delegate.getSSTableMetadata();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public RandomAccessReader openDataReader(RateLimiter limiter)
    {
        return delegate.openDataReader(limiter);
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public RandomAccessReader openDataReader()
    {
        return delegate.openDataReader();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public RandomAccessReader openIndexReader()
    {
        return delegate.openIndexReader();
    }

<<<<<<< HEAD
=======
    @Override
    public ChannelProxy getDataChannel()
    {
        return delegate.getDataChannel();
    }

    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public ChannelProxy getIndexChannel()
    {
        return delegate.getIndexChannel();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public FileHandle getIndexFile()
    {
        return delegate.getIndexFile();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public long getCreationTimeFor(Component component)
    {
        return delegate.getCreationTimeFor(component);
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public long getKeyCacheHit()
    {
        return delegate.getKeyCacheHit();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public long getKeyCacheRequest()
    {
        return delegate.getKeyCacheRequest();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public void incrementReadCount()
    {
        delegate.incrementReadCount();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public EncodingStats stats()
    {
        return delegate.stats();
    }

<<<<<<< HEAD
    public Ref<SSTableReader> tryRef()
    {
        return selfRef.tryRef();
    }

    public Ref<SSTableReader> selfRef()
    {
        return selfRef;
    }

    public Ref<SSTableReader> ref()
    {
        return selfRef.ref();
    }

=======
    @Override
    public Ref<SSTableReader> tryRef()
    {
        return delegate.tryRef();
    }

    @Override
    public Ref<SSTableReader> selfRef()
    {
        return delegate.selfRef();
    }

    @Override
    public Ref<SSTableReader> ref()
    {
        return delegate.ref();
    }

    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    void setup(boolean trackHotness)
    {
        delegate.setup(trackHotness);
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public void overrideReadMeter(RestorableMeter readMeter)
    {
        delegate.overrideReadMeter(readMeter);
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public void addTo(Ref.IdentityCollection identities)
    {
        delegate.addTo(identities);
    }

<<<<<<< HEAD
    public TableMetadata metadata()
    {
        return delegate.metadata();
    }

=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public IPartitioner getPartitioner()
    {
        return delegate.getPartitioner();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return delegate.decorateKey(key);
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public String getIndexFilename()
    {
        return delegate.getIndexFilename();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public String getColumnFamilyName()
    {
        return delegate.getColumnFamilyName();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public String getKeyspaceName()
    {
        return delegate.getKeyspaceName();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public List<String> getAllFilePaths()
    {
        return delegate.getAllFilePaths();
    }

<<<<<<< HEAD
//    protected long estimateRowsFromIndex(RandomAccessReader ifile) throws IOException
//    {
//        return delegate.estimateRowsFromIndex(ifile);
//    }

=======
    @Override
    protected long estimateRowsFromIndex(RandomAccessReader ifile) throws IOException
    {
        try
        {
            return (Long) ESTIMATE_ROWS_FROM_INDEX.invoke(delegate, ifile);
        }
        catch (IllegalAccessException e)
        {
            throw new AssertionError(e);
        }
        catch (InvocationTargetException e)
        {
            Throwable cause = e.getCause();
            if (cause instanceof IOException)
                throw (IOException) cause;
            if (cause instanceof Error)
                throw (Error) cause;
            if (cause instanceof RuntimeException)
                throw (RuntimeException) cause;
            throw new RuntimeException(cause);
        }
    }

    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public long bytesOnDisk()
    {
        return delegate.bytesOnDisk();
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    public String toString()
    {
        return delegate.toString();
    }

<<<<<<< HEAD
    public AbstractBounds<Token> getBounds()
    {
        return delegate.getBounds();
    }

    public ChannelProxy getDataChannel()
    {
        return delegate.getDataChannel();
    }
}
=======
    @Override
    public void addComponents(Collection<Component> newComponents)
    {
        delegate.addComponents(newComponents);
    }
}
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
