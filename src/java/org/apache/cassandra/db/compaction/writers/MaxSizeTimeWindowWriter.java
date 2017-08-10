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
package org.apache.cassandra.db.compaction.writers;

import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.StreamingHistogram;

public class MaxSizeTimeWindowWriter extends CompactionAwareWriter
{
    private final long[] sstableTimeBoundaries;
    private final Pair<Directories.DataDirectory,SSTableWriter>[] sstableWriters;
    private final Set<SSTableReader> allSSTables;
    private int currentSizeBucket = 0;

    public MaxSizeTimeWindowWriter(ColumnFamilyStore cfs,
                                   Directories directories,
                                   LifecycleTransaction txn,
                                   Set<SSTableReader> nonExpiredSSTables,
                                   long maxSSTableSize)
    {
        this(cfs, directories, txn, nonExpiredSSTables, maxSSTableSize, false);
    }

    public MaxSizeTimeWindowWriter(ColumnFamilyStore cfs,
                                   Directories directories,
                                   LifecycleTransaction txn,
                                   Set<SSTableReader> nonExpiredSSTables,
                                   long maxSSTableSize,
                                   boolean keepOriginals)
    {
        super(cfs, directories, txn, nonExpiredSSTables, keepOriginals);
        this.allSSTables = txn.originals();

        long totalSize = getTotalWriteSize(nonExpiredSSTables, estimatedTotalKeys, cfs, txn.opType());
        int numberSSTables = (int) Math.max(1, Math.ceil(totalSize / maxSSTableSize));
        sstableTimeBoundaries = calculateSSTableTimeBoundaries(nonExpiredSSTables, numberSSTables);
        sstableWriters = new Pair[numberSSTables];        
    }

    private static long[] calculateSSTableTimeBoundaries(Set<SSTableReader> sstables, int numberSSTables)
    {
        long[] sstableTimeBoundaries = new long[numberSSTables];
        long estimatedColumns = sstables.stream().mapToLong(s -> s.getEstimatedColumnCount().count()).sum();
        int minTime = sstables.stream().mapToInt(s -> s.getSSTableMetadata().minLocalDeletionTime).min().getAsInt();
        int maxTime = sstables.stream().mapToInt(s -> s.getSSTableMetadata().maxLocalDeletionTime).min().getAsInt();
        StreamingHistogram histogram = new StreamingHistogram(SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE, SSTable.TOMBSTONE_HISTOGRAM_SPOOL_SIZE, SSTable.TOMBSTONE_HISTOGRAM_TTL_ROUND_SECONDS);
        sstables.stream().forEach(s -> histogram.merge(s.getSSTableMetadata().estimatedTombstoneDropTime));
        long nextColumnCountBoundary = 0;
        for (int i = 0 ; i < numberSSTables ; ++i)
        {
            nextColumnCountBoundary += estimatedColumns / numberSSTables;
            sstableTimeBoundaries[i] = maxTime = binarySearch(histogram, (maxTime - minTime) / 2, maxTime, nextColumnCountBoundary);
        }
        return sstableTimeBoundaries;
    }

    private static int binarySearch(StreamingHistogram histogram, long jump, int cursor, long columnsAtNextBoundary)
    {
        cursor -= jump;
        while (jump > 1)
        {
            double droppableColumns = histogram.sum(cursor);
            if (droppableColumns < columnsAtNextBoundary)
                cursor -= jump;
            else if (droppableColumns > columnsAtNextBoundary)
                cursor += jump;
            else
                break;
            
            jump /= 2;
        }
        return cursor;
    }

    /**
     * Gets the estimated total amount of data to write during compaction
     */
    private static long getTotalWriteSize(Iterable<SSTableReader> nonExpiredSSTables,
                                          long estimatedTotalKeys,
                                          ColumnFamilyStore cfs,
                                          OperationType compactionType)
    {
        long estimatedKeysBeforeCompaction = 0;
        for (SSTableReader sstable : nonExpiredSSTables)
            estimatedKeysBeforeCompaction += sstable.estimatedKeys();
        estimatedKeysBeforeCompaction = Math.max(1, estimatedKeysBeforeCompaction);
        double estimatedCompactionRatio = (double) estimatedTotalKeys / estimatedKeysBeforeCompaction;

        return Math.round(estimatedCompactionRatio * cfs.getExpectedCompactedFileSize(nonExpiredSSTables, compactionType));
    }

    @Override
    protected boolean realAppend(UnfilteredRowIterator partition)
    {
        // within which `sstableTimeBoundaries` bucket in the current TWCS window does this key belong
        // FIXME
        long deleteAtMicro = partition.partitionLevelDeletion().markedForDeleteAt();
        // TODO also check cells
        
        currentSizeBucket = 0;
        
        switchCompactionLocation(sstableWriters[currentSizeBucket].left);
        RowIndexEntry rie = sstableWriter.append(partition);
        return rie != null;
    }

    @Override
    public void switchCompactionLocation(Directories.DataDirectory location)
    {
        if (null == sstableWriters[currentSizeBucket] || !location.equals(sstableWriters[currentSizeBucket].left))
        {
            sstableWriters[currentSizeBucket] = Pair.create(
                    location, 
                    SSTableWriter.create(
                        Descriptor.fromFilename(cfs.getSSTablePath(getDirectories().getLocationForDisk(location))),
                        estimatedTotalKeys / sstableWriters.length,
                        minRepairedAt,
                        cfs.metadata,
                        new MetadataCollector(allSSTables, cfs.metadata.comparator, 0),
                        SerializationHeader.make(cfs.metadata, nonExpiredSSTables),
                        cfs.indexManager.listIndexes(),
                        txn));
        }
        sstableWriter.switchWriter(sstableWriters[currentSizeBucket].right);
    }
}
