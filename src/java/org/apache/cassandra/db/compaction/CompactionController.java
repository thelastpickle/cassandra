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
package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataTracker;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.AlwaysPresentFilter;

import org.apache.cassandra.utils.concurrent.Refs;

/**
 * Manage compaction options.
 */
public class CompactionController implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionController.class);
    private static final String NEVER_PURGE_TOMBSTONES_PROPERTY = Config.PROPERTY_PREFIX + "never_purge_tombstones";
    static final boolean NEVER_PURGE_TOMBSTONES = Boolean.getBoolean(NEVER_PURGE_TOMBSTONES_PROPERTY);

    public final ColumnFamilyStore cfs;

    // note that overlappingTree and overlappingSSTables will be null if NEVER_PURGE_TOMBSTONES is set - this is a
    // good thing so that noone starts using them and thinks that if overlappingSSTables is empty, there
    // is no overlap.
    private DataTracker.SSTableIntervalTree overlappingTree;
    private Refs<SSTableReader> overlappingSSTables;
    private final Iterable<SSTableReader> compacting;

    public final int gcBefore;

    protected CompactionController(ColumnFamilyStore cfs, int maxValue)
    {
        this(cfs, null, maxValue);
    }

    public CompactionController(ColumnFamilyStore cfs, Set<SSTableReader> compacting, int gcBefore)
    {
        assert cfs != null;
        this.cfs = cfs;
        this.gcBefore = gcBefore;
        this.compacting = compacting;
        refreshOverlaps();
        if (NEVER_PURGE_TOMBSTONES)
            logger.warn("You are running with -Dcassandra.never_purge_tombstones=true, this is dangerous!");
    }

    void maybeRefreshOverlaps()
    {
        if (NEVER_PURGE_TOMBSTONES)
        {
            logger.debug("not refreshing overlaps - running with -D{}=true", NEVER_PURGE_TOMBSTONES_PROPERTY);
            return;
        }

        if (ignoreOverlaps())
        {
            logger.debug("not refreshing overlaps - running with ignoreOverlaps activated");
            return;
        }

        for (SSTableReader reader : overlappingSSTables)
        {
            if (reader.isMarkedCompacted())
            {
                refreshOverlaps();
                return;
            }
        }
    }

    private void refreshOverlaps()
    {
        if (NEVER_PURGE_TOMBSTONES)
            return;

        if (this.overlappingSSTables != null)
            overlappingSSTables.release();

        if (compacting == null || ignoreOverlaps())
            overlappingSSTables = Refs.tryRef(Collections.<SSTableReader>emptyList());
        else
            overlappingSSTables = cfs.getAndReferenceOverlappingSSTables(compacting);
        this.overlappingTree = DataTracker.buildIntervalTree(overlappingSSTables);
    }

    public Set<SSTableReader> getFullyExpiredSSTables()
    {
        return getFullyExpiredSSTables(cfs, compacting, overlappingSSTables, gcBefore, ignoreOverlaps());
    }

    /**
     * Finds expired sstables
     *
     * works something like this;
     * 1. find "global" minTimestamp of overlapping sstables and compacting sstables containing any non-expired data
     * 2. build a list of fully expired candidates
     * 3. check if the candidates to be dropped actually can be dropped (maxTimestamp < global minTimestamp)
     *    - if not droppable, remove from candidates
     * 4. return candidates.
     *
     * @param cfStore
     * @param compacting we take the drop-candidates from this set, it is usually the sstables included in the compaction
     * @param overlapping the sstables that overlap the ones in compacting.
     * @param gcBefore
     * @param ignoreOverlaps don't check if data shadows/overlaps any data in other sstables
     * @return
     */
    public static Set<SSTableReader> getFullyExpiredSSTables(ColumnFamilyStore cfStore,
                                                             Iterable<SSTableReader> compacting,
                                                             Iterable<SSTableReader> overlapping,
                                                             int gcBefore,
                                                             boolean ignoreOverlaps)
    {
        logger.debug("Checking droppable sstables in {}", cfStore);

        if (NEVER_PURGE_TOMBSTONES || compacting == null)
            return Collections.emptySet();

        if (ignoreOverlaps)
        {
            Set<SSTableReader> fullyExpired = new HashSet<>();
            for (SSTableReader candidate : compacting)
            {
                if (candidate.getSSTableMetadata().maxLocalDeletionTime < gcBefore)
                {
                    fullyExpired.add(candidate);
                    logger.trace("Dropping overlap ignored expired SSTable {} (maxLocalDeletionTime={}, gcBefore={})",
                                 candidate, candidate.getSSTableMetadata().maxLocalDeletionTime, gcBefore);
                }
            }
            return fullyExpired;
        }

        List<SSTableReader> candidates = new ArrayList<>();
        long minTimestamp = Long.MAX_VALUE;

        for (SSTableReader sstable : overlapping)
        {
            // Overlapping might include fully expired sstables. What we care about here is
            // the min timestamp of the overlapping sstables that actually contain live data.
            if (sstable.getSSTableMetadata().maxLocalDeletionTime >= gcBefore)
                minTimestamp = Math.min(minTimestamp, sstable.getMinTimestamp());
        }

        for (SSTableReader candidate : compacting)
        {
            if (candidate.getSSTableMetadata().maxLocalDeletionTime < gcBefore)
                candidates.add(candidate);
            else
                minTimestamp = Math.min(minTimestamp, candidate.getMinTimestamp());
        }

        // At this point, minTimestamp denotes the lowest timestamp of any relevant
        // SSTable that contains a constructive value. candidates contains all the
        // candidates with no constructive values. The ones out of these that have
        // (getMaxTimestamp() < minTimestamp) serve no purpose anymore.

        Iterator<SSTableReader> iterator = candidates.iterator();
        while (iterator.hasNext())
        {
            SSTableReader candidate = iterator.next();
            if (candidate.getMaxTimestamp() >= minTimestamp)
            {
                iterator.remove();
            }
            else
            {
               logger.debug("Dropping expired SSTable {} (maxLocalDeletionTime={}, gcBefore={})",
                        candidate, candidate.getSSTableMetadata().maxLocalDeletionTime, gcBefore);
            }
        }
        return new HashSet<>(candidates);
    }

    public static Set<SSTableReader> getFullyExpiredSSTables(ColumnFamilyStore cfStore,
                                                             Iterable<SSTableReader> compacting,
                                                             Iterable<SSTableReader> overlapping,
                                                             int gcBefore)
    {
        return getFullyExpiredSSTables(cfStore, compacting, overlapping, gcBefore, false);
    }

    public String getKeyspace()
    {
        return cfs.keyspace.getName();
    }

    public String getColumnFamily()
    {
        return cfs.name;
    }

    /**
     * @return the largest timestamp before which it's okay to drop tombstones for the given partition;
     * i.e., after the maxPurgeableTimestamp there may exist newer data that still needs to be suppressed
     * in other sstables.  This returns the minimum timestamp for any SSTable that contains this partition and is not
     * participating in this compaction, or LONG.MAX_VALUE if no such SSTable exists.
     */
    public long maxPurgeableTimestamp(DecoratedKey key)
    {
        if (NEVER_PURGE_TOMBSTONES)
            return Long.MIN_VALUE;

        List<SSTableReader> filteredSSTables = overlappingTree.search(key);
        long min = Long.MAX_VALUE;
        for (SSTableReader sstable : filteredSSTables)
        {
            // if we don't have bloom filter(bf_fp_chance=1.0 or filter file is missing),
            // we check index file instead.
            if (sstable.getBloomFilter() instanceof AlwaysPresentFilter && sstable.getPosition(key, SSTableReader.Operator.EQ, false) != null)
                min = Math.min(min, sstable.getMinTimestamp());
            else if (sstable.getBloomFilter().isPresent(key.getKey()))
                min = Math.min(min, sstable.getMinTimestamp());
        }
        return min;
    }

    public void close()
    {
        if (overlappingSSTables != null)
            overlappingSSTables.release();
    }

    /**
     * Is overlapped sstables ignored
     *
     * Control whether or not we are taking into account overlapping sstables when looking for fully expired sstables.
     * In order to reduce the amount of work needed, we look for sstables that can be dropped instead of compacted.
     * As a safeguard mechanism, for each time range of data in a sstable, we are checking globally to see if all data
     * of this time range is fully expired before considering to drop the sstable.
     * This strategy can retain for a long time a lot of sstables on disk (see CASSANDRA-13418) so this option
     * control whether or not this check should be ignored.
     *
     * @return false by default
     */
    protected boolean ignoreOverlaps()
    {
        return false;
    }
}
