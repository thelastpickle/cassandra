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

import java.io.Closeable;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ScannerList;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.db.compaction.CompactionHistoryTabularData.COMPACTION_TYPE_PROPERTY;
import static org.apache.cassandra.db.compaction.CompactionManager.compactionRateLimiterAcquire;
import static org.apache.cassandra.utils.FBUtilities.now;
import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemory;
import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemoryPerSecond;

public class CompactionTask extends AbstractCompactionTask
{
    protected static final Logger logger = LoggerFactory.getLogger(CompactionTask.class);
    protected final long gcBefore;
    protected final boolean keepOriginals;
    /** for trace logging purposes only */
    private static final AtomicLong totalBytesCompacted = new AtomicLong();

    // The compaction strategy is not necessarily available for all compaction tasks (e.g. GC or sstable splitting)
    @Nullable
    private final CompactionStrategy strategy;

    public CompactionTask(CompactionRealm realm,
                          LifecycleTransaction txn,
                          long gcBefore,
                          boolean keepOriginals,
                          @Nullable CompactionStrategy strategy)
    {
        super(realm, txn);
        this.gcBefore = gcBefore;
        this.keepOriginals = keepOriginals;
        this.strategy = strategy;

        if (strategy != null)
            addObserver(strategy);

        logger.debug("Created compaction task with id {} and strategy {}", txn.opId(), strategy);
    }

    /**
     * Create a compaction task without a compaction strategy, currently only called by tests.
     */
    static AbstractCompactionTask forTesting(CompactionRealm realm, LifecycleTransaction txn, long gcBefore)
    {
        return new CompactionTask(realm, txn, gcBefore, false, null);
    }

    /**
     * Create a compaction task for deleted data collection.
     */
    public static AbstractCompactionTask forGarbageCollection(CompactionRealm realm, LifecycleTransaction txn, long gcBefore, CompactionParams.TombstoneOption tombstoneOption)
    {
        AbstractCompactionTask task = new CompactionTask(realm, txn, gcBefore, false, null)
        {
            @Override
            protected CompactionController getCompactionController(Set<SSTableReader> toCompact)
            {
                return new CompactionController(realm, toCompact, gcBefore, null, tombstoneOption);
            }

            @Override
            protected int getLevel()
            {
                return txn.onlyOne().getSSTableLevel();
            }
        };
        task.setUserDefined(true);
        task.setCompactionType(OperationType.GARBAGE_COLLECT);
        return task;
    }

    private static long addToTotalBytesCompacted(long bytesCompacted)
    {
        return totalBytesCompacted.addAndGet(bytesCompacted);
    }

    @Override
    protected int executeInternal()
    {
        run();
        return transaction.originals().size();
    }

    /*
     *  Find the maximum size file in the list .
     */
    private SSTableReader getMaxSizeFile(Iterable<SSTableReader> sstables)
    {
        long maxSize = 0L;
        SSTableReader maxFile = null;
        for (SSTableReader sstable : sstables)
        {
            if (sstable.onDiskLength() > maxSize)
            {
                maxSize = sstable.onDiskLength();
                maxFile = sstable;
            }
        }
        return maxFile;
    }

    @VisibleForTesting
    public boolean reduceScopeForLimitedSpace(Set<SSTableReader> nonExpiredSSTables, long expectedSize)
    {
        if (partialCompactionsAcceptable() && transaction.originals().size() > 1)
        {
            // Try again w/o the largest one.
            SSTableReader removedSSTable = getMaxSizeFile(nonExpiredSSTables);
            logger.warn("insufficient space to compact all requested files. {}MiB required, {} for compaction {} - removing largest SSTable: {}",
                        (float) expectedSize / 1024 / 1024,
                        StringUtils.join(transaction.originals(), ", "),
                        transaction.opId(),
                        removedSSTable);
            // Note that we have removed files that are still marked as compacting.
            // This suboptimal but ok since the caller will unmark all the sstables at the end.
            transaction.cancel(removedSSTable);
            return true;
        }
        return false;
    }

    /**
     * For internal use and testing only.  The rest of the system should go through the submit* methods,
     * which are properly serialized.
     * Caller is in charge of marking/unmarking the sstables as compacting.
     */
    @Override
    protected void runMayThrow() throws Exception
    {
        // The collection of sstables passed may be empty (but not null); even if
        // it is not empty, it may compact down to nothing if all rows are deleted.
        assert transaction != null;

        if (transaction.originals().isEmpty())
            return;

        if (DatabaseDescriptor.isSnapshotBeforeCompaction())
        {
            Instant creationTime = now();
            realm.snapshotWithoutMemtable(creationTime.toEpochMilli() + "-compact-" + realm.getTableName(), creationTime);
        }

        try (CompactionController controller = getCompactionController(transaction.originals());
             CompactionOperation operation = new CompactionOperation(controller, strategy))
        {
            operation.execute();
        }
    }

    /**
     *  The compaction operation is a special case of an {@link AbstractTableOperation} and takes care of executing the
     *  actual compaction and releasing any resources when the compaction is finished.
     *  <p/>
     *  This class also extends {@link AbstractTableOperation} for reporting compaction-specific progress information.
     */
    public final class CompactionOperation implements AutoCloseable
    {
        private final CompactionController controller;
        private final Set<CompactionSSTable> fullyExpiredSSTables;
        private final TimeUUID taskId;
        private final RateLimiter limiter;
        private final long startNanos;
        private final long startTime;
        private final Set<SSTableReader> actuallyCompact;
        private final CompactionProgress progress;

        // resources that are updated and may be read by another thread
        private volatile Collection<SSTableReader> newSStables;
        private volatile long totalKeysWritten;
        private volatile long estimatedKeys;

        // resources that are updated but only read by this thread
        private boolean completed;

        // resources that need closing
        private Refs<SSTableReader> sstableRefs;
        private ScannerList scanners;
        private CompactionIterator compactionIterator;
        private TableOperation op;
        private Closeable obsCloseable;
        private CompactionAwareWriter writer;

        /**
         * Create a new compaction operation.
         * <p/>
         *
         * @param controller the compaction controller is needed by the scanners and compaction iterator to manage options
         */
        private CompactionOperation(CompactionController controller, CompactionStrategy strategy)
        {
            this.controller = controller;

            this.fullyExpiredSSTables = controller.getFullyExpiredSSTables();
            this.taskId = transaction.opId();

            TimeUUID taskId = transaction.opId();
            // select SSTables to compact based on available disk space.
            if (!buildCompactionCandidatesForAvailableDiskSpace(fullyExpiredSSTables, taskId))
            {
                // The set of sstables has changed (one or more were excluded due to limited available disk space).
                // We need to recompute the overlaps between sstables.
                controller.maybeRefreshOverlaps();
            }

            // sanity check: all sstables must belong to the same table
            assert !Iterables.any(transaction.originals(), sstable -> !sstable.descriptor.cfname.equals(realm.getTableName()));

            this.limiter = CompactionManager.instance.getRateLimiter();
            this.startNanos = Clock.Global.nanoTime();
            this.startTime = Clock.Global.currentTimeMillis();
            this.actuallyCompact = Sets.difference(transaction.originals(), fullyExpiredSSTables);
            this.progress = new Progress();
            this.newSStables = Collections.emptyList();
            this.totalKeysWritten = 0;
            this.estimatedKeys = 0;
            this.completed = false;

            Directories dirs = getDirectories();

            try
            {
                // resources that need closing, must be created last in case of exceptions and released if there is an exception in the c.tor
                this.sstableRefs = Refs.ref(actuallyCompact);
                this.scanners = strategy != null ? strategy.getScanners(actuallyCompact)
                                                 : ScannerList.of(actuallyCompact, null);
                this.compactionIterator = new CompactionIterator(compactionType, scanners.scanners, controller, FBUtilities.nowInSeconds(), taskId);
                this.op = compactionIterator.getOperation();
                this.writer = getCompactionAwareWriter(realm, dirs, transaction, actuallyCompact);
                this.obsCloseable = opObserver.onOperationStart(op);

                compObservers.forEach(obs -> obs.onInProgress(progress));
            }
            catch (Throwable t)
            {
                t = Throwables.close(t, obsCloseable, writer, compactionIterator, scanners, sstableRefs); // ok to close even if null

                Throwables.maybeFail(t);
            }
        }

        private void execute()
        {
            try
            {
                execute0();
            }
            catch (Throwable t)
            {
                Throwables.maybeFail(onError(t));
            }
        }

        private void execute0()
        {
            // new sstables from flush can be added during a compaction, but only the compaction can remove them,
            // so in our single-threaded compaction world this is a valid way of determining if we're compacting
            // all the sstables (that existed when we started)
            if (logger.isDebugEnabled())
            {
                debugLogCompactingMessage(taskId);
            }

            long lastCheckObsoletion = startNanos;
            double compressionRatio = scanners.getCompressionRatio();
            if (compressionRatio == MetadataCollector.NO_COMPRESSION_RATIO)
                compressionRatio = 1.0;

            long lastBytesScanned = 0;

            if (!controller.realm.isCompactionActive())
                throw new CompactionInterruptedException(op.getProgress());

            estimatedKeys = writer.estimatedKeys();
            while (compactionIterator.hasNext())
            {
                if (op.isStopRequested())
                    throw new CompactionInterruptedException(op.getProgress());

                UnfilteredRowIterator partition = compactionIterator.next();
                if (writer.append(partition))
                    totalKeysWritten++;

                long bytesScanned = scanners.getTotalBytesScanned();

                // Rate limit the scanners, and account for compression
                if (compactionRateLimiterAcquire(limiter, bytesScanned, lastBytesScanned, compressionRatio))
                    lastBytesScanned = bytesScanned;

                long now = Clock.Global.nanoTime();
                if (now - lastCheckObsoletion > TimeUnit.MINUTES.toNanos(1L))
                {
                    controller.maybeRefreshOverlaps();
                    lastCheckObsoletion = now;
                }
            }

            // point of no return
            newSStables = writer.finish();


            completed = true;
        }

        private Throwable onError(Throwable e)
        {
            if (e instanceof AssertionError)
            {
                // Add additional information to help operators.
                AssertionError error = new AssertionError(
                String.format("Illegal input has been generated, most probably due to corruption in the input sstables\n" +
                              "\t%s\n" +
                              "Try scrubbing the sstables by running\n" +
                              "\tnodetool scrub %s %s\n",
                              transaction.originals(),
                              realm.getKeyspaceName(),
                              realm.getTableName()));
                error.addSuppressed(e);
                return error;
            }

            return e;
        }

        //
        // Closeable
        //

        @Override
        public void close()
        {
            Throwable err = Throwables.close((Throwable) null, obsCloseable, writer, compactionIterator, scanners, sstableRefs);

            if (transaction.isOffline())
                return;

            if (completed)
            {
                updateCompactionHistory(taskId, realm.getKeyspaceName(), realm.getTableName(), progress, ImmutableMap.of(COMPACTION_TYPE_PROPERTY, compactionType.type));
                CompactionManager.instance.incrementRemovedExpiredSSTables(fullyExpiredSSTables.size());
                if (transaction.originals().size() > 0 && actuallyCompact.size() == 0)
                    // this CompactionOperation only deleted fully expired SSTables without compacting anything
                    CompactionManager.instance.incrementDeleteOnlyCompactions();

                if (logger.isDebugEnabled())
                    debugLogCompactionSummaryInfo(taskId, Clock.Global.nanoTime() - startNanos, totalKeysWritten, newSStables, progress);
                if (logger.isTraceEnabled())
                    traceLogCompactionSummaryInfo(totalKeysWritten, estimatedKeys, progress);

                if (strategy != null)
                    strategy.getCompactionLogger().compaction(startTime,
                                                              transaction.originals(),
                                                              Clock.Global.currentTimeMillis(),
                                                              newSStables);

                // update the metrics
                realm.metrics().incBytesCompacted(progress.adjustedInputDiskSize(),
                                                  progress.outputDiskSize(),
                                                  Clock.Global.nanoTime() - startNanos);
            }

            Throwables.maybeFail(err);
        }


        //
        // CompactionProgress
        //

        private final class Progress implements CompactionProgress
        {
            //
            // TableOperation.Progress methods
            //

            @Override
            public Optional<String> keyspace()
            {
                return Optional.of(metadata().keyspace);
            }

            @Override
            public Optional<String> table()
            {
                return Optional.of(metadata().name);
            }

            @Override
            public TableMetadata metadata()
            {
                return realm.metadata();
            }

            /**
             * @return the number of bytes read by the compaction iterator. For compressed or encrypted sstables,
             * this is the number of bytes processed by the iterator after decompression, so this is the current
             * position in the uncompressed sstable files.
             */
            @Override
            public long completed()
            {
                return compactionIterator.bytesRead();
            }

            /**
             * @return the initial number of bytes for input sstables. For compressed or encrypted sstables,
             * this is the number of bytes after decompression, so this is the uncompressed length of sstable files.
             */
            public long total()
            {
                return compactionIterator.totalBytes();
            }

            @Override
            public OperationType operationType()
            {
                return compactionType;
            }

            @Override
            public TimeUUID operationId()
            {
                return taskId;
            }

            @Override
            public TableOperation.Unit unit()
            {
                return TableOperation.Unit.BYTES;
            }

            @Override
            public Set<SSTableReader> sstables()
            {
                return transaction.originals();
            }

            //
            // CompactionProgress
            //

            @Override
            @Nullable
            public CompactionStrategy strategy()
            {
                return CompactionTask.this.strategy;
            }

            @Override
            public boolean isStopRequested()
            {
                return op.isStopRequested();
            }

            @Override
            public Collection<SSTableReader> inSSTables()
            {
                // TODO should we use transaction.originals() and include the expired sstables?
                // This would be more correct but all the metrics we get from CompactionIterator will not be compatible
                return actuallyCompact;
            }

            @Override
            public Collection<SSTableReader> outSSTables()
            {
                return newSStables;
            }

            @Override
            public long inputDiskSize()
            {
                return CompactionSSTable.getTotalBytes(actuallyCompact);
            }

            @Override
            public long inputUncompressedSize()
            {
                return compactionIterator.totalBytes();
            }

            @Override
            public long adjustedInputDiskSize()
            {
                return scanners.getTotalCompressedSize();
            }

            @Override
            public long outputDiskSize()
            {
                return CompactionSSTable.getTotalBytes(newSStables);
            }

            @Override
            public long uncompressedBytesRead()
            {
                return compactionIterator.bytesRead();
            }

            @Override
            public long uncompressedBytesRead(int level)
            {
                return compactionIterator.bytesRead(level);
            }

            @Override
            public long uncompressedBytesWritten()
            {
                return writer.bytesWritten();
            }

            @Override
            public long durationInNanos()
            {
                return Clock.Global.nanoTime() - startNanos;
            }

            @Override
            public long partitionsRead()
            {
                return compactionIterator.totalSourcePartitions();
            }

            @Override
            public long rowsRead()
            {
                return compactionIterator.totalSourceRows();
            }

            @Override
            public long[] partitionsHistogram()
            {
                return compactionIterator.mergedPartitionsHistogram();
            }

            @Override
            public long[] rowsHistogram()
            {
                return compactionIterator.mergedRowsHistogram();
            }

            @Override
            public double sizeRatio()
            {
                long estInputSizeBytes = adjustedInputDiskSize();
                if (estInputSizeBytes > 0)
                    return outputDiskSize() / (double) estInputSizeBytes;

                // this is a valid case, when there are no sstables to actually compact
                // the previous code would return a NaN that would be logged as zero
                return 0;
            }
        }
    }

    @Override
    public CompactionAwareWriter getCompactionAwareWriter(CompactionRealm realm,
                                                          Directories directories,
                                                          LifecycleTransaction transaction,
                                                          Set<SSTableReader> nonExpiredSSTables)
    {
        return new DefaultCompactionWriter(realm, directories, transaction, nonExpiredSSTables, keepOriginals, getLevel());
    }

    public static String updateCompactionHistory(TimeUUID taskId, String keyspaceName, String columnFamilyName, long[] mergedRowCounts, long startSize, long endSize, Map<String, String> compactionProperties)
    {
        StringBuilder mergeSummary = new StringBuilder(mergedRowCounts.length * 10);
        Map<Integer, Long> mergedRows = new HashMap<>();
        for (int i = 0; i < mergedRowCounts.length; i++)
        {
            long count = mergedRowCounts[i];
            if (count == 0)
                continue;

            int rows = i + 1;
            mergeSummary.append(String.format("%d:%d, ", rows, count));
            mergedRows.put(rows, count);
        }
        SystemKeyspace.updateCompactionHistory(taskId, keyspaceName, columnFamilyName, Clock.Global.currentTimeMillis(), startSize, endSize, mergedRows, compactionProperties);
        return mergeSummary.toString();
    }

    protected Directories getDirectories()
    {
        return realm.getDirectories();
    }

    public static long getMinRepairedAt(Set<SSTableReader> actuallyCompact)
    {
        long minRepairedAt= Long.MAX_VALUE;
        for (SSTableReader sstable : actuallyCompact)
            minRepairedAt = Math.min(minRepairedAt, sstable.getSSTableMetadata().repairedAt);
        if (minRepairedAt == Long.MAX_VALUE)
            return ActiveRepairService.UNREPAIRED_SSTABLE;
        return minRepairedAt;
    }

    public static TimeUUID getPendingRepair(Set<SSTableReader> sstables)
    {
        if (sstables.isEmpty())
        {
            return ActiveRepairService.NO_PENDING_REPAIR;
        }
        Set<TimeUUID> ids = new HashSet<>();
        for (SSTableReader sstable: sstables)
            ids.add(sstable.getSSTableMetadata().pendingRepair);

        if (ids.size() != 1)
            throw new RuntimeException(String.format("Attempting to compact pending repair sstables with sstables from other repair, or sstables not pending repair: %s", ids));

        return ids.iterator().next();
    }

    public static boolean getIsTransient(Set<SSTableReader> sstables)
    {
        if (sstables.isEmpty())
        {
            return false;
        }

        boolean isTransient = sstables.iterator().next().isTransient();

        if (!Iterables.all(sstables, sstable -> sstable.isTransient() == isTransient))
        {
            throw new RuntimeException("Attempting to compact transient sstables with non transient sstables");
        }

        return isTransient;
    }


    /**
     * Checks if we have enough disk space to execute the compaction.  Drops the largest sstable out of the Task until
     * there's enough space (in theory) to handle the compaction.
     *
     * @return true if there is enough disk space to execute the complete compaction, false if some sstables are excluded.
     */
    protected boolean buildCompactionCandidatesForAvailableDiskSpace(final Set<CompactionSSTable> fullyExpiredSSTables, TimeUUID taskId)
    {
        if(!realm.isCompactionDiskSpaceCheckEnabled() && compactionType == OperationType.COMPACTION)
        {
            logger.info("Compaction space check is disabled - trying to compact all sstables");
            return true;
        }

        final Set<SSTableReader> nonExpiredSSTables = Sets.difference(transaction.originals(), fullyExpiredSSTables);
        int sstablesRemoved = 0;

        while(!nonExpiredSSTables.isEmpty())
        {
            // Only consider write size of non expired SSTables
            long writeSize;
            try
            {
                writeSize = realm.getExpectedCompactedFileSize(nonExpiredSSTables, compactionType);
                Map<File, Long> expectedNewWriteSize = new HashMap<>();
                List<File> newCompactionDatadirs = realm.getDirectoriesForFiles(nonExpiredSSTables); // TODO use realm instead of cfs
                long writeSizePerOutputDatadir = writeSize / Math.max(newCompactionDatadirs.size(), 1);
                for (File directory : newCompactionDatadirs)
                    expectedNewWriteSize.put(directory, writeSizePerOutputDatadir);

                Map<File, Long> expectedWriteSize = CompactionManager.instance.active.estimatedRemainingWriteBytes();

                // todo: abort streams if they block compactions
                if (realm.getDirectories().hasDiskSpaceForCompactionsAndStreams(expectedNewWriteSize, expectedWriteSize))
                    break;
            }
            catch (Exception e)
            {
                logger.error("Could not check if there is enough disk space for compaction {}", taskId, e);
                break;
            }

            if (!reduceScopeForLimitedSpace(nonExpiredSSTables, writeSize))
            {
                // we end up here if we can't take any more sstables out of the compaction.
                // usually means we've run out of disk space

                // but we can still compact expired SSTables
                if(partialCompactionsAcceptable() && fullyExpiredSSTables.size() > 0 )
                {
                    // sanity check to make sure we compact only fully expired SSTables.
                    assert transaction.originals().equals(fullyExpiredSSTables);
                    break;
                }

                String msg = String.format("Not enough space for compaction (%s) of %s.%s, estimated sstables = %d, expected write size = %d",
                                           taskId,
                                           realm.getKeyspaceName(),
                                           realm.getTableName(),
                                           Math.max(1, writeSize / strategy.getMaxSSTableBytes()),
                                           writeSize);
                logger.warn(msg);
                CompactionManager.instance.incrementAborted();
                throw new RuntimeException(msg);
            }

            sstablesRemoved++;
            logger.warn("Not enough space for compaction {}, {}MiB estimated. Reducing scope.",
                        taskId, (float) writeSize / 1024 / 1024);
        }

        if(sstablesRemoved > 0)
        {
            CompactionManager.instance.incrementCompactionsReduced();
            CompactionManager.instance.incrementSstablesDropppedFromCompactions(sstablesRemoved);
            return false;
        }
        return true;
    }

    protected int getLevel()
    {
        return 0;
    }

    protected CompactionController getCompactionController(Set<SSTableReader> toCompact)
    {
        return new CompactionController(realm, toCompact, gcBefore);
    }

    protected boolean partialCompactionsAcceptable()
    {
        return !isUserDefined;
    }

    public static long getMaxDataAge(Collection<SSTableReader> sstables)
    {
        long max = 0;
        for (SSTableReader sstable : sstables)
        {
            if (sstable.maxDataAge > max)
                max = sstable.maxDataAge;
        }
        return max;
    }

    private void debugLogCompactionSummaryInfo(TimeUUID taskId,
                                               long durationInNano,
                                               long totalKeysWritten,
                                               Collection<SSTableReader> newSStables,
                                               CompactionProgress progress)
    {
        // log a bunch of statistics about the result and save to system table compaction_history
        long dTime = TimeUnit.NANOSECONDS.toMillis(durationInNano);

        long totalMergedPartitions = 0;
        long[] mergedPartitionCounts = progress.partitionsHistogram();
        StringBuilder mergeSummary = new StringBuilder(mergedPartitionCounts.length * 10);
        mergeSummary.append('{');
        for (int i = 0; i < mergedPartitionCounts.length; i++)
        {
            long mergedPartitionCount = mergedPartitionCounts[i];
            if (mergedPartitionCount != 0)
            {
                totalMergedPartitions += mergedPartitionCount * (i + 1);
                mergeSummary.append(i).append(':').append(mergedPartitionCount).append(", ");
            }
        }
        mergeSummary.append('}');

        StringBuilder newSSTableNames = new StringBuilder(newSStables.size() * 100);
        for (SSTableReader reader : newSStables)
            newSSTableNames.append(reader.descriptor.baseFileUri()).append(",");
        logger.debug("Compacted ({}) {} sstables to [{}] to level={}. {} to {} (~{}% of original) in {}ms. " +
                     "Read Throughput = {}, Write Throughput = {}, Row Throughput = ~{}/s, Partition Throughput = ~{}/s." +
                     " {} total partitions merged to {}. Partition merge counts were {}.",
                     taskId,
                     transaction.originals().size(),
                     newSSTableNames.toString(),
                     getLevel(),
                     prettyPrintMemory(progress.adjustedInputDiskSize()),
                     prettyPrintMemory(progress.outputDiskSize()),
                     (int) (progress.sizeRatio() * 100),
                     dTime,
                     prettyPrintMemoryPerSecond(progress.adjustedInputDiskSize(), durationInNano),
                     prettyPrintMemoryPerSecond(progress.outputDiskSize(), durationInNano),
                     progress.rowsRead() / (TimeUnit.NANOSECONDS.toSeconds(durationInNano) + 1),
                     (int) progress.partitionsRead() / (TimeUnit.NANOSECONDS.toSeconds(progress.durationInNanos()) + 1),
                     totalMergedPartitions,
                     totalKeysWritten,
                     mergeSummary.toString());
    }

    private void debugLogCompactingMessage(TimeUUID taskId)
    {
        Set<SSTableReader> originals = transaction.originals();
        StringBuilder ssTableLoggerMsg = new StringBuilder(originals.size() * 100);
        ssTableLoggerMsg.append("Compacting (").append(taskId).append(')').append(" [");
        for (SSTableReader sstr : originals)
        {
            ssTableLoggerMsg.append(sstr.getFilename())
                            .append(":level=")
                            .append(sstr.getSSTableLevel())
                            .append(", ");
        }
        ssTableLoggerMsg.append("]");

        logger.debug(ssTableLoggerMsg.toString());
    }


    private static void updateCompactionHistory(TimeUUID id,
                                                String keyspaceName,
                                                String columnFamilyName,
                                                CompactionProgress progress,
                                                Map<String, String> compactionProperties)
    {
        long[] mergedPartitionsHistogram = progress.partitionsHistogram();
        Map<Integer, Long> mergedPartitions = new HashMap<>(mergedPartitionsHistogram.length);
        for (int i = 0; i < mergedPartitionsHistogram.length; i++)
        {
            long count = mergedPartitionsHistogram[i];
            if (count == 0)
                continue;

            int rows = i + 1;
            mergedPartitions.put(rows, count);
        }
        SystemKeyspace.updateCompactionHistory(id,
                                               keyspaceName,
                                               columnFamilyName,
                                               Clock.Global.currentTimeMillis(),
                                               progress.adjustedInputDiskSize(),
                                               progress.outputDiskSize(),
                                               mergedPartitions,
                                               compactionProperties);
    }

    private void traceLogCompactionSummaryInfo(long totalKeysWritten,
                                               long estimatedKeys,
                                               CompactionProgress progress)
    {
        logger.trace("CF Total Bytes Compacted: {}", prettyPrintMemory(addToTotalBytesCompacted(progress.outputDiskSize())));
        logger.trace("Actual #keys: {}, Estimated #keys:{}, Err%: {}",
                     totalKeysWritten,
                     estimatedKeys,
                     ((double) (totalKeysWritten - estimatedKeys) / totalKeysWritten));
    }
}
