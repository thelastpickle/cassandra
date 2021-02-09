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
package org.apache.cassandra.db;

import java.io.IOException;

import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.util.DataInputPlus;

/**
 * Helper class to deserialize Unfiltered object from disk efficiently.
 *
 * More precisely, this class is used by the low-level reader to ensure
 * we don't do more work than necessary (i.e. we don't allocate/deserialize
 * objects for things we don't care about).
 */
public class UnfilteredDeserializer
{
    protected final TableMetadata metadata;
    protected final DataInputPlus in;
    protected final DeserializationHelper helper;

    private final ClusteringPrefix.Deserializer clusteringDeserializer;
    private final SerializationHeader header;

    private int nextFlags;
    private int nextExtendedFlags;
    private boolean isReady;
    private boolean isDone;

    private final Row.Builder builder;

    private UnfilteredDeserializer(TableMetadata metadata,
                                   DataInputPlus in,
                                   SerializationHeader header,
                                   DeserializationHelper helper)
    {
        this.metadata = metadata;
        this.in = in;
        this.helper = helper;
        this.header = header;
        this.clusteringDeserializer = new ClusteringPrefix.Deserializer(metadata.comparator, in, header);
        this.builder = BTreeRow.sortedBuilder();
    }

    public static UnfilteredDeserializer create(TableMetadata metadata,
                                                DataInputPlus in,
                                                SerializationHeader header,
                                                DeserializationHelper helper)
    {
        return new UnfilteredDeserializer(metadata, in, header, helper);
    }

    /**
     * Whether or not there is more atom to read.
     */
    public boolean hasNext() throws IOException
    {
        if (isReady)
            return true;

        prepareNext();
        return !isDone;
    }

    private void prepareNext() throws IOException
    {
        if (isDone)
            return;

        nextFlags = in.readUnsignedByte();
        if (UnfilteredSerializer.isEndOfPartition(nextFlags))
        {
            isDone = true;
            isReady = false;
            return;
        }

        nextExtendedFlags = UnfilteredSerializer.readExtendedFlags(in, nextFlags);

        clusteringDeserializer.prepare(nextFlags, nextExtendedFlags);
        isReady = true;
    }

    /**
     * Compare the provided bound to the next atom to read on disk.
     *
     * This will not read/deserialize the whole atom but only what is necessary for the
     * comparison. Whenever we know what to do with this atom (read it or skip it),
     * readNext or skipNext should be called.
     */
    public int compareNextTo(ClusteringBound bound) throws IOException
    {
        if (!isReady)
            prepareNext();

        assert !isDone;

        return clusteringDeserializer.compareNextTo(bound);
    }

    /**
     * Returns whether the next atom is a row or not.
     */
    public boolean nextIsRow() throws IOException
    {
        if (!isReady)
            prepareNext();

        return UnfilteredSerializer.kind(nextFlags) == Unfiltered.Kind.ROW;
    }

    /**
     * Returns the next atom.
     */
    public Unfiltered readNext() throws IOException
    {
        isReady = false;
        if (UnfilteredSerializer.kind(nextFlags) == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
        {
            ClusteringBoundOrBoundary bound = clusteringDeserializer.deserializeNextBound();
            return UnfilteredSerializer.serializer.deserializeMarkerBody(in, header, bound);
        }
        else
        {
            builder.newRow(clusteringDeserializer.deserializeNextClustering());
            return UnfilteredSerializer.serializer.deserializeRowBody(in, header, helper, nextFlags, nextExtendedFlags, builder);
        }
    }

    /**
     * Clears any state in this deserializer.
     */
    public void clearState()
    {
        isReady = false;
        isDone = false;
    }

    /**
     * Skips the next atom.
     */
    public void skipNext() throws IOException
    {
        isReady = false;
        clusteringDeserializer.skipNext();
        if (UnfilteredSerializer.kind(nextFlags) == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
        {
            UnfilteredSerializer.serializer.skipMarkerBody(in);
        }
        else
        {
<<<<<<< HEAD
            UnfilteredSerializer.serializer.skipRowBody(in);
=======
            private final AtomIterator atoms;
            private final LegacyLayout.CellGrouper grouper;
            private final TombstoneTracker tombstoneTracker;
            private final CFMetaData metadata;
            private final SerializationHelper helper;

            private Unfiltered next;

            UnfilteredIterator(CFMetaData metadata,
                               DeletionTime partitionDeletion,
                               SerializationHelper helper,
                               Supplier<LegacyLayout.LegacyAtom> atomReader)
            {
                this.metadata = metadata;
                this.helper = helper;
                this.grouper = new LegacyLayout.CellGrouper(metadata, helper);
                this.tombstoneTracker = new TombstoneTracker(partitionDeletion);
                this.atoms = new AtomIterator(atomReader, metadata);
            }


            public boolean hasNext()
            {
                // Note that we loop on next == null because TombstoneTracker.openNew() could return null below or the atom might be shadowed.
                while (next == null)
                {
                    if (atoms.hasNext())
                    {
                        // If there is a range tombstone to open strictly before the next row/RT, we need to return that open (or boundary) marker first.
                        if (tombstoneTracker.hasOpeningMarkerBefore(atoms.peek()))
                        {
                            next = tombstoneTracker.popOpeningMarker();
                        }
                        // If a range tombstone closes strictly before the next row/RT, we need to return that close (or boundary) marker first.
                        else if (tombstoneTracker.hasClosingMarkerBefore(atoms.peek()))
                        {
                            next = tombstoneTracker.popClosingMarker();
                        }
                        else
                        {
                            LegacyLayout.LegacyAtom atom = atoms.next();
                            if (tombstoneTracker.isShadowed(atom))
                                continue;

                            if (atom.isRowAtom(metadata))
                                next = readRow(atom);
                            else
                                tombstoneTracker.openNew(atom.asRangeTombstone());
                        }
                    }
                    else if (tombstoneTracker.hasOpenTombstones())
                    {
                        next = tombstoneTracker.popMarker();
                    }
                    else
                    {
                        return false;
                    }
                }
                return true;
            }

            private Unfiltered readRow(LegacyLayout.LegacyAtom first)
            {
                LegacyLayout.CellGrouper grouper = first.isStatic()
                                                 ? LegacyLayout.CellGrouper.staticGrouper(metadata, helper)
                                                 : this.grouper;
                grouper.reset();
                // We know the first atom is not shadowed and is a "row" atom, so can be added blindly.
                grouper.addAtom(first);

                // We're less sure about the next atoms. In particular, CellGrouper want to make sure we only pass it
                // "row" atoms (it's the only type it knows how to handle) so we should handle anything else.
                while (atoms.hasNext())
                {
                    // Peek, but don't consume the next atom just yet
                    LegacyLayout.LegacyAtom atom = atoms.peek();
                    // First, that atom may be shadowed in which case we can simply ignore it. Note that this handles
                    // the case of repeated RT start marker after we've crossed an index boundary, which could well
                    // appear in the middle of a row (CASSANDRA-14008).
                    if (!tombstoneTracker.hasClosingMarkerBefore(atom) && tombstoneTracker.isShadowed(atom))
                    {
                        atoms.next(); // consume the atom since we only peeked it so far
                        continue;
                    }

                    // Second, we should only pass "row" atoms to the cell grouper
                    if (atom.isRowAtom(metadata))
                    {
                        if (!grouper.addAtom(atom))
                            break; // done with the row; don't consume the atom
                        atoms.next(); // the grouper "accepted" the atom, consume it since we only peeked above
                    }
                    else
                    {
                        LegacyLayout.LegacyRangeTombstone rt = (LegacyLayout.LegacyRangeTombstone) atom;
                        // This means we have a non-row range tombstone. Unfortunately, that does not guarantee the
                        // current row is finished (though it may), because due to the logic within LegacyRangeTombstone
                        // constructor, we can get an out-of-order RT that includes on the current row (even if it is
                        // already started) and extends past it.

                        // So first, evacuate the easy case of the range tombstone simply starting after the current
                        // row, in which case we're done with the current row (but don't consume the new RT yet so it
                        // gets handled as any other non-row RT).
                        if (grouper.startsAfterCurrentRow(rt))
                            break;

                        // Otherwise, we "split" the RT in 2: the part covering the current row, which is now an
                        // inRowAtom and can be passed to the grouper, and the part after that, which we push back into
                        // the iterator for later processing.
                        Clustering currentRow = grouper.currentRowClustering();
                        atoms.next(); // consume since we had only just peeked it so far and we're using it
                        atoms.pushOutOfOrder(rt.withNewStart(ClusteringBound.exclusiveStartOf(currentRow)));
                        // Note: in theory the withNewStart is a no-op here, but not taking any risk
                        grouper.addAtom(rt.withNewStart(ClusteringBound.inclusiveStartOf(currentRow))
                                          .withNewEnd(ClusteringBound.inclusiveEndOf(currentRow)));
                    }
                }

                return grouper.getRow();
            }

            public Unfiltered next()
            {
                if (!hasNext())
                    throw new UnsupportedOperationException();
                Unfiltered toReturn = next;
                next = null;
                return toReturn;
            }

            public Unfiltered peek()
            {
                if (!hasNext())
                    throw new UnsupportedOperationException();
                return next;
            }

            public void clearState()
            {
                atoms.clearState();
                tombstoneTracker.clearState();
                next = null;
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            // Wraps the input of the deserializer to provide an iterator (and skip shadowed atoms).
            // Note: this could use guava AbstractIterator except that we want to be able to clear
            // the internal state of the iterator so it's cleaner to do it ourselves.
            private static class AtomIterator implements PeekingIterator<LegacyLayout.LegacyAtom>
            {
                private final Supplier<LegacyLayout.LegacyAtom> atomReader;
                private boolean readerExhausted;
                private LegacyLayout.LegacyAtom next;

                private final Comparator<LegacyLayout.LegacyAtom> atomComparator;
                // May temporarily store atoms that needs to be handler later than when they were deserialized.
                // Lazily initialized since it is used infrequently.
                private Queue<LegacyLayout.LegacyAtom> outOfOrderAtoms;

                private AtomIterator(Supplier<LegacyLayout.LegacyAtom> atomReader, CFMetaData metadata)
                {
                    this.atomReader = atomReader;
                    this.atomComparator = LegacyLayout.legacyAtomComparator(metadata);
                }

                public boolean hasNext()
                {
                    if (readerExhausted)
                        return hasOutOfOrderAtoms(); // We have to return out of order atoms when reader exhausts

                    // Note that next() and peek() assumes that next has been set by this method, so we do it even if
                    // we have some outOfOrderAtoms stacked up.
                    if (next == null)
                        next = atomReader.get();

                    readerExhausted = next == null;
                    return !readerExhausted || hasOutOfOrderAtoms();
                }

                public LegacyLayout.LegacyAtom next()
                {
                    if (!hasNext())
                        throw new UnsupportedOperationException();

                    if (hasOutOrderAtomBeforeNext())
                        return outOfOrderAtoms.poll();

                    LegacyLayout.LegacyAtom toReturn = next;
                    next = null;
                    return toReturn;
                }

                private boolean hasOutOfOrderAtoms()
                {
                    return outOfOrderAtoms != null && !outOfOrderAtoms.isEmpty();
                }

                private boolean hasOutOrderAtomBeforeNext()
                {
                    // Note that if outOfOrderAtoms is null, the first condition will be false, so we can save a null
                    // check on calling `outOfOrderAtoms.peek()` in the right branch.
                    return hasOutOfOrderAtoms()
                           && (next == null || atomComparator.compare(outOfOrderAtoms.peek(), next) <= 0);
                }

                public LegacyLayout.LegacyAtom peek()
                {
                    if (!hasNext())
                        throw new UnsupportedOperationException();
                    if (hasOutOrderAtomBeforeNext())
                        return outOfOrderAtoms.peek();
                    return next;
                }

                /**
                 * Push back an atom in the iterator assuming said atom sorts strictly _after_ the atom returned by
                 * the last next() call (meaning the pushed atom fall in the part of the iterator that has not been
                 * returned yet, not before). The atom will then be returned by the iterator in proper order.
                 */
                public void pushOutOfOrder(LegacyLayout.LegacyAtom atom)
                {
                    if (outOfOrderAtoms == null)
                        outOfOrderAtoms = new PriorityQueue<>(atomComparator);
                    outOfOrderAtoms.offer(atom);
                }

                public void clearState()
                {
                    this.next = null;
                    this.readerExhausted = false;
                    if (outOfOrderAtoms != null)
                        outOfOrderAtoms.clear();
                }

                public void remove()
                {
                    throw new UnsupportedOperationException();
                }
            }

            /**
             * Tracks which range tombstones are open when deserializing the old format.
             * <p>
             * This is a bit tricky because in the old of format we could have duplicated tombstones, overlapping ones,
             * shadowed ones, etc.., but we should generate from that a "flat" output where at most one non-shadoowed
             * range is open at any given time and without empty range.
             * <p>
             * One consequence of that is that we have to be careful to not generate markers too soon. For instance,
             * we might get a range tombstone [1, 1]@3 followed by [1, 10]@5. So if we generate an opening marker on
             * the first tombstone (so INCL_START(1)@3), we're screwed when we get to the 2nd range tombstone: we really
             * should ignore the first tombstone in that that and generate INCL_START(1)@5 (assuming obviously we don't
             * have one more range tombstone starting at 1 in the stream). This is why we have the
             * {@link #hasOpeningMarkerBefore} method: in practice, we remember when a marker should be opened, but only
             * generate that opening marker when we're sure that we won't get anything shadowing that marker.
             * <p>
             * For closing marker, we also have a {@link #hasClosingMarkerBefore} because in the old format the closing
             * markers comes with the opening one, but we should generate them "in order" in the new format.
             */
            private class TombstoneTracker
            {
                private final DeletionTime partitionDeletion;

                // As explained in the javadoc, we need to wait to generate an opening marker until we're sure we have
                // seen anything that could shadow it. So this remember a marker that needs to be opened but hasn't
                // been yet. This is truly returned when hasOpeningMarkerBefore tells us it's safe to.
                private RangeTombstoneMarker openMarkerToReturn;

                // Open tombstones sorted by their closing bound (i.e. first tombstone is the first to close).
                // As we only track non-fully-shadowed ranges, the first range is necessarily the currently
                // open tombstone (the one with the higher timestamp).
                private final SortedSet<LegacyLayout.LegacyRangeTombstone> openTombstones;

                public TombstoneTracker(DeletionTime partitionDeletion)
                {
                    this.partitionDeletion = partitionDeletion;
                    this.openTombstones = new TreeSet<>((rt1, rt2) -> metadata.comparator.compare(rt1.stop.bound, rt2.stop.bound));
                }

                /**
                 * Checks if the provided atom is fully shadowed by the open tombstones of this tracker (or the partition deletion).
                 */
                public boolean isShadowed(LegacyLayout.LegacyAtom atom)
                {
                    assert !hasClosingMarkerBefore(atom);
                    long timestamp = atom.isCell() ? atom.asCell().timestamp : atom.asRangeTombstone().deletionTime.markedForDeleteAt();

                    if (partitionDeletion.deletes(timestamp))
                        return true;

                    SortedSet<LegacyLayout.LegacyRangeTombstone> coveringTombstones = atom.isRowAtom(metadata) ? openTombstones : openTombstones.tailSet(atom.asRangeTombstone());
                    return Iterables.any(coveringTombstones, tombstone -> tombstone.deletionTime.deletes(timestamp));
                }

                /**
                 * Whether there is an outstanding opening marker that should be returned before we process the provided row/RT.
                 */
                public boolean hasOpeningMarkerBefore(LegacyLayout.LegacyAtom atom)
                {
                    return openMarkerToReturn != null
                           && metadata.comparator.compare(openMarkerToReturn.openBound(false), atom.clustering()) < 0;
                }

                public Unfiltered popOpeningMarker()
                {
                    assert openMarkerToReturn != null;
                    Unfiltered toReturn = openMarkerToReturn;
                    openMarkerToReturn = null;
                    return toReturn;
                }

                /**
                 * Whether the currently open marker closes stricly before the provided row/RT.
                 */
                public boolean hasClosingMarkerBefore(LegacyLayout.LegacyAtom atom)
                {
                    return !openTombstones.isEmpty()
                           && metadata.comparator.compare(openTombstones.first().stop.bound, atom.clustering()) < 0;
                }

                /**
                 * Returns the unfiltered corresponding to closing the currently open marker (and update the tracker accordingly).
                 */
                public Unfiltered popClosingMarker()
                {
                    assert !openTombstones.isEmpty();

                    Iterator<LegacyLayout.LegacyRangeTombstone> iter = openTombstones.iterator();
                    LegacyLayout.LegacyRangeTombstone first = iter.next();
                    iter.remove();

                    // If that was the last open tombstone, we just want to close it. Otherwise, we have a boundary with the
                    // next tombstone
                    if (!iter.hasNext())
                        return new RangeTombstoneBoundMarker(first.stop.bound, first.deletionTime);

                    LegacyLayout.LegacyRangeTombstone next = iter.next();
                    return RangeTombstoneBoundaryMarker.makeBoundary(false, first.stop.bound, first.stop.bound.invert(), first.deletionTime, next.deletionTime);
                }

                 /**
                  * Pop whatever next marker needs to be popped. This should be called as many time as necessary (until
                  * {@link #hasOpenTombstones} returns {@false}) when all atoms have been consumed to "empty" the tracker.
                  */
                 public Unfiltered popMarker()
                 {
                     assert hasOpenTombstones();
                     return openMarkerToReturn == null ? popClosingMarker() : popOpeningMarker();
                 }

                /**
                 * Update the tracker given the provided newly open tombstone. This potentially update openMarkerToReturn
                 * to account for th new opening.
                 *
                 * Note that this method assumes that:
                 +  1) the added tombstone is not fully shadowed: !isShadowed(tombstone).
                 +  2) there is no marker to open that open strictly before this new tombstone: !hasOpeningMarkerBefore(tombstone).
                 +  3) no opened tombstone closes before that tombstone: !hasClosingMarkerBefore(tombstone).
                 + One can check that this is only called after the condition above have been checked in UnfilteredIterator.hasNext above.
                 */
                public void openNew(LegacyLayout.LegacyRangeTombstone tombstone)
                {
                    if (openTombstones.isEmpty())
                    {
                        // If we have an openMarkerToReturn, the corresponding RT must be in openTombstones (or we wouldn't know when to close it)
                        assert openMarkerToReturn == null;
                        openTombstones.add(tombstone);
                        openMarkerToReturn = new RangeTombstoneBoundMarker(tombstone.start.bound, tombstone.deletionTime);
                        return;
                    }

                    if (openMarkerToReturn != null)
                    {
                        // If the new opening supersedes the one we're about to return, we need to update the one to return.
                        if (tombstone.deletionTime.supersedes(openMarkerToReturn.openDeletionTime(false)))
                            openMarkerToReturn = openMarkerToReturn.withNewOpeningDeletionTime(false, tombstone.deletionTime);
                    }
                    else
                    {
                        // We have no openMarkerToReturn set yet so set it now if needs be.
                        // Since openTombstones isn't empty, it means we have a currently ongoing deletion. And if the new tombstone
                        // supersedes that ongoing deletion, we need to close the opening  deletion and open with the new one.
                        DeletionTime currentOpenDeletion = openTombstones.first().deletionTime;
                        if (tombstone.deletionTime.supersedes(currentOpenDeletion))
                            openMarkerToReturn = RangeTombstoneBoundaryMarker.makeBoundary(false, tombstone.start.bound.invert(), tombstone.start.bound, currentOpenDeletion, tombstone.deletionTime);
                    }

                    // In all cases, we know !isShadowed(tombstone) so we need to add the tombstone (note however that we may not have set openMarkerToReturn if the
                    // new tombstone doesn't supersedes the current deletion _but_ extend past the marker currently open)
                    add(tombstone);
                }

                /**
                 * Adds a new tombstone to openTombstones, removing anything that would be shadowed by this new tombstone.
                 */
                private void add(LegacyLayout.LegacyRangeTombstone tombstone)
                {
                    // First, remove existing tombstone that is shadowed by this tombstone.
                    Iterator<LegacyLayout.LegacyRangeTombstone> iter = openTombstones.iterator();
                    while (iter.hasNext())
                    {

                        LegacyLayout.LegacyRangeTombstone existing = iter.next();
                        // openTombstones is ordered by stop bound and the new tombstone can't be shadowing anything that
                        // stop after it.
                        if (metadata.comparator.compare(tombstone.stop.bound, existing.stop.bound) < 0)
                            break;

                        // Note that we remove an existing tombstone even if it is equal to the new one because in that case,
                        // either the existing strictly stops before the new one and we don't want it, or it stops exactly
                        // like the new one but we're going to inconditionally add the new one anyway.
                        if (!existing.deletionTime.supersedes(tombstone.deletionTime))
                            iter.remove();
                    }
                    openTombstones.add(tombstone);
                }

                public boolean hasOpenTombstones()
                {
                    return openMarkerToReturn != null || !openTombstones.isEmpty();
                }

                public void clearState()
                {
                    openMarkerToReturn = null;
                    openTombstones.clear();
                }
            }
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
        }
    }
}
