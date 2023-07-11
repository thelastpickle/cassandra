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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.Util.dk;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

//TODO This test needs rethinking because we always now end up with a single segment after a flush
// and we are not restricted to Integer.MAX_VALUE in the segments
public class SegmentFlushTest
{
    private static long segmentRowIdOffset;
    private static int minSegmentRowId;
    private static int maxSegmentRowId;
    private static PrimaryKey minKey;
    private static PrimaryKey maxKey;
    private static ByteBuffer minTerm;
    private static ByteBuffer maxTerm;
    private static int numRowsPerSegment;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void reset()
    {
        SegmentBuilder.updateLastValidSegmentRowId(-1); // reset
    }

    @Test
    public void testFlushBetweenRowIds() throws Exception
    {
        // exceeds max rowId per segment
        testFlushBetweenRowIds(0, Integer.MAX_VALUE, 2);
        testFlushBetweenRowIds(0, Long.MAX_VALUE, 2);
        testFlushBetweenRowIds(0, SegmentBuilder.LAST_VALID_SEGMENT_ROW_ID + 1, 2);
        testFlushBetweenRowIds(Integer.MAX_VALUE - SegmentBuilder.LAST_VALID_SEGMENT_ROW_ID - 1, Integer.MAX_VALUE, 2);
        testFlushBetweenRowIds(Long.MAX_VALUE - SegmentBuilder.LAST_VALID_SEGMENT_ROW_ID - 1, Long.MAX_VALUE, 2);
    }

    @Test
    public void testNoFlushBetweenRowIds() throws Exception
    {
        // not exceeds max rowId per segment
        testFlushBetweenRowIds(0, SegmentBuilder.LAST_VALID_SEGMENT_ROW_ID, 1);
        testFlushBetweenRowIds(Long.MAX_VALUE - SegmentBuilder.LAST_VALID_SEGMENT_ROW_ID, Long.MAX_VALUE - 1, 1);
    }

    private void testFlushBetweenRowIds(long sstableRowId1, long sstableRowId2, int segments) throws Exception
    {
        Path tmpDir = Files.createTempDirectory("SegmentFlushTest");
        IndexDescriptor indexDescriptor = IndexDescriptor.create(new Descriptor(new File(tmpDir.toFile()), "ks", "cf", new SequenceBasedSSTableId(1)),
                                                                 Murmur3Partitioner.instance,
                                                                 SAITester.EMPTY_COMPARATOR);

        ColumnMetadata column = ColumnMetadata.regularColumn("sai", "internal", "column", UTF8Type.instance);
        IndexMetadata config = IndexMetadata.fromSchemaMetadata("index_name", IndexMetadata.Kind.CUSTOM, null);

        IndexContext indexContext = new IndexContext("ks",
                                                     "cf",
                                                     UTF8Type.instance,
                                                     new ClusteringComparator(),
                                                     column,
                                                     IndexTarget.Type.SIMPLE,
                                                     config,
                                                     MockSchema.newCFS("ks"));

        SSTableIndexWriter writer = new SSTableIndexWriter(indexDescriptor, indexContext, V1OnDiskFormat.SEGMENT_BUILD_MEMORY_LIMITER, () -> true);

        List<DecoratedKey> keys = Arrays.asList(dk("1"), dk("2"));
        Collections.sort(keys);

        DecoratedKey key1 = keys.get(0);
        ByteBuffer term1 = UTF8Type.instance.decompose("a");
        Row row1 = createRow(column, term1);
        writer.addRow(SAITester.TEST_FACTORY.create(key1, Clustering.EMPTY), row1, sstableRowId1);

        // expect a flush if exceed max rowId per segment
        DecoratedKey key2 = keys.get(1);
        ByteBuffer term2 = UTF8Type.instance.decompose("b");
        Row row2 = createRow(column, term2);
        writer.addRow(SAITester.TEST_FACTORY.create(key2, Clustering.EMPTY), row2, sstableRowId2);

        writer.complete(Stopwatch.createStarted());

        MetadataSource source = MetadataSource.loadColumnMetadata(indexDescriptor, indexContext);

        // verify segment count
        List<SegmentMetadata> segmentMetadatas = SegmentMetadata.load(source, indexDescriptor.primaryKeyFactory);
        assertEquals(segments, segmentMetadatas.size());

        // verify segment metadata
        SegmentMetadata segmentMetadata = segmentMetadatas.get(0);
        segmentRowIdOffset = sstableRowId1; // segmentRowIdOffset is the first sstable row id
        minSegmentRowId = 0;
        maxSegmentRowId = segments == 1 ? (int) (sstableRowId2 - segmentRowIdOffset) : 0;
        minKey = SAITester.TEST_FACTORY.createTokenOnly(key1.getToken());
        maxKey = SAITester.TEST_FACTORY.createTokenOnly(segments == 1 ? key2.getToken() : key1.getToken());
        minTerm = term1;
        maxTerm = segments == 1 ? term2 : term1;
        numRowsPerSegment = segments == 1 ? 2 : 1;
        verifySegmentMetadata(segmentMetadata);
        verifyStringIndex(indexDescriptor, indexContext, segmentMetadata);

        // verify 2nd segment
        if (segments > 1)
        {
            Preconditions.checkState(segments == 2);
            segmentRowIdOffset = sstableRowId2;
            minSegmentRowId = 0;
            maxSegmentRowId = 0;
            minKey = SAITester.TEST_FACTORY.createTokenOnly(key2.getToken());
            maxKey = SAITester.TEST_FACTORY.createTokenOnly(key2.getToken());
            minTerm = term2;
            maxTerm = term2;
            numRowsPerSegment = 1;

            segmentMetadata = segmentMetadatas.get(1);
            verifySegmentMetadata(segmentMetadata);
            verifyStringIndex(indexDescriptor, indexContext, segmentMetadata);
        }
    }

    private void verifyStringIndex(IndexDescriptor indexDescriptor, IndexContext indexContext, SegmentMetadata segmentMetadata) throws IOException
    {
        FileHandle termsData = indexDescriptor.createPerIndexFileHandle(IndexComponent.TERMS_DATA, indexContext);
        FileHandle postingLists = indexDescriptor.createPerIndexFileHandle(IndexComponent.POSTING_LISTS, indexContext);

        long termsFooterPointer = Long.parseLong(segmentMetadata.componentMetadatas.get(IndexComponent.TERMS_DATA).attributes.get(SAICodecUtils.FOOTER_POINTER));

        try (TermsReader reader = new TermsReader(indexContext,
                                                  termsData,
                                                  postingLists,
                                                  segmentMetadata.componentMetadatas.get(IndexComponent.TERMS_DATA).root,
                                                  termsFooterPointer))
        {
            TermsIterator iterator = reader.allTerms(0);
            assertEquals(minTerm, iterator.getMinTerm());
            assertEquals(maxTerm, iterator.getMaxTerm());

            verifyTermPostings(iterator, minTerm, minSegmentRowId, minSegmentRowId);

            if (numRowsPerSegment > 1)
            {
                verifyTermPostings(iterator, maxTerm, maxSegmentRowId, maxSegmentRowId);
            }

            assertFalse(iterator.hasNext());
        }
    }

    private void verifyTermPostings(TermsIterator iterator, ByteBuffer expectedTerm, int minSegmentRowId, int maxSegmentRowId) throws IOException
    {
        ByteComparable term = iterator.next();
        PostingList postings = iterator.postings();

        assertEquals(0, ByteComparable.compare(term, ByteComparable.fixedLength(expectedTerm), ByteComparable.Version.OSS50));
        assertEquals(minSegmentRowId == maxSegmentRowId ? 1 : 2, postings.size());
    }

    private void verifySegmentMetadata(SegmentMetadata segmentMetadata)
    {
        assertEquals(segmentRowIdOffset, segmentMetadata.minSSTableRowId);
        assertEquals(minKey, segmentMetadata.minKey);
        assertEquals(maxKey, segmentMetadata.maxKey);
        assertEquals(minTerm, segmentMetadata.minTerm);
        assertEquals(maxTerm, segmentMetadata.maxTerm);
        assertEquals(numRowsPerSegment, segmentMetadata.numRows);
    }

    private Row createRow(ColumnMetadata column, ByteBuffer value)
    {
        Row.Builder builder1 = BTreeRow.sortedBuilder();
        builder1.newRow(Clustering.EMPTY);
        builder1.addCell(BufferCell.live(column, 0, value));
        return builder1.build();
    }

    private void assertOverflow(long sstableRowId1, long sstableRowId2) throws Exception
    {
        try
        {
            testFlushBetweenRowIds(sstableRowId1, sstableRowId2, 0);
            fail("Expect integer overflow, but didn't");
        }
        catch (ArithmeticException e)
        {
            assertTrue(e.getMessage().contains("integer overflow"));
        }
    }

    private void assertIllegalEndOfStream(long sstableRowId1, long sstableRowId2) throws Exception
    {
        try
        {
            testFlushBetweenRowIds(sstableRowId1, sstableRowId2, 0);
            fail("Expect integer overflow, but didn't");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage().contains("END_OF_STREAM"));
        }
    }
}
