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

package org.apache.cassandra.index.sai.disk.v2.hnsw;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;

import com.google.common.collect.HashBiMap;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.disk.vector.ConcurrentVectorValues;
import org.apache.cassandra.index.sai.disk.vector.OnDiskOrdinalsMap;
import org.apache.cassandra.index.sai.disk.vector.RamAwareVectorValues;
import org.apache.cassandra.index.sai.disk.vector.VectorPostings;
import org.apache.cassandra.index.sai.disk.vector.VectorPostingsWriter;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class OnDiskOrdinalsMapTest
{

    final private int dimension = 10;

    @BeforeClass
    public static void setup()
    {
        // otherwise "FileHandle fileHandle = builder.complete()" throws
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void testRowIdsMatchOrdinalsSet() throws Exception {
        boolean ordinalsMatchRowIds = createOdomAndGetRowIdsMatchOrdinals(HashBiMap.create());
        assertTrue(ordinalsMatchRowIds);
    }

    @Test
    public void testRowIdsMatchOrdinalsNotSet() throws Exception {
        boolean ordinalsMatchRowIds = createOdomAndGetRowIdsMatchOrdinals(null);
        assertFalse(ordinalsMatchRowIds);
    }

    @Test
    public void testForEachRowidMatching() throws Exception
    {
        testForEach(HashBiMap.create());
    }

    @Test
    public void testForEachFileReading() throws Exception
    {
        testForEach(null);
    }

    private void testForEach(HashBiMap<Integer, Integer> ordinalsMap) throws Exception
    {
        File tempFile = temp("testfile");

        var deletedOrdinals = new HashSet<Integer>();
        RamAwareVectorValues vectorValues = generateVectors(10);

        var postingsMap = generatePostingsMap(vectorValues);

        for (var p: postingsMap.entrySet()) {
            p.getValue().computeRowIds(x -> x);
        }

        PostingsMetadata postingsMd = writePostings(ordinalsMap, tempFile, vectorValues, postingsMap, deletedOrdinals);

        FileHandle.Builder builder = new FileHandle.Builder(tempFile);
        try (FileHandle fileHandle = builder.complete())
        {
            OnDiskOrdinalsMap odom = new OnDiskOrdinalsMap(fileHandle, postingsMd.postingsOffset, postingsMd.postingsLength);

            try (var ordinalsView = odom.getOrdinalsView())
            {
                final AtomicInteger count = new AtomicInteger(0);
                ordinalsView.forEachOrdinalInRange(-100, Integer.MAX_VALUE, (rowId, ordinal) -> {
                    assertTrue(ordinal >= 0);
                    assertTrue(ordinal < vectorValues.size());
                    count.incrementAndGet();
                });
                assertEquals(vectorValues.size(), count.get());
            }

            odom.close();
        }
    }

    private boolean createOdomAndGetRowIdsMatchOrdinals(HashBiMap<Integer, Integer> ordinalsMap) throws Exception
    {
        File tempFile = temp("testfile");

        var deletedOrdinals = new HashSet<Integer>();
        RamAwareVectorValues vectorValues = generateVectors(10);

        var postingsMap = generatePostingsMap(vectorValues);

        for (var p: postingsMap.entrySet()) {
            p.getValue().computeRowIds(x -> x);
        }

        PostingsMetadata postingsMd = writePostings(ordinalsMap, tempFile, vectorValues, postingsMap, deletedOrdinals);

        FileHandle.Builder builder = new FileHandle.Builder(tempFile);
        try (FileHandle fileHandle = builder.complete())
        {
            OnDiskOrdinalsMap odom = new OnDiskOrdinalsMap(fileHandle, postingsMd.postingsOffset, postingsMd.postingsLength);
            boolean rowIdsMatchOrdinals = (boolean) FieldUtils.readField(odom, "rowIdsMatchOrdinals", true);
            odom.close();
            return rowIdsMatchOrdinals;
        }
    }

    private static PostingsMetadata writePostings(HashBiMap<Integer, Integer> ordinalsMap, File tempFile, RamAwareVectorValues vectorValues, Map<float[], VectorPostings<Integer>> postingsMap, HashSet<Integer> deletedOrdinals) throws IOException
    {
        SequentialWriter writer = new SequentialWriter(tempFile,
                                                       SequentialWriterOption.newBuilder().finishOnClose(true).build());

        IntUnaryOperator reverseOrdinalsMapper = ordinalsMap == null
                                                           ? x -> x
                                                           : x -> ordinalsMap.inverse().getOrDefault(x, x);

        long postingsOffset = writer.position();
        long postingsPosition = new VectorPostingsWriter<Integer>(ordinalsMap != null, reverseOrdinalsMapper)
                                    .writePostings(writer, vectorValues, postingsMap, deletedOrdinals);
        long postingsLength = postingsPosition - postingsOffset;

        writer.close();
        PostingsMetadata postingsMd = new PostingsMetadata(postingsOffset, postingsLength);
        return postingsMd;
    }

    private static class PostingsMetadata
    {
        public final long postingsOffset;
        public final long postingsLength;

        public PostingsMetadata(long postingsOffset, long postingsLength)
        {
            this.postingsOffset = postingsOffset;
            this.postingsLength = postingsLength;
        }
    }

    private Map<float[], VectorPostings<Integer>> generatePostingsMap(RamAwareVectorValues vectorValues)
    {
        Map<float[], VectorPostings<Integer>> postingsMap = new ConcurrentSkipListMap<>(Arrays::compare);

        for (int i = 0; i < vectorValues.size(); i++)
        {
            float[] vector = vectorValues.vectorValue(i);
            int ordinal = i;
            var vp = new VectorPostings<>(ordinal);
            vp.setOrdinal(ordinal);
            postingsMap.put(vector, vp);
        }

        return postingsMap;
    }

    private RamAwareVectorValues generateVectors(int totalOrdinals)
    {
        var vectorValues = new ConcurrentVectorValues(dimension);
        for (int i = 0; i < totalOrdinals; i++)
        {
            float[] rawVector = new float[dimension];
            Arrays.fill(rawVector, (float) i);
            vectorValues.add(i, rawVector);
        }
        return vectorValues;
    }

    private static File temp(String id)
    {
        File file = FileUtils.createTempFile(id, "tmp");
        file.deleteOnExit();
        return file;
    }

}