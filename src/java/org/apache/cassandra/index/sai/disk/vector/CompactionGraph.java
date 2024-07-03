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

package org.apache.cassandra.index.sai.disk.vector;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.disk.Feature;
import io.github.jbellis.jvector.graph.disk.FeatureId;
import io.github.jbellis.jvector.graph.disk.FusedADC;
import io.github.jbellis.jvector.graph.disk.InlineVectors;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.pq.PQVectors;
import io.github.jbellis.jvector.pq.ProductQuantization;
import io.github.jbellis.jvector.util.Accountable;
import io.github.jbellis.jvector.util.RamUsageEstimator;
import io.github.jbellis.jvector.vector.ArrayByteSequence;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.ByteSequence;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v3.V3OnDiskFormat;
import org.apache.cassandra.index.sai.disk.vector.VectorPostings.CompactionVectorPostings;
import org.apache.cassandra.index.sai.utils.LowPriorityThreadFactory;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.index.sai.disk.v3.V3OnDiskFormat.JVECTOR_2_VERSION;

public class CompactionGraph implements Closeable, Accountable
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionGraph.class);
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    private static final ForkJoinPool compactionFjp = new ForkJoinPool(Runtime.getRuntime().availableProcessors(),
                                                                       new LowPriorityThreadFactory(),
                                                                       null,
                                                                       false);

    private final GraphIndexBuilder builder;
    private final VectorType.VectorSerializer serializer;
    private final VectorSimilarityFunction similarityFunction;
    private final ChronicleMap<VectorFloat<?>, CompactionVectorPostings> postingsMap;
    private final PQVectors pqVectors;
    private final ArrayList<ByteSequence<?>> pqVectorsList;
    private final IndexComponents.ForWrite perIndexComponents;
    private final IndexContext context;
    private final boolean unitVectors;
    private final int postingsEntriesAllocated;
    private final File postingsFile;
    private boolean postingsOneToOne;
    private int nextOrdinal = 0;
    private final ProductQuantization compressor;
    private final OnDiskGraphIndexWriter writer;
    private final long termsOffset;

    public CompactionGraph(IndexComponents.ForWrite perIndexComponents, ProductQuantization compressor, boolean unitVectors, long keyCount) throws IOException
    {
        this.perIndexComponents = perIndexComponents;
        this.context = perIndexComponents.context();
        this.unitVectors = unitVectors;
        var indexConfig = context.getIndexWriterConfig();
        var termComparator = context.getValidator();
        int dimension = ((VectorType<?>) termComparator).dimension;

        // We need to tell Chronicle Map (CM) how many entries to expect.  it's critical not to undercount,
        // or CM will crash.  However, we don't want to just pass in a max entries count of 2B, since it eagerly
        // allocated segments for that many entries, which takes about 25s.
        //
        // If our estimate turns out to be too small, it's not the end of the world, we'll flush this segment
        // and start another to avoid crashing CM.  But we'd rather not do this because the whole goal of
        // CompactionGraph is to write one segment only.
        var dd = perIndexComponents.descriptor();
        var rowsPerKey = Keyspace.open(dd.ksname).getColumnFamilyStore(dd.cfname).getMeanRowsPerPartition();
        long estimatedRows = (long) (1.1 * keyCount * rowsPerKey); // 10% fudge factor
        int maxRowsInGraph = Integer.MAX_VALUE - 100_000; // leave room for a few more async additions until we flush
        postingsEntriesAllocated = estimatedRows > maxRowsInGraph ? maxRowsInGraph : (int) estimatedRows;

        serializer = (VectorType.VectorSerializer) termComparator.getSerializer();
        similarityFunction = indexConfig.getSimilarityFunction();
        postingsOneToOne = true;
        this.compressor = compressor;

        // the extension here is important to signal to CFS.scrubDataDirectories that it should be removed if present at restart
        Component tmpComponent = new Component(Component.Type.CUSTOM, "chronicle" + Descriptor.TMP_EXT);
        postingsFile = dd.fileFor(tmpComponent);
        postingsMap = ChronicleMapBuilder.of((Class<VectorFloat<?>>) (Class) VectorFloat.class, (Class<CompactionVectorPostings>) (Class) CompactionVectorPostings.class)
                                         .averageKeySize(dimension * Float.BYTES)
                                         .averageValueSize(VectorPostings.emptyBytesUsed() + RamUsageEstimator.NUM_BYTES_OBJECT_REF + 2 * Integer.BYTES)
                                         .keyMarshaller(new VectorFloatMarshaller())
                                         .valueMarshaller(new VectorPostings.Marshaller())
                                         .entries(postingsEntriesAllocated)
                                         .createPersistedTo(postingsFile.toJavaIOFile());

        // VSTODO add LVQ
        pqVectorsList = new ArrayList<>(postingsEntriesAllocated);
        pqVectors = new PQVectors(compressor, pqVectorsList);
        builder = new GraphIndexBuilder(BuildScoreProvider.pqBuildScoreProvider(similarityFunction, pqVectors),
                                        dimension,
                                        indexConfig.getAnnMaxDegree(),
                                        indexConfig.getConstructionBeamWidth(),
                                        1.2f,
                                        dimension > 3 ? 1.2f : 1.4f,
                                        compactionFjp, compactionFjp);

        var indexFile = perIndexComponents.addOrGet(IndexComponentType.TERMS_DATA).file();
        termsOffset = (indexFile.exists() ? indexFile.length() : 0)
                      + SAICodecUtils.headerSize();
        var writerBuilder = new OnDiskGraphIndexWriter.Builder(builder.getGraph(), indexFile.toPath())
                            .withStartOffset(termsOffset)
                            .with(new InlineVectors(dimension))
                            .withMapper(new OrdinalMapper.IdentityMapper());
        if (V3OnDiskFormat.WRITE_JVECTOR3_FORMAT)
        {
            writerBuilder = writerBuilder.with(new FusedADC(indexConfig.getMaximumNodeConnections(), compressor));
        }
        else
        {
            writerBuilder = writerBuilder.withVersion(JVECTOR_2_VERSION);
        }
        writer = writerBuilder.build();
        writer.getOutput().seek(indexFile.length()); // position at the end of the previous segment before writing our own header
        SAICodecUtils.writeHeader(SAICodecUtils.toLuceneOutput(writer.getOutput()));
    }

    @Override
    public void close() throws IOException
    {
        // this gets called in finally{} blocks, so use closeQuietly to avoid generating additional exceptions
        FileUtils.closeQuietly(writer);
        FileUtils.closeQuietly(postingsMap);
        Files.delete(postingsFile.toJavaIOFile().toPath());
    }

    public int size()
    {
        return builder.getGraph().size();
    }

    public boolean isEmpty()
    {
        return postingsMap.values().stream().allMatch(VectorPostings::isEmpty);
    }

    /**
     * @return the result of adding the given (vector) term; see {@link InsertionResult}
     */
    public InsertionResult maybeAddVector(ByteBuffer term, Integer key) throws IOException
    {
        assert term != null && term.remaining() != 0;

        var vector = vts.createFloatVector(serializer.deserializeFloatArray(term));
        // Validate the vector.  Since we are compacting, invalid vectors are ignored instead of failing the operation.
        try
        {
            VectorValidation.validateIndexable(vector, similarityFunction);
        }
        catch (InvalidRequestException e)
        {
            if (StorageService.instance.isInitialized())
                logger.trace("Ignoring invalid vector during index build against existing data: {}", (Object) e);
            else
                logger.trace("Ignoring invalid vector during commitlog replay: {}", (Object) e);
            return new InsertionResult(0);
        }

        var bytesUsed = 0L;
        var postings = postingsMap.get(vector);
        if (postings == null)
        {
            // add a new entry
            // this all runs on the same compaction thread, so we don't need to worry about concurrency
            int ordinal = nextOrdinal++;
            postings = new CompactionVectorPostings(ordinal, key);
            postingsMap.put(vector, postings);
            writer.writeInline(ordinal, Feature.singleState(FeatureId.INLINE_VECTORS, new InlineVectors.State(vector)));
            var encoded = (ArrayByteSequence) compressor.encode(vector);
            pqVectorsList.add(encoded);

            bytesUsed += encoded.ramBytesUsed();
            bytesUsed += postings.ramBytesUsed();
            return new InsertionResult(bytesUsed, ordinal, vector);
        }

        // postings list already exists, just add the new key (if it's not already in the list)
        postingsOneToOne = false;
        if (postings.add(key))
            bytesUsed += postings.bytesPerPosting();
        return new InsertionResult(bytesUsed);
    }

    public long addGraphNode(InsertionResult result)
    {
        return builder.addGraphNode(result.ordinal, result.vector);
    }

    public SegmentMetadata.ComponentMetadataMap flush(Set<Integer> deletedOrdinals) throws IOException
    {
        assert deletedOrdinals.isEmpty(); // this is only to provide a consistent api with CassandraOnHeapGraph

        int nInProgress = builder.insertsInProgress();
        assert nInProgress == 0 : String.format("Attempting to write graph while %d inserts are in progress", nInProgress);
        assert nextOrdinal == builder.getGraph().size() : String.format("nextOrdinal %d != graph size %d -- ordinals should be sequential",
                                                                        nextOrdinal, builder.getGraph().size());
        assert pqVectors.count() == builder.getGraph().size() : String.format("vector count %d != graph size %d",
                                                                              pqVectors.count(), builder.getGraph().size());
        assert postingsMap.keySet().size() == builder.getGraph().size() : String.format("postings map entry count %d != vector count %d",
                                                                                        postingsMap.keySet().size(), builder.getGraph().size());
        if (logger.isDebugEnabled())
        {
            logger.debug("Writing graph with {} rows and {} distinct vectors",
                         postingsMap.values().stream().mapToInt(VectorPostings::size).sum(), builder.getGraph().size());
            logger.debug("Estimated size is {} + {}", pqVectors.ramBytesUsed(), builder.getGraph().ramBytesUsed());
        }

        try (var postingsOutput = perIndexComponents.addOrGet(IndexComponentType.POSTING_LISTS).openOutput(true);
             var pqOutput = perIndexComponents.addOrGet(IndexComponentType.PQ).openOutput(true))
        {
            SAICodecUtils.writeHeader(postingsOutput);
            SAICodecUtils.writeHeader(pqOutput);

            // write PQ (time to do this is negligible, don't bother doing it async)
            long pqOffset = pqOutput.getFilePointer();
            CassandraOnHeapGraph.writePqHeader(pqOutput.asSequentialWriter(), unitVectors, VectorCompression.CompressionType.PRODUCT_QUANTIZATION);
            pqVectors.write(pqOutput.asSequentialWriter(), JVECTOR_2_VERSION); // VSTODO old version until we add APQ
            long pqLength = pqOutput.getFilePointer() - pqOffset;

            // write postings asynchronously while we run cleanup().  this requires the index header to be present
            writer.writeHeader();
            long postingsOffset = postingsOutput.getFilePointer();
            var es = Executors.newSingleThreadExecutor(new NamedThreadFactory("CompactionGraphPostingsWriter"));
            var indexHandle = perIndexComponents.get(IndexComponentType.TERMS_DATA).createFileHandle();
            var index = OnDiskGraphIndex.load(indexHandle::createReader, termsOffset);
            var postingsFuture = es.submit(() -> {
                try (var view = index.getView())
                {
                    return new VectorPostingsWriter<Integer>(postingsOneToOne, builder.getGraph().size(), i -> i)
                           .writePostings(postingsOutput.asSequentialWriter(), view, postingsMap, deletedOrdinals);
                }
            });

            // complete internal graph clean up
            builder.cleanup();

            // wait for postings to finish writing and clean up related resources
            long postingsEnd = postingsFuture.get();
            long postingsLength = postingsEnd - postingsOffset;
            es.shutdown();
            index.close();
            indexHandle.close();

            // write the graph edge lists and optionally fused adc features
            var start = System.nanoTime();
            if (writer.getFeatureSet().contains(FeatureId.FUSED_ADC))
            {
                try (var view = builder.getGraph().getView())
                {
                    var supplier = Feature.singleStateFactory(FeatureId.FUSED_ADC, ordinal -> new FusedADC.State(view, pqVectors, ordinal));
                    writer.write(supplier);
                }
            }
            else
            {
                writer.write(Map.of());
            }
            SAICodecUtils.writeFooter(writer.getOutput(), writer.checksum());
            logger.info("Writing graph took {}ms", (System.nanoTime() - start) / 1_000_000);
            long termsLength = writer.getOutput().position() - termsOffset;

            // write remaining footers/checksums
            SAICodecUtils.writeFooter(pqOutput);
            SAICodecUtils.writeFooter(postingsOutput);

            // add components to the metadata map
            return CassandraOnHeapGraph.createMetadataMap(termsOffset, termsLength, postingsOffset, postingsLength, pqOffset, pqLength);
        }
        catch (ExecutionException | InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public long ramBytesUsed()
    {
        return pqVectors.ramBytesUsed() + builder.getGraph().ramBytesUsed();
    }

    public boolean requiresFlush()
    {
        return nextOrdinal >= postingsEntriesAllocated;
    }

    private static class VectorFloatMarshaller implements BytesReader<VectorFloat<?>>, BytesWriter<VectorFloat<?>> {
        @Override
        public void write(Bytes out, VectorFloat<?> vector) {
            out.writeInt(vector.length());
            for (int i = 0; i < vector.length(); i++) {
                out.writeFloat(vector.get(i));
            }
        }

        @Override
        public VectorFloat<?> read(Bytes in, VectorFloat<?> using) {
            int length = in.readInt();
            if (using == null) {
                float[] data = new float[length];
                for (int i = 0; i < length; i++) {
                    data[i] = in.readFloat();
                }
                return vts.createFloatVector(data);
            }

            for (int i = 0; i < length; i++) {
                using.set(i, in.readFloat());
            }
            return using;
        }
    }

    /**
     * AddResult is a container for the result of maybeAddVector.  If this call resulted in a new
     * vector being added to the graph, then `ordinal` and `vector` fields will be populated, otherwise
     * they will be null.
     * <p>
     * bytesUsed is always populated and always non-negative (it will be smaller, but not zero,
     * when adding a vector that already exists in the graph to a new row).
     */
    public static class InsertionResult
    {
        public final long bytesUsed;
        public final Integer ordinal;
        public final VectorFloat<?> vector;

        public InsertionResult(long bytesUsed, Integer ordinal, VectorFloat<?> vector)
        {
            this.bytesUsed = bytesUsed;
            this.ordinal = ordinal;
            this.vector = vector;
        }

        public InsertionResult(long bytesUsed)
        {
            this(bytesUsed, null, null);
        }
    }
}