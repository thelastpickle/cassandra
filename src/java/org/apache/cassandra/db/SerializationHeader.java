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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.exceptions.InvalidColumnTypeException;
import org.apache.cassandra.exceptions.UnknownColumnException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.metadata.IMetadataComponentSerializer;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.AbstractTypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.config.CassandraRelevantProperties.DURATION_IN_MAPS_COMPATIBILITY_MODE;

public class SerializationHeader
{
    private static final Logger logger = LoggerFactory.getLogger(SerializationHeader.class);

    public static final Serializer serializer = new Serializer();

    private final boolean isForSSTable;

    private final AbstractType<?> keyType;
    private final List<AbstractType<?>> clusteringTypes;

    private final RegularAndStaticColumns columns;
    private final EncodingStats stats;

    private final Map<ByteBuffer, AbstractType<?>> typeMap;

    private SerializationHeader(boolean isForSSTable,
                                AbstractType<?> keyType,
                                List<AbstractType<?>> clusteringTypes,
                                RegularAndStaticColumns columns,
                                EncodingStats stats,
                                Map<ByteBuffer, AbstractType<?>> typeMap)
    {
        this.isForSSTable = isForSSTable;
        this.keyType = keyType;
        this.clusteringTypes = clusteringTypes;
        this.columns = columns;
        this.stats = stats;
        this.typeMap = typeMap;
    }

    public static SerializationHeader makeWithoutStats(TableMetadata metadata)
    {
        return new SerializationHeader(true, metadata, metadata.regularAndStaticColumns(), EncodingStats.NO_STATS);
    }

    public static SerializationHeader make(TableMetadata metadata, Collection<SSTableReader> sstables)
    {
        // The serialization header has to be computed before the start of compaction (since it's used to write)
        // the result. This means that when compacting multiple sources, we won't have perfectly accurate stats
        // (for EncodingStats) since compaction may delete, purge and generally merge rows in unknown ways. This is
        // kind of ok because those stats are only used for optimizing the underlying storage format and so we
        // just have to strive for as good as possible. Currently, we stick to a relatively naive merge of existing
        // global stats because it's simple and probably good enough in most situation but we could probably
        // improve our merging of inaccuracy through the use of more fine-grained stats in the future.
        // Note however that to avoid seeing our accuracy degrade through successive compactions, we don't base
        // our stats merging on the compacted files headers, which as we just said can be somewhat inaccurate,
        // but rather on their stats stored in StatsMetadata that are fully accurate.
        EncodingStats.Collector stats = new EncodingStats.Collector();
        RegularAndStaticColumns.Builder columns = RegularAndStaticColumns.builder();
        // We need to order the SSTables by descending generation to be sure that we use latest column metadata.
        for (SSTableReader sstable : orderByDescendingGeneration(sstables))
        {
            stats.updateTimestamp(sstable.getMinTimestamp());
            stats.updateLocalDeletionTime(sstable.getMinLocalDeletionTime());
            stats.updateTTL(sstable.getMinTTL());
            columns.addAll(sstable.header.columns());
        }
        return new SerializationHeader(true, metadata, columns.build(), stats.get());
    }

    private static Collection<SSTableReader> orderByDescendingGeneration(Collection<SSTableReader> sstables)
    {
        if (sstables.size() < 2)
            return sstables;

        List<SSTableReader> readers = new ArrayList<>(sstables);
        readers.sort(SSTableReader.idReverseComparator);
        return readers;
    }

    public SerializationHeader(boolean isForSSTable,
                               TableMetadata metadata,
                               RegularAndStaticColumns columns,
                               EncodingStats stats)
    {
        this(isForSSTable,
             metadata.partitionKeyType,
             metadata.comparator.subtypes(),
             columns,
             stats,
             null);
    }

    public RegularAndStaticColumns columns()
    {
        return columns;
    }

    public boolean hasStatic()
    {
        return !columns.statics.isEmpty();
    }

    public boolean isForSSTable()
    {
        return isForSSTable;
    }

    public EncodingStats stats()
    {
        return stats;
    }

    public AbstractType<?> keyType()
    {
        return keyType;
    }

    public List<AbstractType<?>> clusteringTypes()
    {
        return clusteringTypes;
    }

    public Columns columns(boolean isStatic)
    {
        return isStatic ? columns.statics : columns.regulars;
    }

    public AbstractType<?> getType(ColumnMetadata column)
    {
        return typeMap == null ? column.type : typeMap.get(column.name.bytes);
    }

    public void writeTimestamp(long timestamp, DataOutputPlus out) throws IOException
    {
        out.writeUnsignedVInt(timestamp - stats.minTimestamp);
    }

    public void writeLocalDeletionTime(int localDeletionTime, DataOutputPlus out) throws IOException
    {
        out.writeUnsignedVInt(localDeletionTime - stats.minLocalDeletionTime);
    }

    public void writeTTL(int ttl, DataOutputPlus out) throws IOException
    {
        out.writeUnsignedVInt(ttl - stats.minTTL);
    }

    public void writeDeletionTime(DeletionTime dt, DataOutputPlus out) throws IOException
    {
        writeTimestamp(dt.markedForDeleteAt(), out);
        writeLocalDeletionTime(dt.localDeletionTime(), out);
    }

    public long readTimestamp(DataInputPlus in) throws IOException
    {
        return in.readUnsignedVInt() + stats.minTimestamp;
    }

    public int readLocalDeletionTime(DataInputPlus in) throws IOException
    {
        return (int)in.readUnsignedVInt() + stats.minLocalDeletionTime;
    }

    public int readTTL(DataInputPlus in) throws IOException
    {
        return (int)in.readUnsignedVInt() + stats.minTTL;
    }

    public DeletionTime readDeletionTime(DataInputPlus in) throws IOException
    {
        long markedAt = readTimestamp(in);
        int localDeletionTime = readLocalDeletionTime(in);
        return new DeletionTime(markedAt, localDeletionTime);
    }

    public long timestampSerializedSize(long timestamp)
    {
        return TypeSizes.sizeofUnsignedVInt(timestamp - stats.minTimestamp);
    }

    public long localDeletionTimeSerializedSize(int localDeletionTime)
    {
        return TypeSizes.sizeofUnsignedVInt(localDeletionTime - stats.minLocalDeletionTime);
    }

    public long ttlSerializedSize(int ttl)
    {
        return TypeSizes.sizeofUnsignedVInt(ttl - stats.minTTL);
    }

    public long deletionTimeSerializedSize(DeletionTime dt)
    {
        return timestampSerializedSize(dt.markedForDeleteAt())
             + localDeletionTimeSerializedSize(dt.localDeletionTime());
    }

    public void skipTimestamp(DataInputPlus in) throws IOException
    {
        in.readUnsignedVInt();
    }

    public void skipLocalDeletionTime(DataInputPlus in) throws IOException
    {
        in.readUnsignedVInt();
    }

    public void skipTTL(DataInputPlus in) throws IOException
    {
        in.readUnsignedVInt();
    }

    public void skipDeletionTime(DataInputPlus in) throws IOException
    {
        skipTimestamp(in);
        skipLocalDeletionTime(in);
    }

    public Component toComponent()
    {
        LinkedHashMap<ByteBuffer, AbstractType<?>> staticColumns = new LinkedHashMap<>();
        LinkedHashMap<ByteBuffer, AbstractType<?>> regularColumns = new LinkedHashMap<>();
        for (ColumnMetadata column : columns.statics)
            staticColumns.put(column.name.bytes, column.type);
        for (ColumnMetadata column : columns.regulars)
            regularColumns.put(column.name.bytes, column.type);
        return new Component(keyType, clusteringTypes, staticColumns, regularColumns, stats);
    }

    @Override
    public String toString()
    {
        return String.format("SerializationHeader[key=%s, cks=%s, columns=%s, stats=%s, typeMap=%s]", keyType, clusteringTypes, columns, stats, typeMap);
    }

    /**
     * We need the TableMetadata to properly deserialize a SerializationHeader but it's clunky to pass that to
     * a SSTable component, so we use this temporary object to delay the actual need for the metadata.
     */
    public static class Component extends MetadataComponent
    {
        private final AbstractType<?> keyType;
        private final List<AbstractType<?>> clusteringTypes;
        private final LinkedHashMap<ByteBuffer, AbstractType<?>> staticColumns;
        private final LinkedHashMap<ByteBuffer, AbstractType<?>> regularColumns;
        private final EncodingStats stats;

        private Component(AbstractType<?> keyType,
                          List<AbstractType<?>> clusteringTypes,
                          LinkedHashMap<ByteBuffer, AbstractType<?>> staticColumns,
                          LinkedHashMap<ByteBuffer, AbstractType<?>> regularColumns,
                          EncodingStats stats)
        {
            this.keyType = keyType;
            this.clusteringTypes = clusteringTypes;
            this.staticColumns = staticColumns;
            this.regularColumns = regularColumns;
            this.stats = stats;
        }

        /**
         * <em>Only</em> exposed for {@link org.apache.cassandra.io.sstable.SSTableHeaderFix}.
         */
        public static Component buildComponentForTools(AbstractType<?> keyType,
                                                       List<AbstractType<?>> clusteringTypes,
                                                       LinkedHashMap<ByteBuffer, AbstractType<?>> staticColumns,
                                                       LinkedHashMap<ByteBuffer, AbstractType<?>> regularColumns,
                                                       EncodingStats stats)
        {
            return new Component(keyType, clusteringTypes, staticColumns, regularColumns, stats);
        }

        public MetadataType getType()
        {
            return MetadataType.HEADER;
        }

        private static AbstractType<?> validateType(String description,
                                                    TableMetadata table,
                                                    ByteBuffer columnName,
                                                    AbstractType<?> type,
                                                    boolean allowImplicitlyFrozenTuples,
                                                    boolean isForOfflineTool)
        {
            boolean dropped = table.getDroppedColumn(columnName) != null;
            boolean isPrimaryKeyColumn = Iterables.any(table.primaryKeyColumns(), cd -> cd.name.bytes.equals(columnName));

            try
            {
                type.validateForColumn(columnName, isPrimaryKeyColumn, table.isCounter(), dropped, isForOfflineTool, DURATION_IN_MAPS_COMPATIBILITY_MODE.getBoolean());
                return type;
            }
            catch (InvalidColumnTypeException e)
            {
                AbstractType<?> fixed = allowImplicitlyFrozenTuples ? tryFix(type, columnName, isPrimaryKeyColumn, table.isCounter(), dropped, isForOfflineTool) : null;
                if (fixed == null)
                {
                    // We don't know how to fix. We throw an error here because reading such table may result in corruption
                    String msg = String.format("Error reading SSTable header %s, the type for column %s in %s is %s, which is invalid (%s); " +
                                               "The type could not be automatically fixed.",
                                               description, ColumnIdentifier.toCQLString(columnName), table, type.asCQL3Type().toSchemaString(),
                                               e.getMessage());
                    throw new IllegalArgumentException(msg, e);
                }
                else
                {
                    logger.debug("Error reading SSTable header {}, the type for column {} in {} is {}, which is " +
                                 "invalid ({}); The type has been automatically fixed to {}, but please contact " +
                                 "support if this is incorrect.",
                                 description, ColumnIdentifier.toCQLString(columnName), table, type.asCQL3Type().toSchemaString(),
                                 e.getMessage(), fixed.asCQL3Type().toSchemaString());
                    return fixed;
                }
            }
        }

        /**
         * Attempts to return a "fixed" (and thus valid) version of the type. Doing is so is only possible in restrained
         * case where we know why the type is invalid and are confident we know what it should be.
         *
         * @return if we know how to auto-magically fix the invalid type that triggered this exception, the hopefully
         * fixed version of said type. Otherwise, {@code null}.
         */
        public static AbstractType<?> tryFix(AbstractType<?> invalidType, ByteBuffer name, boolean isPrimaryKeyColumn, boolean isCounterTable, boolean isDroppedColumn, boolean isForOfflineTool)
        {
            AbstractType<?> fixed = tryFixInternal(invalidType, isPrimaryKeyColumn, isDroppedColumn);
            if (fixed != null)
            {
                try
                {
                    // Make doubly sure the fixed type is valid before returning it.
                    fixed.validateForColumn(name, isPrimaryKeyColumn, isCounterTable, isDroppedColumn, isForOfflineTool, DURATION_IN_MAPS_COMPATIBILITY_MODE.getBoolean());
                    return fixed;
                }
                catch (InvalidColumnTypeException e2)
                {
                    // Continue as if we hadn't been able to fix, since we haven't
                }
            }
            return null;
        }

        private static AbstractType<?> tryFixInternal(AbstractType<?> invalidType, boolean isPrimaryKeyColumn, boolean isDroppedColumn)
        {
            if (isPrimaryKeyColumn)
            {
                // The only issue we have a fix to in that case if the type is not frozen; we can then just freeze it.
                if (invalidType.isMultiCell())
                    return invalidType.freeze();
            }
            else
            {
                // Here again, it's mainly issues of frozen-ness that are fixable, namely multi-cell types that either:
                // - are tuples, yet not for a dropped column (and so _should_ be frozen). In which case we freeze it.
                // - has non-frozen subtypes. In which case, we just freeze all subtypes.
                if (invalidType.isMultiCell())
                {
                    boolean isMultiCell = !invalidType.isTuple() || isDroppedColumn;
                    return invalidType.with(AbstractType.freeze(invalidType.subTypes()), isMultiCell);
                }

            }
            // In other case, we don't know how to fix (at least somewhat auto-magically) and will have to fail.
            return null;
        }

        private static AbstractType<?> validatePartitionKeyType(String descriptor,
                                                                TableMetadata table,
                                                                AbstractType<?> fullType,
                                                                boolean allowImplicitlyFrozenTuples,
                                                                boolean isForOfflineTool)
        {
            List<ColumnMetadata> pkColumns = table.partitionKeyColumns();
            int pkCount = pkColumns.size();

            if (pkCount == 1)
                return validateType(descriptor, table, pkColumns.get(0).name.bytes, fullType, allowImplicitlyFrozenTuples, isForOfflineTool);

            List<AbstractType<?>> subTypes = fullType.subTypes();
            assert fullType instanceof CompositeType && subTypes.size() == pkCount
                    : String.format("In %s, got %s as table %s partition key type but partition key is %s",
                                    descriptor, fullType, table, pkColumns);

            return CompositeType.getInstance(validatePKTypes(descriptor, table, pkColumns, subTypes, allowImplicitlyFrozenTuples, isForOfflineTool));
        }

        private static List<AbstractType<?>> validatePKTypes(String descriptor,
                                                             TableMetadata table,
                                                             List<ColumnMetadata> columns,
                                                             List<AbstractType<?>> types,
                                                             boolean allowImplicitlyFrozenTuples,
                                                             boolean isForOfflineTool)
        {
            int count = types.size();
            List<AbstractType<?>> updated = new ArrayList<>(count);
            for (int i = 0; i < count; i++)
            {
                updated.add(validateType(descriptor,
                                         table,
                                         columns.get(i).name.bytes,
                                         types.get(i),
                                         allowImplicitlyFrozenTuples,
                                         isForOfflineTool));
            }
            return updated;
        }

        public SerializationHeader toHeader(Descriptor descriptor, TableMetadata metadata) throws UnknownColumnException
        {
            return toHeader(descriptor.toString(), metadata, descriptor.version, false);
        }

        public SerializationHeader toHeader(String descriptor, TableMetadata metadata, Version sstableVersion, boolean isForOfflineTool) throws UnknownColumnException
        {
            Map<ByteBuffer, AbstractType<?>> typeMap = new HashMap<>(staticColumns.size() + regularColumns.size());
            RegularAndStaticColumns.Builder builder = RegularAndStaticColumns.builder();

            for (Map<ByteBuffer, AbstractType<?>> map : ImmutableList.of(staticColumns, regularColumns))
            {
                boolean isStatic = map == staticColumns;
                for (Map.Entry<ByteBuffer, AbstractType<?>> e : map.entrySet())
                {
                    ByteBuffer name = e.getKey();
                    AbstractType<?> type = validateType(descriptor, metadata, name, e.getValue(), sstableVersion.hasImplicitlyFrozenTuples(), isForOfflineTool);
                    AbstractType<?> other = typeMap.put(name, type);
                    if (other != null && !other.equals(type))
                        throw new IllegalStateException("Column " + name + " occurs as both regular and static with types " + other + "and " + e.getValue());

                    ColumnMetadata column = metadata.getColumn(name);
                    if (column == null || column.isStatic() != isStatic)
                    {
                        // TODO: this imply we don't read data for a column we don't yet know about, which imply this is theoretically
                        // racy with column addition. Currently, it is up to the user to not write data before the schema has propagated
                        // and this is far from being the only place that has such problem in practice. This doesn't mean we shouldn't
                        // improve this.

                        // If we don't find the definition, it could be we have data for a dropped column, and we shouldn't
                        // fail deserialization because of that. So we grab a "fake" ColumnDefinition that ensure proper
                        // deserialization. The column will be ignored later on anyway.
                        column = metadata.getDroppedColumn(name, isStatic);
                        if (column == null)
                            throw new UnknownColumnException("Unknown column " + UTF8Type.instance.getString(name) + " during deserialization of " + metadata.keyspace + '.' + metadata.name);
                    }
                    builder.add(column);
                }
            }

            AbstractType<?> partitionKeys = validatePartitionKeyType(descriptor, metadata, keyType, sstableVersion.hasImplicitlyFrozenTuples(), isForOfflineTool);
            List<AbstractType<?>> clusterings = validatePKTypes(descriptor,
                                                                metadata,
                                                                metadata.clusteringColumns(),
                                                                clusteringTypes,
                                                                sstableVersion.hasImplicitlyFrozenTuples(),
                                                                isForOfflineTool);

            return new SerializationHeader(true,
                                           partitionKeys,
                                           clusterings,
                                           builder.build(),
                                           stats,
                                           typeMap);
        }

        @Override
        public boolean equals(Object o)
        {
            if(!(o instanceof Component))
                return false;

            Component that = (Component)o;
            return Objects.equals(this.keyType, that.keyType)
                && Objects.equals(this.clusteringTypes, that.clusteringTypes)
                && Objects.equals(this.staticColumns, that.staticColumns)
                && Objects.equals(this.regularColumns, that.regularColumns)
                && Objects.equals(this.stats, that.stats);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(keyType, clusteringTypes, staticColumns, regularColumns, stats);
        }

        @Override
        public String toString()
        {
            return String.format("SerializationHeader.Component[key=%s, cks=%s, statics=%s, regulars=%s, stats=%s]",
                                 keyType, clusteringTypes, staticColumns, regularColumns, stats);
        }

        public AbstractType<?> getKeyType()
        {
            return keyType;
        }

        public List<AbstractType<?>> getClusteringTypes()
        {
            return clusteringTypes;
        }

        public Map<ByteBuffer, AbstractType<?>> getStaticColumns()
        {
            return staticColumns;
        }

        public Map<ByteBuffer, AbstractType<?>> getRegularColumns()
        {
            return regularColumns;
        }

        public EncodingStats getEncodingStats()
        {
            return stats;
        }

        @SuppressWarnings("unused")
        public Component withMigratedKeyspaces(Map<String, String> keyspaceMapping)
        {
            if (keyspaceMapping.isEmpty())
                return this;

            AbstractType<?> newKeyType = keyType.overrideKeyspace(ks -> keyspaceMapping.getOrDefault(ks, ks));
            List<AbstractType<?>> clusteringTypes = this.clusteringTypes.stream().map(t -> t.overrideKeyspace(ks -> keyspaceMapping.getOrDefault(ks, ks))).collect(Collectors.toList());
            LinkedHashMap<ByteBuffer, AbstractType<?>> staticColumns = this.staticColumns.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().overrideKeyspace(ks -> keyspaceMapping.getOrDefault(ks, ks)),
                    (a, b) -> { throw new IllegalArgumentException("Duplicate key"); },
                    LinkedHashMap::new));
            LinkedHashMap<ByteBuffer, AbstractType<?>> regularColumns = this.regularColumns.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().overrideKeyspace(ks -> keyspaceMapping.getOrDefault(ks, ks)),
                    (a, b) -> { throw new IllegalArgumentException("Duplicate key"); },
                    LinkedHashMap::new));
            return new Component(newKeyType, clusteringTypes, staticColumns, regularColumns, stats);
        }

    }

    public static class Serializer implements IMetadataComponentSerializer<Component>
    {
        private final AbstractTypeSerializer typeSerializer = new AbstractTypeSerializer();

        public void serializeForMessaging(SerializationHeader header, ColumnFilter selection, DataOutputPlus out, boolean hasStatic) throws IOException
        {
            EncodingStats.serializer.serialize(header.stats, out);

            if (selection == null)
            {
                if (hasStatic)
                    Columns.serializer.serialize(header.columns.statics, out);
                Columns.serializer.serialize(header.columns.regulars, out);
            }
            else
            {
                if (hasStatic)
                    Columns.serializer.serializeSubset(header.columns.statics, selection.fetchedColumns().statics, out);
                Columns.serializer.serializeSubset(header.columns.regulars, selection.fetchedColumns().regulars, out);
            }
        }

        public SerializationHeader deserializeForMessaging(DataInputPlus in, TableMetadata metadata, ColumnFilter selection, boolean hasStatic) throws IOException
        {
            EncodingStats stats = EncodingStats.serializer.deserialize(in);

            AbstractType<?> keyType = metadata.partitionKeyType;
            List<AbstractType<?>> clusteringTypes = metadata.comparator.subtypes();

            Columns statics, regulars;
            if (selection == null)
            {
                statics = hasStatic ? Columns.serializer.deserialize(in, metadata) : Columns.NONE;
                regulars = Columns.serializer.deserialize(in, metadata);
            }
            else
            {
                statics = hasStatic ? Columns.serializer.deserializeSubset(selection.fetchedColumns().statics, in) : Columns.NONE;
                regulars = Columns.serializer.deserializeSubset(selection.fetchedColumns().regulars, in);
            }

            return new SerializationHeader(false, keyType, clusteringTypes, new RegularAndStaticColumns(statics, regulars), stats, null);
        }

        public long serializedSizeForMessaging(SerializationHeader header, ColumnFilter selection, boolean hasStatic)
        {
            long size = EncodingStats.serializer.serializedSize(header.stats);

            if (selection == null)
            {
                if (hasStatic)
                    size += Columns.serializer.serializedSize(header.columns.statics);
                size += Columns.serializer.serializedSize(header.columns.regulars);
            }
            else
            {
                if (hasStatic)
                    size += Columns.serializer.serializedSubsetSize(header.columns.statics, selection.fetchedColumns().statics);
                size += Columns.serializer.serializedSubsetSize(header.columns.regulars, selection.fetchedColumns().regulars);
            }
            return size;
        }

        // For SSTables
        public void serialize(Version version, Component header, DataOutputPlus out) throws IOException
        {
            EncodingStats.serializer.serialize(header.stats, out);

            typeSerializer.serialize(header.keyType, out);
            typeSerializer.serializeList(header.clusteringTypes, out);

            writeColumnsWithTypes(header.staticColumns, out);
            writeColumnsWithTypes(header.regularColumns, out);
        }

        // For SSTables
        public Component deserialize(Version version, DataInputPlus in) throws IOException
        {
            EncodingStats stats = EncodingStats.serializer.deserialize(in);

            AbstractType<?> keyType = typeSerializer.deserialize(in);
            List<AbstractType<?>> clusteringTypes = typeSerializer.deserializeList(in);

            LinkedHashMap<ByteBuffer, AbstractType<?>> staticColumns = readColumnsWithType(in);
            LinkedHashMap<ByteBuffer, AbstractType<?>> regularColumns = readColumnsWithType(in);

            return new Component(keyType, clusteringTypes, staticColumns, regularColumns, stats);
        }

        // For SSTables
        public int serializedSize(Version version, Component header)
        {
            int size = EncodingStats.serializer.serializedSize(header.stats);

            size += typeSerializer.serializedSize(header.keyType);
            size += TypeSizes.sizeofUnsignedVInt(header.clusteringTypes.size());
            for (AbstractType<?> type : header.clusteringTypes)
                size += typeSerializer.serializedSize(type);

            size += sizeofColumnsWithTypes(header.staticColumns);
            size += sizeofColumnsWithTypes(header.regularColumns);
            return size;
        }

        private void writeColumnsWithTypes(Map<ByteBuffer, AbstractType<?>> columns, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt(columns.size());
            for (Map.Entry<ByteBuffer, AbstractType<?>> entry : columns.entrySet())
            {
                ByteBufferUtil.writeWithVIntLength(entry.getKey(), out);
                typeSerializer.serialize(entry.getValue(), out);
            }
        }

        private long sizeofColumnsWithTypes(Map<ByteBuffer, AbstractType<?>> columns)
        {
            long size = TypeSizes.sizeofUnsignedVInt(columns.size());
            for (Map.Entry<ByteBuffer, AbstractType<?>> entry : columns.entrySet())
            {
                size += ByteBufferUtil.serializedSizeWithVIntLength(entry.getKey());
                size += typeSerializer.serializedSize(entry.getValue());
            }
            return size;
        }

        private LinkedHashMap<ByteBuffer, AbstractType<?>> readColumnsWithType(DataInputPlus in) throws IOException
        {
            int length = (int) in.readUnsignedVInt();
            LinkedHashMap<ByteBuffer, AbstractType<?>> typeMap = new LinkedHashMap<>(length);
            for (int i = 0; i < length; i++)
            {
                ByteBuffer name = ByteBufferUtil.readWithVIntLength(in);
                typeMap.put(name, typeSerializer.deserialize(in));
            }
            return typeMap;
        }
    }
}
