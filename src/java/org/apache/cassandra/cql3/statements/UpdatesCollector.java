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

package org.apache.cassandra.cql3.statements;

import java.util.List;

<<<<<<< HEAD
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
=======
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Maps;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.TableMetadata;

public interface UpdatesCollector
{
<<<<<<< HEAD
    PartitionUpdate.Builder getPartitionUpdateBuilder(TableMetadata metadata, DecoratedKey dk, ConsistencyLevel consistency);
    List<IMutation> toMutations();
=======
    /**
     * The columns that will be updated for each table (keyed by the table ID).
     */
    private final Map<UUID, PartitionColumns> updatedColumns;

    /**
     * The estimated number of updated rows per partition.
     */
    private final Map<UUID, HashMultiset<ByteBuffer>> partitionCounts;

    public final long createdAt = System.currentTimeMillis();

    /**
     * The mutations per keyspace.
     */
    private final Map<String, Map<ByteBuffer, IMutation>> mutations;

    public UpdatesCollector(Map<UUID, PartitionColumns> updatedColumns, Map<UUID, HashMultiset<ByteBuffer>> partitionCounts)
    {
        super();
        this.updatedColumns = updatedColumns;
        this.partitionCounts = partitionCounts;
        mutations = Maps.newHashMapWithExpectedSize(partitionCounts.size()); // most often this is too big - optimised for the single-table case
    }

    /**
     * Gets the <code>PartitionUpdate</code> for the specified column family and key. If the update does not
     * exist it will be created.
     *
     * @param cfm the column family meta data
     * @param dk the partition key
     * @param consistency the consistency level
     * @return the <code>PartitionUpdate</code> for the specified column family and key
     */
    public PartitionUpdate getPartitionUpdate(CFMetaData cfm, DecoratedKey dk, ConsistencyLevel consistency)
    {
        Mutation mut = getMutation(cfm, dk, consistency);
        PartitionUpdate upd = mut.get(cfm);
        if (upd == null)
        {
            PartitionColumns columns = updatedColumns.get(cfm.cfId);
            assert columns != null;
            upd = new PartitionUpdate(cfm, dk, columns, partitionCounts.get(cfm.cfId).count(dk.getKey()), (int)(createdAt / 1000));
            mut.add(upd);
        }
        return upd;
    }

    /**
     * Check all partition updates contain only valid values for any
     * indexed columns.
     */
    public void validateIndexedColumns()
    {
        for (Map<ByteBuffer, IMutation> perKsMutations : mutations.values())
            for (IMutation mutation : perKsMutations.values())
                for (PartitionUpdate update : mutation.getPartitionUpdates())
                    Keyspace.openAndGetStore(update.metadata()).indexManager.validate(update);
    }

    private Mutation getMutation(CFMetaData cfm, DecoratedKey dk, ConsistencyLevel consistency)
    {
        String ksName = cfm.ksName;
        IMutation mutation = keyspaceMap(ksName).get(dk.getKey());
        if (mutation == null)
        {
            Mutation mut = new Mutation(ksName, dk, createdAt, cfm.params.cdc);
            mutation = cfm.isCounter() ? new CounterMutation(mut, consistency) : mut;
            keyspaceMap(ksName).put(dk.getKey(), mutation);
            return mut;
        }
        return cfm.isCounter() ? ((CounterMutation) mutation).getMutation() : (Mutation) mutation;
    }

    /**
     * Returns a collection containing all the mutations.
     * @return a collection containing all the mutations.
     */
    public Collection<IMutation> toMutations()
    {
        // The case where all statement where on the same keyspace is pretty common
        if (mutations.size() == 1)
            return mutations.values().iterator().next().values();

        List<IMutation> ms = new ArrayList<>(mutations.size());
        for (Map<ByteBuffer, IMutation> ksMap : mutations.values())
            ms.addAll(ksMap.values());

        return ms;
    }

    /**
     * Returns the key-mutation mappings for the specified keyspace.
     *
     * @param ksName the keyspace name
     * @return the key-mutation mappings for the specified keyspace.
     */
    private Map<ByteBuffer, IMutation> keyspaceMap(String ksName)
    {
        Map<ByteBuffer, IMutation> ksMap = mutations.get(ksName);
        if (ksMap == null)
        {
            ksMap = new HashMap<>();
            mutations.put(ksName, ksMap);
        }
        return ksMap;
    }
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
}
