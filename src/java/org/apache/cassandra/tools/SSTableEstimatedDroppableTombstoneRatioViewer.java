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
package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMetadata;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Shows the EstimatedDroppableTombstoneRatio from a sstable metadata
 */
public class SSTableEstimatedDroppableTombstoneRatioViewer
{
    /**
     * @param args a timestamp in seconds to base gcBefore off, and a list of sstables whose metadata we're interested in
     */
    public static void main(String[] args) throws IOException
    {
        PrintStream out = System.out;
        if (args.length < 3)
        {
            out.println("Usage: sstabletombstonesestimate <keyspace> <table> <timestamp_in_seconds> [csv]");
            out.println();
            out.println("timestamp_in_seconds is the timestamp in the future the estimated droppable tombstones ratio will be calculated off");
            out.println();
            out.println("Outputted format will be");
            out.println(" host, sstable, Estimated droppable tombstones, Minimum timestamp, Maximum timestamp, max local deletion time");
            System.exit(1);
        }

        int gcBefore = Integer.parseInt(args[2]);

        // load keyspace descriptions.
        DatabaseDescriptor.loadSchemas(false);

        if (Schema.instance.getCFMetaData(args[0], args[1]) == null)
        {
            out.println(String.format("Unknown keyspace/columnFamily %s.%s", args[0], args[1]));
        }
        else
        {
            Keyspace keyspace = Keyspace.openWithoutSSTables(args[0]);
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(args[1]);
            Directories.SSTableLister lister = cfs.directories.sstableLister().skipTemporary(true);

            for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
            {
                if (!entry.getValue().contains(Component.DATA))
                    continue;

                Descriptor descriptor = entry.getKey();
                SSTableMetadata stats = SSTableMetadata.serializer.deserialize(descriptor).left;

                if (4 == args.length && "csv".equalsIgnoreCase(args[3])) {
                    out.printf(
                            "%s,%s,%s,%s,%s,%s,%s,%s%n",
                            FBUtilities.getBroadcastAddress().getHostName(),
                            descriptor.baseFilename(),
                            new File(descriptor.filenameFor(Component.DATA)).length(),
                            stats.getEstimatedDroppableTombstoneRatio(gcBefore),
                            stats.minTimestamp,
                            stats.maxTimestamp,
                            Integer.MAX_VALUE == stats.maxLocalDeletionTime ? "n"   : "y",
                            stats.maxLocalDeletionTime);

                } else {
                    out.println();

                    out.printf("SSTable: %s%n", descriptor.baseFilename());
                    out.printf("SSTable size: %skb%n", new File(descriptor.filenameFor(Component.DATA)).length()/1000);

                    out.printf(
                            "Estimated droppable tombstones: %s%n",
                            stats.getEstimatedDroppableTombstoneRatio(gcBefore));

                    out.println();

                    out.printf("Minimum timestamp: %s - %s%n",
                            stats.minTimestamp,
                            new Date(stats.minTimestamp/1000).toString());

                    out.printf("Maximum timestamp: %s - %s%n",
                            stats.maxTimestamp,
                            new Date(stats.maxTimestamp/1000).toString());

                    out.printf("SSTable max local deletion time: %s - %s%n",
                            stats.maxLocalDeletionTime,
                            new Date(stats.maxLocalDeletionTime*1000L).toString());
                }
            }
        }
        System.exit(0); // We need that to stop non daemonized threads
    }
}
