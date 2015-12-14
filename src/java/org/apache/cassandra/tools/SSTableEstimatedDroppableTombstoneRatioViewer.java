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

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMetadata;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

/**
 * Shows the EstimatedDroppableTombstoneRatio from a sstable metadata
 */
public class SSTableEstimatedDroppableTombstoneRatioViewer
{
    static
    {
        CassandraDaemon.initLog4j();
    }

    private static final String TOOL_NAME = "sstabletombstonesestimate";
    private static final String VERBOSE_OPTION  = "verbose";
    private static final String HELP_OPTION  = "help";
    private static final String CSV_OPTION  = "csv";
    private static final String TIMESTAMP_OPTION  = "timestamp";

    /**
     * @param args a timestamp in seconds to base gcBefore off, and a list of sstables whose metadata we're interested in
     */
    public static void main(String[] args) throws IOException
    {
        Options options = Options.parseArgs(args);
        PrintStream out = System.out;

        // load keyspace descriptions.
//        DatabaseDescriptor.loadSchemas(false);

//        if (Schema.instance.getCFMetaData(options.keyspaceName, options.cfName) == null)
//        {
//            out.println(String.format("Unknown keyspace/table %s.%s", options.keyspaceName, options.cfName));
//        }
//        else
//        {
//            Keyspace keyspace = Keyspace.openWithoutSSTables(options.keyspaceName);
//            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(options.cfName);
            //Directories.SSTableLister lister = cfs.directories.sstableLister().skipTemporary(true);

            long deletableBytes = 0;

            for (String fname : options.files)
//            for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
            {
                Descriptor descriptor = Descriptor.fromFilename(fname);
//                if (!entry.getValue().contains(Component.DATA))
//                    continue;
//
//                Descriptor descriptor = entry.getKey();
                SSTableMetadata stats = SSTableMetadata.serializer.deserialize(descriptor).left;

//                boolean fullyExpired = cfs.metadata.getGcGraceSeconds() + stats.maxLocalDeletionTime <= options.timestamp;
                boolean fullyExpired = 86400 + stats.maxLocalDeletionTime <= options.timestamp;

                if (fullyExpired)
                    deletableBytes += new File(descriptor.filenameFor(Component.DATA)).length();

                if (options.verbose || fullyExpired)
                {
                    if (options.csv) {
                        out.printf(
                                "%n%s,%s,%s,%s,%s,%s,%s,%s,%s",
                                FBUtilities.getBroadcastAddress().getHostName(),
                                descriptor.baseFilename(),
                                new File(descriptor.filenameFor(Component.DATA)).length(),
                                stats.getEstimatedDroppableTombstoneRatio(options.timestamp),
                                stats.minTimestamp,
                                stats.maxTimestamp,
                                Integer.MAX_VALUE == stats.maxLocalDeletionTime ? "n" : "y",
                                stats.maxLocalDeletionTime,
                                fullyExpired ? "y" : "n");

                    }
                    else
                    {
                        out.printf("%nSSTable: %s%n", descriptor.baseFilename());
                        out.printf("SSTable size: %skb%n", new File(descriptor.filenameFor(Component.DATA)).length()/1000);

                        out.printf(
                                "Estimated droppable tombstones: %s%n%n",
                                stats.getEstimatedDroppableTombstoneRatio(options.timestamp));

                        out.printf("Minimum timestamp: %s - %s%n",
                                stats.minTimestamp,
                                new Date(stats.minTimestamp/1000).toString());

                        out.printf("Maximum timestamp: %s - %s%n",
                                stats.maxTimestamp,
                                new Date(stats.maxTimestamp/1000).toString());

                        out.printf("SSTable max local deletion time: %s - %s%n",
                                stats.maxLocalDeletionTime,
                                new Date(stats.maxLocalDeletionTime*1000L).toString());

                        if (options.verbose && fullyExpired)
                            out.println("Eligible for removal");

                    }
                }
            }
            if (options.verbose || !options.csv)
                out.printf("%n%n  ### %s kb of sstables can be deleted ###%n", deletableBytes / 1000);
//        }
        System.exit(0); // We need that to stop non daemonized threads
    }

    private static class Options
    {
//        public final String keyspaceName;
//        public final String cfName;
        public final String[] files;

        public boolean csv;
        public boolean verbose;
        public int timestamp;

        private Options(String[] files)
        {
//            this.keyspaceName = keyspaceName;
//            this.cfName = cfName;
            this.files = files;
        }

        public static Options parseArgs(String cmdArgs[])
        {
            CommandLineParser parser = new GnuParser();
            BulkLoader.CmdLineOptions options = getCmdLineOptions();
            try
            {
                CommandLine cmd = parser.parse(options, cmdArgs, false);

                if (cmd.hasOption(HELP_OPTION))
                {
                    printUsage(options);
                    System.exit(0);
                }

                String[] args = cmd.getArgs();
                if (args.length == 0)
                {
                    String msg = args.length < 2 ? "Missing arguments" : "Too many arguments";
                    System.err.println(msg);
                    printUsage(options);
                    System.exit(1);
                }

//                String keyspaceName = args[0];
//                String cfName = args[1];

                //Options opts = new Options(keyspaceName, cfName);
                Options opts = new Options(args);

                opts.csv = cmd.hasOption(CSV_OPTION);
                opts.verbose = cmd.hasOption(VERBOSE_OPTION);

                opts.timestamp = cmd.hasOption(TIMESTAMP_OPTION)
                        ? Integer.parseInt(cmd.getOptionValue(TIMESTAMP_OPTION))
                        : (int) System.currentTimeMillis() / 1000;

                return opts;
            }
            catch (ParseException e)
            {
                errorMsg(e.getMessage(), options);
                return null;
            }
        }

        private static void errorMsg(String msg, BulkLoader.CmdLineOptions options)
        {
            System.err.println(msg);
            printUsage(options);
            System.exit(1);
        }

        private static BulkLoader.CmdLineOptions getCmdLineOptions()
        {
            BulkLoader.CmdLineOptions options = new BulkLoader.CmdLineOptions();
            options.addOption("v",  VERBOSE_OPTION, "verbose output");
            options.addOption("h",  HELP_OPTION, "display this help message");

            options.addOption(
                    "c",
                    CSV_OPTION,
                    "output in the csv format:"
                            + " host, sstable, estimated droppable tombstones,"
                            + " minimum timestamp, maximum timestamp,"
                            + " max local deletion time");

            options.addOption(
                    "t",
                    TIMESTAMP_OPTION,
                    true,
                    "the timestamp in seconds in the future. "
                            + "sstables with a max local deletion time older than this are marked for deletion. "
                            + "estimated droppable tombstones ratio is also calculated off this. ");

            return options;
        }

        public static void printUsage(BulkLoader.CmdLineOptions options)
        {
            String usage = String.format("%s [options] <keyspace> <table>", TOOL_NAME);
            StringBuilder header = new StringBuilder();
            header.append("--\n");

            header.append("Use " + TOOL_NAME + " to investigate what impact on disk storage different TTL "
                    + "and/or gc_grace_seconds values would have, or how successful we would be if we can manually "
                    + "purge data pretending that it had a shorter TTL than it really did." );

            header.append("\n--\n");
            header.append("Options are:");
            new HelpFormatter().printHelp(usage, header.toString(), options, "");
        }
    }
}
