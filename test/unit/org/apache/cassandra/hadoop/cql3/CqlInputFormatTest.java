package org.apache.cassandra.hadoop.cql3;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.hadoop.ColumnFamilySplit;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

public final class CqlInputFormatTest
{
    @Test
    public void test_collectSplits()
    {
        String partitionerName = Murmur3Partitioner.class.getSimpleName();
        Configuration conf = new Configuration();
        ConfigHelper.setInputPartitioner(conf, partitionerName);
        IPartitioner partitioner = ConfigHelper.getInputPartitioner(conf);
        List<ColumnFamilySplit> splits = new ArrayList<>();
        for (int i = 0 ; i < 25600 ; ++i)
        {
            String[] dataNodes = new String[]
            {
                "host-" + (i%100),
                "host-" + (i%100)+1,
                "host-" + (i%100)+2
            };

            Range<Token> dhtRange = new Range<>(partitioner.getTokenFactory().fromString("" + i),
                                                partitioner.getTokenFactory().fromString("" + (i+1)));

            ColumnFamilySplit split = new ColumnFamilySplit(Collections.singletonList(dhtRange),
                                                            1,
                                                            dataNodes,
                                                            partitionerName);

            splits.add(split);
        }
        List<InputSplit> collected = CqlInputFormat.collectSplits(splits, conf);
        assert 100 == collected.size();
        for (InputSplit split : collected)
        {
            assert 256 == ((ColumnFamilySplit)split).getTokenRanges().size();
        }
    }
}
