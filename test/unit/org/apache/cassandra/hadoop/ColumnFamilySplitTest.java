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
package org.apache.cassandra.hadoop;

import java.util.List;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.CfSplit;


public final class ColumnFamilySplitTest
{

    @Test
    public void test_serialization() throws UnknownHostException, IOException
    {
        IPartitioner partitioner = new RandomPartitioner();
        Token.TokenFactory factory = partitioner.getTokenFactory();

        String[] endpoints = new String[]
        {
            "localhost-1",
            "localhost-2",
            "localhost-3"
        };

        CfSplit subSplit = new CfSplit("2", "3", Long.MAX_VALUE);
        Token left = factory.fromString(subSplit.getStart_token());
        Token right = factory.fromString(subSplit.getEnd_token());
        Range<Token> range = new Range<>(left, right);
        List<Range<Token>> ranges = range.isWrapAround() ? range.unwrap() : ImmutableList.of(range);

        ColumnFamilySplit pre = new ColumnFamilySplit(
                                    ranges,
                                    subSplit.getRow_count(),
                                    endpoints,
                                    partitioner.getClass().getName());

        File file = getOutput("test-serialization");
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(file)))
        {
            pre.write(out);
        }

        try (DataInputStream in = new DataInputStream(new FileInputStream(file)))
        {
            ColumnFamilySplit post = ColumnFamilySplit.read(in);

            assert pre.getLength() == post.getLength();
            assert pre.getLocations().length == post.getLocations().length;
            for (int i = 0 ; i < pre.getLocations().length ; ++i)
            {
                assert pre.getLocations()[i].equals(post.getLocations()[i]);
            }
            assert pre.getTokenRanges().size() == post.getTokenRanges().size();
            for (int i = 0 ; i < pre.getTokenRanges().size() ; ++i)
            {
                assert pre.getTokenRanges().get(i).equals(post.getTokenRanges().get(i));
            }
        }
    }

    private static File getOutput(String name) throws IOException
    {
        File f = File.createTempFile(ColumnFamilySplit.class.getName() + '.' + name, ".db");
        assert f.exists() : f.getPath();
        return f;
    }
}