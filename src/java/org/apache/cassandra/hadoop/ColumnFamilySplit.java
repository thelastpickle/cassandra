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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.TokenRange;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;


public class ColumnFamilySplit extends InputSplit implements Writable, org.apache.hadoop.mapred.InputSplit
{
    private List<Range<Token>> tokenRanges;
    private long length;
    private String[] dataNodes;
    private String partitionerClassname;

    public static List<Range<Token>> toRangeTokens(List<TokenRange> tokenRanges, String partitionerClassname)
    {
        assert !tokenRanges.isEmpty();
        Token.TokenFactory factory = FBUtilities.newPartitioner(partitionerClassname).getTokenFactory();
        List<Range<Token>> rangeTokens = new ArrayList<>();
        for (TokenRange tokenRange : tokenRanges)
        {
            assert tokenRange.getStart() != null;
            assert tokenRange.getEnd() != null;
            Token left = factory.fromString(tokenRange.getStart().toString());
            Token right = factory.fromString(tokenRange.getEnd().toString());
            Range<Token> range = new Range<>(left, right);
            rangeTokens.add(range);
        }
        return rangeTokens;
    }

    public ColumnFamilySplit(List<Range<Token>> tokenRanges, long length, String[] dataNodes, String partitionerClassname)
    {
        assert !tokenRanges.isEmpty();
        for (Range<Token> tokenRange : tokenRanges)
        {
            assert tokenRange.left != null;
            assert tokenRange.right != null;
        }
        this.tokenRanges = tokenRanges;
        this.length = length;
        this.dataNodes = dataNodes;
        this.partitionerClassname = partitionerClassname;
    }

    public List<Range<Token>> getTokenRanges()
    {
        return tokenRanges;
    }

    // getLength and getLocations satisfy the InputSplit abstraction

    public long getLength()
    {
        return length;
    }

    public String[] getLocations()
    {
        return dataNodes;
    }

    // This should only be used by KeyspaceSplit.read();
    protected ColumnFamilySplit() {}

    // These three methods are for serializing and deserializing
    // KeyspaceSplits as needed by the Writable interface.
    public void write(DataOutput out) throws IOException
    {
        out.writeUTF(partitionerClassname);
        out.writeLong(length);
        out.writeInt(tokenRanges.size());
        try
        {
            Token.TokenFactory factory = FBUtilities.newPartitioner(partitionerClassname).getTokenFactory();
            for (Range<Token> range : tokenRanges)
            {
                out.writeUTF(factory.toString(range.left));
                out.writeUTF(factory.toString(range.right));
            }
            out.writeInt(dataNodes.length);
            for (String endpoint : dataNodes)
                out.writeUTF(endpoint);
        }
        catch (ConfigurationException ex)
        {
            throw new IOException(ex);
        }
    }

    public void readFields(DataInput in) throws IOException
    {
        partitionerClassname = in.readUTF();
        length = in.readLong();
        int tokenRangeSize = in.readInt();
        try
        {
            IPartitioner partitioner = FBUtilities.newPartitioner(partitionerClassname);
            Token.TokenFactory factory = partitioner.getTokenFactory();
            tokenRanges = new ArrayList<>();
            List<String> endpoints = new ArrayList<>();
            for (int i = 0 ; i < tokenRangeSize ; ++i)
                tokenRanges.add(new Range<>(factory.fromString(in.readUTF()), factory.fromString(in.readUTF())));

            int numOfEndpoints = in.readInt();
            dataNodes = new String[numOfEndpoints];
            for(int i = 0; i < numOfEndpoints; i++)
            {
                String datanode = in.readUTF();
                dataNodes[i] = datanode;
                endpoints.add(datanode);
            }
        }
        catch (ConfigurationException ex)
        {
            throw new IOException(ex);
        }
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ColumnFamilySplit([");
        for (Range<Token> range : tokenRanges)
            sb.append("(").append(range.left).append(", '").append(range.right).append("],");

        sb.setLength(sb.length()-1);
        sb.append("] @");
        if (null != dataNodes)
        {
            for (String dataNode : dataNodes)
                sb.append(dataNode).append(',');

            sb.setLength(sb.length()-1);
        }
        return sb.append(')').toString();
    }

    public static ColumnFamilySplit read(DataInput in) throws IOException
    {
        ColumnFamilySplit w = new ColumnFamilySplit();
        w.readFields(in);
        return w;
    }
}
