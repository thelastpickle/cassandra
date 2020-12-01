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

package org.apache.cassandra.dht.tokenallocator;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;

import org.assertj.core.api.Assertions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.tools.Util;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.fail;

public class OfflineTokenAllocatorTest
{
    private static final Logger logger = LoggerFactory.getLogger(OfflineTokenAllocatorTest.class);

    private static final OutputHandler FAIL_ON_WARN_OUTPUT = new OutputHandler.SystemOutput(true, true)
    {
        @Override
        public void warn(String msg)
        {
            fail(msg);
        }

        @Override
        public void warn(String msg, Throwable th)
        {
            fail(msg);
        }
    };

    @Before
    public void setup()
    {
        Util.initDatabaseDescriptor();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedPartitioner()
    {
        List<OfflineTokenAllocator.FakeNode> nodes = OfflineTokenAllocator.allocate(3, 4, new int[]{1,1,1}, FAIL_ON_WARN_OUTPUT, ByteOrderedPartitioner.instance);
        Assert.assertEquals(3, nodes.size());
    }

    /**
     * Cycle through a matrix of valid ranges.
     */
    @Test
    public void testTokenGenerations()
    {
        for (int numTokens = 1; numTokens <= 16 ; ++numTokens)
        {
            for (int rf = 1; rf <=5; ++rf)
            {
                int nodeCount = 32;
                for (int racks = rf; racks <= Math.min(10, nodeCount); ++racks)
                {
                    int[] nodeToRack = makeRackCountArray(nodeCount, racks);
                    for (IPartitioner partitioner : new IPartitioner[] { Murmur3Partitioner.instance, RandomPartitioner.instance })
                    {
                        logger.info("Testing offline token allocator for numTokens={}, rf={}, racks={}, nodeToRack={}, partitioner={}",
                                    numTokens, rf, racks, nodeToRack, partitioner);
                        List<OfflineTokenAllocator.FakeNode> nodes = OfflineTokenAllocator.allocate(rf, numTokens, nodeToRack, FAIL_ON_WARN_OUTPUT, partitioner);
                        Collection<Token> allTokens = Lists.newArrayList();
                        for (OfflineTokenAllocator.FakeNode node : nodes)
                        {
                            Assertions.assertThat(node.tokens()).hasSize(numTokens);
                            Assertions.assertThat(allTokens).doesNotContainAnyElementsOf(node.tokens());
                            allTokens.addAll(node.tokens());
                        }
                    }
                }
            }
        }
    }

    private static int[] makeRackCountArray(int nodes, int racks)
    {
        assert nodes > 0;
        assert racks > 0;
        // Distribute nodes among the racks in round-robin fashion in the order the user is supposed to start them.
        int[] rackCounts = new int[racks];
        int rack = 0;
        for (int node = 0; node < nodes; node++)
        {
            rackCounts[rack]++;
            if (++rack == racks)
                rack = 0;
        }
        return rackCounts;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTokenGenerator_more_rf_than_racks()
    {
        OfflineTokenAllocator.allocate(3, 16, new int[]{1, 1}, FAIL_ON_WARN_OUTPUT, Murmur3Partitioner.instance);
    }


    @Test(expected = IllegalArgumentException.class)
    public void testTokenGenerator_more_rf_than_nodes()
    {
        OfflineTokenAllocator.allocate(3, 16, new int[]{2}, FAIL_ON_WARN_OUTPUT, Murmur3Partitioner.instance);
    }

    @Test
    public void testTokenGenerator_single_rack_or_single_rf()
    {
        // Simple cases, single rack or single replication.
        OfflineTokenAllocator.allocate(1, 16, new int[]{1}, FAIL_ON_WARN_OUTPUT, Murmur3Partitioner.instance);
        OfflineTokenAllocator.allocate(1, 16, new int[]{1, 1}, FAIL_ON_WARN_OUTPUT, Murmur3Partitioner.instance);
        OfflineTokenAllocator.allocate(2, 16, new int[]{2}, FAIL_ON_WARN_OUTPUT, Murmur3Partitioner.instance);
    }

    @Test
    public void testTokenGenerator_unbalanced_racks()
    {
        OfflineTokenAllocator.allocate(1, 16, new int[]{5, 1}, FAIL_ON_WARN_OUTPUT, Murmur3Partitioner.instance);
        OfflineTokenAllocator.allocate(1, 16, new int[]{5, 1, 1}, FAIL_ON_WARN_OUTPUT, Murmur3Partitioner.instance);
        OfflineTokenAllocator.allocate(3, 16, new int[]{5, 1}, FAIL_ON_WARN_OUTPUT, Murmur3Partitioner.instance);
        OfflineTokenAllocator.allocate(3, 16, new int[]{5, 1, 1}, FAIL_ON_WARN_OUTPUT, Murmur3Partitioner.instance);
    }
}
