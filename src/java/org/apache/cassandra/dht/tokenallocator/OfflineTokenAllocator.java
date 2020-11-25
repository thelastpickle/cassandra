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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.locator.TokenMetadata;

public class OfflineTokenAllocator
{
    private static final Logger logger = LoggerFactory.getLogger(OfflineTokenAllocator.class);

    public static List<FakeNode> allocate(int rf, int numTokens, int[] nodesPerRack, boolean verbose)
    {
        List<FakeNode> fakeNodes = new ArrayList<>(Arrays.stream(nodesPerRack).sum());
        MultinodeAllocator allocator = new MultinodeAllocator(rf, numTokens, verbose);

        int racks = nodesPerRack.length;
        int nodeId = 0;
        int rackId = 0;
        // Distribute nodes among the racks in round-robin fashion in the order the user is supposed to start them.
        while (nodesPerRack[rackId] > 0)
        {
            // Allocate tokens for current node
            fakeNodes.add(allocator.allocateTokensForNode(nodeId++, rackId));

            // Find next rack with unallocated node
            int nextRack = (rackId+1) % racks;
            while (nodesPerRack[nextRack] == 0 && nextRack != rackId)
                nextRack = (nextRack+1) % racks;

            // Update nodesPerRack and rackId
            nodesPerRack[rackId]--;
            rackId = nextRack;
        }
        allocator.validateAllocation();
        return fakeNodes;
    }

    public static class FakeNode
    {
        protected final InetAddressAndPort fakeAddress;
        private final int rackId;
        private final Collection<Token> tokens;

        public FakeNode(InetAddressAndPort address, Integer rackId, Collection<Token> tokens)
        {
            this.fakeAddress = address;
            this.rackId = rackId;
            this.tokens = tokens;
        }

        public int nodeId()
        {
            return fakeAddress.port;
        }

        public int rackId()
        {
            return this.rackId;
        }

        public Collection<Token> tokens()
        {
            return tokens;
        }
    }

    static class MultinodeAllocator
    {
        final FakeSnitch fakeSnitch;
        final TokenMetadata fakeMetadata;
        final TokenAllocation allocation;
        final Set<Integer> allocatedRacks = Sets.newHashSet();
        final Map<Integer, SummaryStatistics> lastCheckPoint = Maps.newHashMap();
        private final boolean verbose;

        public MultinodeAllocator(int rf, int numTokens, boolean verbose)
        {
            this.fakeSnitch = new FakeSnitch();
            this.fakeMetadata = new TokenMetadata(fakeSnitch);
            this.allocation = TokenAllocation.create(fakeSnitch, fakeMetadata, rf, numTokens, false);
            this.verbose = verbose;
        }

        public FakeNode allocateTokensForNode(Integer nodeId, Integer rackId)
        {
            // Verify if allocation is balanced after each round across all racks
            if (allocatedRacks.contains(rackId))
            {
                validateAllocation();
                allocatedRacks.clear();
            }

            // Update snitch and token metadata info
            InetAddressAndPort fakeNodeAddress = getLoopbackAddressWithPort(nodeId);
            fakeSnitch.nodeByRack.put(fakeNodeAddress, rackId);
            fakeMetadata.updateTopology(fakeNodeAddress);

            // Allocate tokens
            Collection<Token> tokens = allocation.allocate(fakeNodeAddress);
            if (verbose)
                logger.warn("Allocated {} tokens for node {} on rack {}.", tokens.size(), nodeId, rackId);
            allocatedRacks.add(rackId);

            return new FakeNode(fakeNodeAddress, rackId, tokens);
        }

        public void validateAllocation()
        {
            for (Integer allocatedRackId : allocatedRacks)
            {
                SummaryStatistics newOwnership = allocation.getAllocationRingOwnership(SimpleSnitch.DATA_CENTER_NAME, Integer.toString(allocatedRackId));
                SummaryStatistics oldOwnership = lastCheckPoint.put(allocatedRackId, newOwnership);
                if (verbose)
                {
                    if (oldOwnership != null)
                        logger.warn("Replicated node load in rack={} before allocation: {}", allocatedRackId, TokenAllocation.statToString(oldOwnership));
                    logger.warn("Replicated node load in rack={} after allocation: {}", allocatedRackId, TokenAllocation.statToString(newOwnership));
                }
                if (oldOwnership != null && oldOwnership.getStandardDeviation() != 0.0 && newOwnership.getStandardDeviation() > oldOwnership.getStandardDeviation())
                {
                    logger.warn("Unexpected growth in standard deviation on rack {} from {} to {}.", allocatedRackId, String.format("%.5f", oldOwnership.getStandardDeviation()),
                                 String.format("%.5f", newOwnership.getStandardDeviation()));
                }
            }
        }
    }

    static class FakeSnitch extends SimpleSnitch
    {
        final Map<InetAddressAndPort, Integer> nodeByRack = new HashMap<>();

        @Override
        public String getRack(InetAddressAndPort endpoint)
        {
            return Integer.toString(nodeByRack.get(endpoint));
        }
    }

    private static InetAddressAndPort getLoopbackAddressWithPort(int port)
    {
        try
        {
            return InetAddressAndPort.getByAddressOverrideDefaults(InetAddress.getByName("127.0.0.1"), port);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e); //shouldn't happen
        }
    }
}