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
package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.TokenMetadata.Topology;
import org.apache.cassandra.utils.FBUtilities;

import com.google.common.collect.Multimap;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.service.StorageService;

/**
 * <p>
 * This Replication Strategy takes a property file that gives the same replication factor to use in each datacenter.
 * </p>
 * <p>
 * Each newly created datacenter will automatically have the same replication factor as the existing ones.
 * </p>
 * <p>
 * This class also caches the Endpoints and invalidates the cache if there is a change in the number of tokens. Such
 * change would also trigger the computation of the {datacenter:replication factor} map as the topology has changed and
 * datacenters may have been added/removed.
 * </p>
 * <p>
 * This replication strategy is largely based off NetworkTopologyStrategy which it subclasses to ease compatibility with
 * the rest of the codebase (eg ConsistencyLevel.java)
 * </p>
 */
public final class EachDatacenterStrategy extends NetworkTopologyStrategy
{
    public static final String REPLICATION_FACTOR_STRING = "each_replication_factor";
    public static final String EXCLUDED_DATACENTERS_STRING = "exclusions";

    private static final Logger logger = LoggerFactory.getLogger(EachDatacenterStrategy.class);

    private final IEndpointSnitch snitch;
    private final Map<String, String> configOptions;
    private final TokenMetadata tokenMetadata;

    private final AtomicReference<Map<String, Integer>> datacenters = new AtomicReference<>();

    // track when the token range changes, signaling we need to rebuild the datacenter list
    private volatile long lastInvalidatedVersion = -1;

    public EachDatacenterStrategy(String keyspaceName,
                                  TokenMetadata tokenMetadata,
                                  IEndpointSnitch snitch,
                                  Map<String, String> configOptions) throws ConfigurationException
    {
        super(keyspaceName, tokenMetadata, snitch, Collections.<String, String> singletonMap(REPLICATION_FACTOR_STRING, configOptions.get(REPLICATION_FACTOR_STRING)));
        this.configOptions = configOptions;
        this.snitch = snitch;
        this.tokenMetadata = tokenMetadata;
        validateOptions();
        assert StorageService.instance.getTokenMetadata() == tokenMetadata;
        updateReplicationFactorPerDc();
    }

    public ArrayList<InetAddress> getNaturalEndpoints(RingPosition searchPosition)
    {
        // tokenMetadata can have internally changed (ie ringVersion is incremented), and
        //  the super method clears the cache and calls `calculateNaturalEndpoints(..)` with a cloned `tokenMetadata`.
        // So we must first call `computeReplicationFactorPerDc(..)` with the real (original) `tokenMetadata`
        //  to ensure datacenters gets updated appropriately.
        updateReplicationFactorPerDc();
        return super.getNaturalEndpoints(searchPosition);
    }

    /**
     * Computes and saves the map of datacenters with the associated replication factor.
     */
    private Map<String, Integer> updateReplicationFactorPerDc()
    {

        long lastVersion = tokenMetadata.getRingVersion();
        if (lastVersion > lastInvalidatedVersion)
        {
            Map<String, Integer> newDatacenters = computeReplicationFactorPerDc(tokenMetadata);
            datacenters.set(newDatacenters);
            lastInvalidatedVersion = lastVersion;
            logger.info("Updated topology: configured datacenter replicas are {}", FBUtilities.toString(newDatacenters));
            return newDatacenters;
        }
        return datacenters.get();
    }

    /**
     * Computes the map of datacenters with the associated replication factor.
     *
     *
     * @param tokenMetadata
     */
    private Map<String, Integer> computeReplicationFactorPerDc(TokenMetadata tokenMetadata)
    {
        Map<String, Integer> newDatacenters = new HashMap<>();

        if (configOptions != null)
        {
            Integer replicas = Integer.valueOf(configOptions.get(REPLICATION_FACTOR_STRING));
            Set<String> datacenterSet = tokenMetadata.cloneOnlyTokenMap().getTopology().getDatacenterEndpoints().keySet();
            for (String dc : datacenterSet)
            {
                if (!getExcludedDatacenters().contains(dc))
                    newDatacenters.put(dc, replicas);
            }
        }

        logger.trace("Configured datacenter replicas are {}", FBUtilities.toString(newDatacenters));
        return Collections.unmodifiableMap(newDatacenters);
    }

    private Collection<String> getExcludedDatacenters()
    {
        return Arrays.asList(configOptions.getOrDefault(EXCLUDED_DATACENTERS_STRING, "").replace(" ", "").split(","));
    }

    /**
     * calculate endpoints in one pass through the tokens by tracking our progress in each DC, rack etc.
     */
    @SuppressWarnings("serial")
    public List<InetAddress> calculateNaturalEndpoints(Token searchToken, TokenMetadata tokenMetadata)
    {
        Map<String, Integer> datacenters = computeReplicationFactorPerDc(tokenMetadata);
        // we want to preserve insertion order so that the first added endpoint becomes primary
        Set<InetAddress> replicas = new LinkedHashSet<>();

        // replicas we have found in each DC
        Map<String, Set<InetAddress>> dcReplicas = new HashMap<>(datacenters.size());
        for (Map.Entry<String, Integer> dc : datacenters.entrySet())
            dcReplicas.put(dc.getKey(), new HashSet<InetAddress>(dc.getValue()));

        Topology topology = tokenMetadata.getTopology();
        // all endpoints in each DC, so we can check when we have exhausted all the members of a DC
        Multimap<String, InetAddress> allEndpoints = topology.getDatacenterEndpoints();
        // all racks in a DC so we can check when we have exhausted all racks in a DC
        Map<String, Multimap<String, InetAddress>> racks = topology.getDatacenterRacks();
        assert !allEndpoints.isEmpty() && !racks.isEmpty() : "not aware of any cluster members";

        // tracks the racks we have already placed replicas in
        Map<String, Set<String>> seenRacks = new HashMap<>(datacenters.size());
        for (Map.Entry<String, Integer> dc : datacenters.entrySet())
            seenRacks.put(dc.getKey(), new HashSet<String>());

        // tracks the endpoints that we skipped over while looking for unique racks
        // when we relax the rack uniqueness we can append this to the current result so we don't have to wind back
        // the iterator
        Map<String, Set<InetAddress>> skippedDcEndpoints = new HashMap<>(datacenters.size());
        for (Map.Entry<String, Integer> dc : datacenters.entrySet())
            skippedDcEndpoints.put(dc.getKey(), new LinkedHashSet<InetAddress>());

        Iterator<Token> tokenIter = TokenMetadata.ringIterator(tokenMetadata.sortedTokens(), searchToken, false);
        while (tokenIter.hasNext() && !hasSufficientReplicas(dcReplicas, allEndpoints, datacenters))
        {
            Token next = tokenIter.next();
            InetAddress ep = tokenMetadata.getEndpoint(next);
            String dc = snitch.getDatacenter(ep);
            // have we already found all replicas for this dc?
            if (!datacenters.containsKey(dc) || hasSufficientReplicas(dc, dcReplicas, allEndpoints, datacenters))
                continue;
            // can we skip checking the rack?
            if (seenRacks.get(dc).size() == racks.get(dc).keySet().size())
            {
                dcReplicas.get(dc).add(ep);
                replicas.add(ep);
            }
            else
            {
                String rack = snitch.getRack(ep);
                // is this a new rack?
                if (seenRacks.get(dc).contains(rack))
                {
                    skippedDcEndpoints.get(dc).add(ep);
                }
                else
                {
                    dcReplicas.get(dc).add(ep);
                    replicas.add(ep);
                    seenRacks.get(dc).add(rack);
                    // if we've run out of distinct racks, add the hosts we skipped past already (up to RF)
                    if (seenRacks.get(dc).size() == racks.get(dc).keySet().size())
                    {
                        Iterator<InetAddress> skippedIt = skippedDcEndpoints.get(dc).iterator();
                        while (skippedIt.hasNext() && !hasSufficientReplicas(dc, dcReplicas, allEndpoints, datacenters))
                        {
                            InetAddress nextSkipped = skippedIt.next();
                            dcReplicas.get(dc).add(nextSkipped);
                            replicas.add(nextSkipped);
                        }
                    }
                }
            }
        }

        return new ArrayList<>(replicas);
    }

    private static boolean hasSufficientReplicas(String dc,
                                                 Map<String, Set<InetAddress>> dcReplicas,
                                                 Multimap<String, InetAddress> allEndpoints,
                                                 Map<String, Integer> datacenters)
    {
        int replicas = datacenters.get(dc) == null ? 0 : datacenters.get(dc);
        return dcReplicas.get(dc).size() >= Math.min(allEndpoints.get(dc).size(), replicas);
    }

    private static boolean hasSufficientReplicas(Map<String,
                                                 Set<InetAddress>> dcReplicas,
                                                 Multimap<String, InetAddress> allEndpoints,
                                                 Map<String, Integer> datacenters)
    {
        for (String dc : datacenters.keySet())
            if (!hasSufficientReplicas(dc, dcReplicas, allEndpoints, datacenters))
                return false;
        return true;
    }

    public int getReplicationFactor()
    {
        int total = 0;
        for (int repFactor : datacenters.get().values())
            total += repFactor;
        return total;
    }

    public int getReplicationFactor(String dc)
    {
        Integer replicas = datacenters.get().get(dc);
        return replicas == null ? 0 : replicas;
    }

    public Set<String> getDatacenters()
    {
        return datacenters.get().keySet();
    }

    public void validateOptions() throws ConfigurationException
    {
        for (Entry<String, String> e : this.configOptions.entrySet())
        {
            if (!e.getKey().equalsIgnoreCase(REPLICATION_FACTOR_STRING)
                    && !e.getKey().equalsIgnoreCase(EXCLUDED_DATACENTERS_STRING))
                throw new ConfigurationException(
                        REPLICATION_FACTOR_STRING + " and " + EXCLUDED_DATACENTERS_STRING
                                + " are the only valid options for EachDatacenterStrategy");
            if (e.getKey().equalsIgnoreCase(REPLICATION_FACTOR_STRING))
                validateReplicationFactor(e.getValue());
        }

        if (!this.configOptions.containsKey(REPLICATION_FACTOR_STRING))
            throw new ConfigurationException(
                    REPLICATION_FACTOR_STRING + " is a required option for EachDatacenterStrategy");
    }

    public Collection<String> recognizedOptions()
    {
        return Arrays.asList(REPLICATION_FACTOR_STRING, EXCLUDED_DATACENTERS_STRING);
    }

    public boolean hasSameSettings(AbstractReplicationStrategy other)
    {
        return super.hasSameSettings(other) && ((EachDatacenterStrategy) other).datacenters.equals(datacenters);
    }
}
