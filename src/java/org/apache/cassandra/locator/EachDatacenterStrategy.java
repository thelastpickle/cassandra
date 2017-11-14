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
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.TokenMetadata.Topology;
import org.apache.cassandra.utils.FBUtilities;

import com.google.common.collect.Multimap;

/**
 * <p>
 * This Replication Strategy takes a property file that gives the intended
 * replication factor in each datacenter.  The sum total of the datacenter
 * replication factor values should be equal to the keyspace replication
 * factor.
 * </p>
 * <p>
 * So for example, if the keyspace replication factor is 6, the
 * datacenter replication factors could be 3, 2, and 1 - so 3 replicas in
 * one datacenter, 2 in another, and 1 in another - totalling 6.
 * </p>
 * This class also caches the Endpoints and invalidates the cache if there is a
 * change in the number of tokens.
 */
public class EachDatacenterStrategy extends AbstractReplicationStrategy
{
    private final IEndpointSnitch snitch;
    private Map<String, Integer> datacenters;
    private final ReentrantReadWriteLock datacentersLock = new ReentrantReadWriteLock();
    private final Lock datacentersReadLock = datacentersLock.readLock();
    private final Lock datacentersWriteLock = datacentersLock.writeLock();
    // track when the token range changes, signaling we need to rebuild the datacenter list
    private volatile long lastInvalidatedVersion = -1;
    private static final Logger logger = LoggerFactory.getLogger(EachDatacenterStrategy.class);
    private static final String REPLICATION_FACTOR_STRING = "replication_factor";
    private static final String EXCLUDED_DATACENTERS_STRING = "exclusions";

    public EachDatacenterStrategy(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch,
            Map<String, String> configOptions) throws ConfigurationException
    {
        super(keyspaceName, tokenMetadata, snitch, configOptions);
        this.snitch = snitch;
        validateOptions();
        computeReplicationFactorPerDc(tokenMetadata);

    }

    /**
     * Computes the map of datacenters with the associated replication factor.
     *
     *
     * @param tokenMetadata
     */
    private void computeReplicationFactorPerDc(TokenMetadata tokenMetadata)
    {
        long lastVersion = tokenMetadata.getRingVersion();
        logger.debug("lastVersion {} / lastInvalidatedVersion {}", lastVersion, lastInvalidatedVersion);
        if (lastVersion != lastInvalidatedVersion)
        {
            datacentersWriteLock.lock();
            try
            {
                checkAgainAndComputeRf(lastVersion, tokenMetadata);
            }
            finally
            {
                datacentersWriteLock.unlock();
            }
        }
    }

    private void checkAgainAndComputeRf(long lastVersion, TokenMetadata tokenMetadata)
    {
        if (lastVersion != lastInvalidatedVersion)
        {
            logger.debug("Updating topology for keyspace {}", this.keyspaceName);
            Map<String, Integer> newDatacenters = new HashMap<String, Integer>();
            TokenMetadata tokenMetadataCopy = tokenMetadata.cloneOnlyTokenMap();
            if (configOptions != null)
            {
                Integer replicas = Integer.valueOf(configOptions.get(REPLICATION_FACTOR_STRING));
                Set<String> datacenterSet = tokenMetadataCopy.getTopology().getDatacenterEndpoints()
                        .keySet();
                for (String dc : datacenterSet)
                {
                    if (!getExcludedDatacenters().contains(dc))
                    {
                        newDatacenters.put(dc, replicas);
                    }
                }
            }

            this.datacenters = Collections.unmodifiableMap(newDatacenters);
            lastInvalidatedVersion = lastVersion;
            logger.trace("Configured datacenter replicas are {}", FBUtilities.toString(this.datacenters));
        }
    }

    private Collection<String> getExcludedDatacenters()
    {
        return Arrays.asList(configOptions.getOrDefault(EXCLUDED_DATACENTERS_STRING, "").replace(" ", "").split(","));
    }

    /**
     * calculate endpoints in one pass through the tokens by tracking our progress in each DC, rack etc.
     */
    @Override
    @SuppressWarnings("serial")
    public List<InetAddress> calculateNaturalEndpoints(Token searchToken, TokenMetadata tokenMetadata)
    {
        computeReplicationFactorPerDc(tokenMetadata);
        // we want to preserve insertion order so that the first added endpoint becomes primary
        Set<InetAddress> replicas = new LinkedHashSet<>();
        datacentersReadLock.lock();
        try
        {
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
            while (tokenIter.hasNext() && !hasSufficientReplicas(dcReplicas, allEndpoints))
            {
                Token next = tokenIter.next();
                InetAddress ep = tokenMetadata.getEndpoint(next);
                String dc = snitch.getDatacenter(ep);
                // have we already found all replicas for this dc?
                if (!datacenters.containsKey(dc) || hasSufficientReplicas(dc, dcReplicas, allEndpoints))
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
                            while (skippedIt.hasNext() && !hasSufficientReplicas(dc, dcReplicas, allEndpoints))
                            {
                                InetAddress nextSkipped = skippedIt.next();
                                dcReplicas.get(dc).add(nextSkipped);
                                replicas.add(nextSkipped);
                            }
                        }
                    }
                }
            }
        }
        finally
        {
            datacentersReadLock.unlock();
        }

        return new ArrayList<InetAddress>(replicas);
    }

    private boolean hasSufficientReplicas(String dc, Map<String, Set<InetAddress>> dcReplicas, Multimap<String, InetAddress> allEndpoints)
    {
        return dcReplicas.get(dc).size() >= Math.min(allEndpoints.get(dc).size(), getReplicationFactor(dc));
    }

    private boolean hasSufficientReplicas(Map<String, Set<InetAddress>> dcReplicas, Multimap<String, InetAddress> allEndpoints)
    {
        datacentersReadLock.lock();
        try
        {
            for (String dc : datacenters.keySet())
                if (!hasSufficientReplicas(dc, dcReplicas, allEndpoints))
                    return false;
            return true;
        }
        finally
        {
            datacentersReadLock.unlock();
        }
    }

    @Override
    public int getReplicationFactor()
    {
        datacentersReadLock.lock();
        try
        {
            int total = 0;
            for (int repFactor : datacenters.values())
                total += repFactor;
            return total;
        }
        finally
        {
            datacentersReadLock.unlock();
        }
    }

    public int getReplicationFactor(String dc)
    {
        datacentersReadLock.lock();
        try
        {
            Integer replicas = datacenters.get(dc);
            return replicas == null ? 0 : replicas;
        }
        finally
        {
            datacentersReadLock.unlock();
        }
    }

    public Set<String> getDatacenters()
    {
        datacentersReadLock.lock();
        try
        {
            return datacenters.keySet();
        }
        finally
        {
            datacentersReadLock.unlock();
        }
    }

    @Override
    public void validateOptions() throws ConfigurationException
    {
        for (Entry<String, String> e : this.configOptions.entrySet())
        {
            if (!e.getKey().equalsIgnoreCase(REPLICATION_FACTOR_STRING)
                    && !e.getKey().equalsIgnoreCase(EXCLUDED_DATACENTERS_STRING))
                throw new ConfigurationException(
                        "replication_factor and except are the only valid options for EachDatacenterStrategy");
            if (e.getKey().equalsIgnoreCase(REPLICATION_FACTOR_STRING))
                validateReplicationFactor(e.getValue());
        }
    }

    @Override
    public Collection<String> recognizedOptions()
    {
        return Arrays.asList("replication_factor", "except");
    }

    @Override
    public boolean hasSameSettings(AbstractReplicationStrategy other)
    {
        datacentersReadLock.lock();
        try
        {
            return super.hasSameSettings(other) && ((EachDatacenterStrategy) other).datacenters.equals(datacenters);
        }
        finally
        {
            datacentersReadLock.unlock();
        }
    }
}
