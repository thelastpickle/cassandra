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
import java.util.stream.Collectors;

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
public class EachDatacenterStrategy extends AbstractNetworkTopologyStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(EachDatacenterStrategy.class);

    public EachDatacenterStrategy(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch,
            Map<String, String> configOptions) throws ConfigurationException
    {
        super(keyspaceName, tokenMetadata, snitch, configOptions);
        this.snitch = snitch;

        Map<String, Integer> newDatacenters = new HashMap<String, Integer>();
        if (configOptions != null)
        {
            for (Entry<String, String> entry : configOptions.entrySet())
            {
                String rf = entry.getKey();
                if (!rf.equalsIgnoreCase("replication_factor"))
                    throw new ConfigurationException(
                            "specific dc names should be used for NetworkTopologyStrategy, not EachDatacenterStrategy");
                Integer replicas = Integer.valueOf(entry.getValue());
                Set<String> datacenterSet = tokenMetadata.getTopology().getDatacenterEndpoints().keySet();
                for (String dc : datacenterSet) {
                    newDatacenters.put(dc, replicas);
                }
            }
        }

        this.datacenters = Collections.unmodifiableMap(newDatacenters);
        logger.trace("Configured datacenter replicas are {}", FBUtilities.toString(this.datacenters));
    }
}
