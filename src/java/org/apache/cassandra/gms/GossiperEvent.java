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

package org.apache.cassandra.gms;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.diag.DiagnosticEvent;
import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * DiagnosticEvent implementation for {@link Gossiper} activities.
 */
public class GossiperEvent extends DiagnosticEvent
{
    private final InetAddressAndPort endpoint;
    private final Long quarantineExpiration;
    private final EndpointState localState;

    private final Map<InetAddressAndPort, EndpointState> endpointStateMap;
    private final boolean inShadowRound;
    private final Map<InetAddressAndPort, Long> justRemovedEndpoints;
    private final long lastProcessedMessageAt;
    private final Set<InetAddressAndPort> liveEndpoints;
    private final Set<InetAddressAndPort> seeds;
    private final Set<InetAddressAndPort> seedsInShadowRound;
    private final Map<InetAddressAndPort, Long> unreachableEndpoints;


    public enum GossiperEventType
    {
        MARKED_AS_SHUTDOWN,
        CONVICTED,
        REPLACEMENT_QUARANTINE,
        REPLACED_ENDPOINT,
        EVICTED_FROM_MEMBERSHIP,
        REMOVED_ENDPOINT,
        QUARANTINED_ENDPOINT,
        MARKED_ALIVE,
        REAL_MARKED_ALIVE,
        MARKED_DEAD,
        MAJOR_STATE_CHANGE_HANDLED,
        SEND_GOSSIP_DIGEST_SYN
    }

    public GossiperEventType type;


    private GossiperEvent(GossiperEventType type, Gossiper gossiper, InetAddressAndPort endpoint, Long quarantineExpiration,
                          EndpointState localState)
    {
        this.type = type;
        this.endpoint = endpoint;
        this.quarantineExpiration = quarantineExpiration;
        this.localState = localState;

        this.endpointStateMap = ImmutableMap.copyOf(gossiper.endpointStateMap);
        this.inShadowRound = gossiper.inShadowRound;
        this.justRemovedEndpoints = ImmutableMap.copyOf(gossiper.justRemovedEndpoints);
        this.lastProcessedMessageAt = gossiper.lastProcessedMessageAt;
        this.liveEndpoints = ImmutableSet.copyOf(gossiper.liveEndpoints);
        this.seeds = ImmutableSet.copyOf(gossiper.seeds);
        this.seedsInShadowRound = ImmutableSet.copyOf(gossiper.seedsInShadowRound);
        this.unreachableEndpoints = ImmutableMap.copyOf(gossiper.unreachableEndpoints);
    }


    public static void markedAsShutdown(Gossiper gossiper, InetAddressAndPort endpoint)
    {
        if (isEnabled(GossiperEventType.MARKED_AS_SHUTDOWN))
            DiagnosticEventService.instance().publish(new GossiperEvent(GossiperEventType.MARKED_AS_SHUTDOWN, gossiper, endpoint,
                                                             null, null));
    }

    public static void convicted(Gossiper gossiper, InetAddressAndPort endpoint, double phi)
    {
        if (isEnabled(GossiperEventType.CONVICTED))
            DiagnosticEventService.instance().publish(new GossiperEvent(GossiperEventType.CONVICTED, gossiper, endpoint,
                                                             null, null));
    }

    public static void replacementQuarantine(Gossiper gossiper, InetAddressAndPort endpoint)
    {
        if (isEnabled(GossiperEventType.REPLACEMENT_QUARANTINE))
            DiagnosticEventService.instance().publish(new GossiperEvent(GossiperEventType.REPLACEMENT_QUARANTINE, gossiper, endpoint,
                                                             null, null));
    }

    public static void replacedEndpoint(Gossiper gossiper, InetAddressAndPort endpoint)
    {
        if (isEnabled(GossiperEventType.REPLACED_ENDPOINT))
            DiagnosticEventService.instance().publish(new GossiperEvent(GossiperEventType.REPLACED_ENDPOINT, gossiper, endpoint,
                                                             null, null));
    }

    static void evictedFromMembership(Gossiper gossiper, InetAddressAndPort endpoint)
    {
        if (isEnabled(GossiperEventType.EVICTED_FROM_MEMBERSHIP))
            DiagnosticEventService.instance().publish(new GossiperEvent(GossiperEventType.EVICTED_FROM_MEMBERSHIP, gossiper, endpoint,
                                                             null, null));
    }

    static void removedEndpoint(Gossiper gossiper, InetAddressAndPort endpoint)
    {
        if (isEnabled(GossiperEventType.REMOVED_ENDPOINT))
            DiagnosticEventService.instance().publish(new GossiperEvent(GossiperEventType.REMOVED_ENDPOINT, gossiper, endpoint,
                                                             null, null));
    }

    static void quarantinedEndpoint(Gossiper gossiper, InetAddressAndPort endpoint, long quarantineExpiration)
    {
        if (isEnabled(GossiperEventType.QUARANTINED_ENDPOINT))
            DiagnosticEventService.instance().publish(new GossiperEvent(GossiperEventType.QUARANTINED_ENDPOINT, gossiper, endpoint,
                                                             quarantineExpiration, null));
    }

    static void markedAlive(Gossiper gossiper, InetAddressAndPort addr, EndpointState localState)
    {
        if (isEnabled(GossiperEventType.MARKED_ALIVE))
            DiagnosticEventService.instance().publish(new GossiperEvent(GossiperEventType.MARKED_ALIVE, gossiper, addr,
                                                             null, localState));
    }

    static void realMarkedAlive(Gossiper gossiper, InetAddressAndPort addr, EndpointState localState)
    {
        if (isEnabled(GossiperEventType.REAL_MARKED_ALIVE))
            DiagnosticEventService.instance().publish(new GossiperEvent(GossiperEventType.REAL_MARKED_ALIVE, gossiper, addr,
                                                             null, localState));
    }

    static void markedDead(Gossiper gossiper, InetAddressAndPort addr, EndpointState localState)
    {
        if (isEnabled(GossiperEventType.MARKED_DEAD))
            DiagnosticEventService.instance().publish(new GossiperEvent(GossiperEventType.MARKED_DEAD, gossiper, addr,
                                                             null, localState));
    }

    static void majorStateChangeHandled(Gossiper gossiper, InetAddressAndPort addr, EndpointState state)
    {
        if (isEnabled(GossiperEventType.MAJOR_STATE_CHANGE_HANDLED))
            DiagnosticEventService.instance().publish(new GossiperEvent(GossiperEventType.MAJOR_STATE_CHANGE_HANDLED, gossiper, addr,
                                                             null, state));
    }

    static void sendGossipDigestSyn(Gossiper gossiper, InetAddressAndPort to)
    {
        if (isEnabled(GossiperEventType.SEND_GOSSIP_DIGEST_SYN))
            DiagnosticEventService.instance().publish(new GossiperEvent(GossiperEventType.SEND_GOSSIP_DIGEST_SYN, gossiper, to,
                                                             null, null));
    }

    private static boolean isEnabled(GossiperEventType type)
    {
        return DiagnosticEventService.instance().isEnabled(GossiperEvent.class, type);
    }

    @Override
    public Enum<GossiperEventType> getType()
    {
        return type;
    }

    @Override
    public Object getSource()
    {
        return Gossiper.instance;
    }

    @Override
    public HashMap<String, Serializable> toMap()
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        if (endpoint != null) ret.put("endpoint", endpoint.getHostAddress(true));
        ret.put("quarantineExpiration", quarantineExpiration);
        ret.put("localState", String.valueOf(localState));
        ret.put("endpointStateMap", String.valueOf(endpointStateMap));
        ret.put("inShadowRound", inShadowRound);
        ret.put("justRemovedEndpoints", String.valueOf(justRemovedEndpoints));
        ret.put("lastProcessedMessageAt", lastProcessedMessageAt);
        ret.put("liveEndpoints", String.valueOf(liveEndpoints));
        ret.put("seeds", String.valueOf(seeds));
        ret.put("seedsInShadowRound", String.valueOf(seedsInShadowRound));
        ret.put("unreachableEndpoints", String.valueOf(unreachableEndpoints));
        return ret;
    }
}