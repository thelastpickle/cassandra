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

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.tokenallocator.TokenAllocatorBase.TokenInfo;
import org.apache.cassandra.dht.tokenallocator.TokenAllocatorBase.UnitInfo;
import org.apache.cassandra.dht.tokenallocator.TokenAllocatorBase.Weighted;
import org.apache.cassandra.diag.DiagnosticEvent;
import org.apache.cassandra.diag.DiagnosticEventService;

/**
 * DiagnosticEvent implementation for {@link TokenAllocator} activities.
 */
public class TokenAllocatorEvent<Unit> extends DiagnosticEvent
{

    private final TokenAllocatorEventType type;
    private final TokenAllocatorBase<Unit> allocator;
    private final int replicas;
    private final Integer numTokens;
    private final Collection<Weighted<UnitInfo>> sortedUnits;
    private final Map<Unit, Collection<Token>> unitToTokens;
    private final ImmutableMap<Token, Unit> sortedTokens;
    private final List<Token> tokens;
    private final Unit unit;
    private final TokenInfo<Unit> tokenInfo;

    private TokenAllocatorEvent(TokenAllocatorEventType type, TokenAllocatorBase<Unit> allocator, Integer numTokens,
                                ImmutableList<Weighted<UnitInfo>> sortedUnits, ImmutableMap<Unit, Collection<Token>> unitToTokens,
                                ImmutableMap<Token, Unit> sortedTokens, ImmutableList<Token> tokens, Unit unit,
                                TokenInfo<Unit> tokenInfo)
    {
        this.type = type;
        this.allocator = allocator;
        this.replicas = allocator.getReplicas();
        this.numTokens = numTokens;
        this.sortedUnits = sortedUnits;
        this.unitToTokens = unitToTokens;
        this.sortedTokens = sortedTokens;
        this.tokens = tokens;
        this.unit = unit;
        this.tokenInfo = tokenInfo;
    }

    public static <Unit> void noReplicationTokenAllocatorInstanciated(NoReplicationTokenAllocator<Unit> allocator)
    {
        if (isEnabled(TokenAllocatorEventType.NO_REPLICATION_AWARE_TOKEN_ALLOCATOR_INSTANCIATED))
            DiagnosticEventService.instance().publish(new TokenAllocatorEvent<>(TokenAllocatorEventType.NO_REPLICATION_AWARE_TOKEN_ALLOCATOR_INSTANCIATED, allocator, null, null, null, null, null, null, null));
    }

    public static <Unit> void replicationTokenAllocatorInstanciated(ReplicationAwareTokenAllocator<Unit> allocator)
    {
        if (isEnabled(TokenAllocatorEventType.REPLICATION_AWARE_TOKEN_ALLOCATOR_INSTANCIATED))
            DiagnosticEventService.instance().publish(new TokenAllocatorEvent<>(TokenAllocatorEventType.REPLICATION_AWARE_TOKEN_ALLOCATOR_INSTANCIATED, allocator, null, null, null,null, null, null, null));
    }

    public static <Unit> void unitedAdded(TokenAllocatorBase<Unit> allocator, int numTokens,
                                          Queue<Weighted<UnitInfo>> sortedUnits, NavigableMap<Token, Unit> sortedTokens,
                                          List<Token> tokens, Unit unit)
    {
        if (isEnabled(TokenAllocatorEventType.UNIT_ADDED))
            DiagnosticEventService.instance().publish(new TokenAllocatorEvent<>(TokenAllocatorEventType.UNIT_ADDED,
                                                                     allocator,
                                                                     numTokens,
                                                                     ImmutableList.copyOf(sortedUnits),
                                                                     null,
                                                                     ImmutableMap.copyOf(sortedTokens),
                                                                     ImmutableList.copyOf(tokens),
                                                                     unit,
                                                                     null));
    }

    public static <Unit> void unitedAdded(TokenAllocatorBase<Unit> allocator, int numTokens,
                                          Multimap<Unit, Token> unitToTokens, NavigableMap<Token, Unit> sortedTokens,
                                          List<Token> tokens, Unit unit)
    {
        if (isEnabled(TokenAllocatorEventType.UNIT_ADDED))
            DiagnosticEventService.instance().publish(new TokenAllocatorEvent<>(TokenAllocatorEventType.UNIT_ADDED,
                                                                     allocator,
                                                                     numTokens,
                                                                     null,
                                                                     ImmutableMap.copyOf(unitToTokens.asMap()),
                                                                     ImmutableMap.copyOf(sortedTokens),
                                                                     ImmutableList.copyOf(tokens),
                                                                     unit,
                                                                     null));
    }


    public static <Unit> void unitRemoved(TokenAllocatorBase<Unit> allocator, Unit unit,
                                          Queue<Weighted<UnitInfo>> sortedUnits, Map<Token, Unit> sortedTokens)
    {
        if (isEnabled(TokenAllocatorEventType.UNIT_REMOVED))
            DiagnosticEventService.instance().publish(new TokenAllocatorEvent<>(TokenAllocatorEventType.UNIT_REMOVED,
                                                                     allocator,
                                                                     null,
                                                                     ImmutableList.copyOf(sortedUnits),
                                                                     null,
                                                                     ImmutableMap.copyOf(sortedTokens),
                                                                     null,
                                                                     unit,
                                                                     null));
    }

    public static <Unit> void unitRemoved(TokenAllocatorBase<Unit> allocator, Unit unit,
                                          Multimap<Unit, Token> unitToTokens, Map<Token, Unit> sortedTokens)
    {
        if (isEnabled(TokenAllocatorEventType.UNIT_REMOVED))
            DiagnosticEventService.instance().publish(new TokenAllocatorEvent<>(TokenAllocatorEventType.UNIT_REMOVED,
                                                                     allocator,
                                                                     null,
                                                                     null,
                                                                     ImmutableMap.copyOf(unitToTokens.asMap()),
                                                                     ImmutableMap.copyOf(sortedTokens),
                                                                     null,
                                                                     unit,
                                                                     null));
    }

    public static <Unit> void tokenInfosCreated(TokenAllocatorBase<Unit> allocator, Queue<Weighted<UnitInfo>> sortedUnits,
                                                Map<Token, Unit> sortedTokens, TokenInfo<Unit> tokenInfo)
    {
        if (isEnabled(TokenAllocatorEventType.TOKEN_INFOS_CREATED))
            DiagnosticEventService.instance().publish(new TokenAllocatorEvent<>(TokenAllocatorEventType.TOKEN_INFOS_CREATED,
                                                                     allocator,
                                                                     null,
                                                                     ImmutableList.copyOf(sortedUnits),
                                                                     null,
                                                                     ImmutableMap.copyOf(sortedTokens),
                                                                     null,
                                                                     null,
                                                                     tokenInfo));
    }

    public static <Unit> void tokenInfosCreated(TokenAllocatorBase<Unit> allocator, Multimap<Unit, Token> unitToTokens,
                                                TokenInfo<Unit> tokenInfo)
    {
        if (isEnabled(TokenAllocatorEventType.TOKEN_INFOS_CREATED))
            DiagnosticEventService.instance().publish(new TokenAllocatorEvent<>(TokenAllocatorEventType.TOKEN_INFOS_CREATED,
                                                                     allocator,
                                                                     null,
                                                                     null,
                                                                     ImmutableMap.copyOf(unitToTokens.asMap()),
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     tokenInfo));
    }

    public static <Unit> void randomTokensGenerated(TokenAllocatorBase<Unit> allocator,
                                                    int numTokens, Queue<Weighted<UnitInfo>> sortedUnits,
                                                    NavigableMap<Token, Unit> sortedTokens, Unit newUnit,
                                                    Set<Token> tokens)
    {
        if (isEnabled(TokenAllocatorEventType.RANDOM_TOKENS_GENERATED))
            DiagnosticEventService.instance().publish(new TokenAllocatorEvent<>(TokenAllocatorEventType.RANDOM_TOKENS_GENERATED,
                                                                     allocator,
                                                                     numTokens,
                                                                     ImmutableList.copyOf(sortedUnits),
                                                                     null,
                                                                     ImmutableMap.copyOf(sortedTokens),
                                                                     ImmutableList.copyOf(tokens),
                                                                     newUnit,
                                                                     null));
    }

    public static <Unit> void randomTokensGenerated(TokenAllocatorBase<Unit> allocator,
                                                    int numTokens, Multimap<Unit, Token> unitToTokens,
                                                    NavigableMap<Token, Unit> sortedTokens, Unit newUnit,
                                                    Set<Token> tokens)
    {
        if (isEnabled(TokenAllocatorEventType.RANDOM_TOKENS_GENERATED))
            DiagnosticEventService.instance().publish(new TokenAllocatorEvent<>(TokenAllocatorEventType.RANDOM_TOKENS_GENERATED,
                                                                     allocator,
                                                                     numTokens,
                                                                     null,
                                                                     ImmutableMap.copyOf(unitToTokens.asMap()),
                                                                     ImmutableMap.copyOf(sortedTokens),
                                                                     ImmutableList.copyOf(tokens),
                                                                     newUnit,
                                                                     null));
    }

    private static boolean isEnabled(TokenAllocatorEventType type)
    {
        return DiagnosticEventService.instance().isEnabled(TokenAllocatorEvent.class, type);
    }

    public enum TokenAllocatorEventType
    {
        REPLICATION_AWARE_TOKEN_ALLOCATOR_INSTANCIATED,
        NO_REPLICATION_AWARE_TOKEN_ALLOCATOR_INSTANCIATED,
        UNIT_ADDED,
        UNIT_REMOVED,
        TOKEN_INFOS_CREATED,
        RANDOM_TOKENS_GENERATED,
        TOKENS_ALLOCATED
    }

    public Enum<?> getType()
    {
        return type;
    }

    public Object getSource()
    {
        return allocator;
    }

    public HashMap<String, Serializable> toMap()
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        if (allocator != null)
        {
            if (allocator.partitioner != null) ret.put("partitioner", allocator.partitioner.getClass().getSimpleName());
            if (allocator.strategy != null) ret.put("strategy", allocator.strategy.getClass().getSimpleName());
        }
        ret.put("replicas", replicas);
        ret.put("numTokens", this.numTokens);
        ret.put("sortedUnits", String.valueOf(sortedUnits));
        ret.put("sortedTokens", String.valueOf(sortedTokens));
        ret.put("unitToTokens", String.valueOf(unitToTokens));
        ret.put("tokens", String.valueOf(tokens));
        ret.put("unit", String.valueOf(unit));
        ret.put("tokenInfo", String.valueOf(tokenInfo));
        return ret;
    }
}