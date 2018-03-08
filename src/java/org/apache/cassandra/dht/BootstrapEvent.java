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

package org.apache.cassandra.dht;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.diag.DiagnosticEvent;
import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;

/**
 * DiagnosticEvent implementation for bootstrap related activities.
 */
public class BootstrapEvent extends DiagnosticEvent
{

    private final BootstrapEventType type;
    private final TokenMetadata tokenMetadata;
    private final InetAddressAndPort address;
    private final String allocationKeyspace;
    private final Integer numTokens;
    private final Collection<Token> tokens;

    private BootstrapEvent(BootstrapEventType type, InetAddressAndPort address, TokenMetadata tokenMetadata,
                           String allocationKeyspace, int numTokens, ImmutableCollection<Token> tokens)
    {
        this.type = type;
        this.address = address;
        this.tokenMetadata = tokenMetadata;
        this.allocationKeyspace = allocationKeyspace;
        this.numTokens = numTokens;
        this.tokens = tokens;
    }

    public static void useSpecifiedTokens(InetAddressAndPort address, String allocationKeyspace, Collection<Token> initialTokens,
                                          int numTokens)
    {
        if (isEnabled(BootstrapEventType.BOOTSTRAP_USING_SPECIFIED_TOKENS))
            DiagnosticEventService.instance().publish(new BootstrapEvent(BootstrapEventType.BOOTSTRAP_USING_SPECIFIED_TOKENS,
                                                              address,
                                                              null,
                                                              allocationKeyspace,
                                                              numTokens,
                                                              ImmutableList.copyOf(initialTokens)));
    }

    public static void useRandomTokens(InetAddressAndPort address, TokenMetadata metadata, int numTokens, Collection<Token> tokens)
    {
        if (isEnabled(BootstrapEventType.BOOTSTRAP_USING_RANDOM_TOKENS))
            DiagnosticEventService.instance().publish(new BootstrapEvent(BootstrapEventType.BOOTSTRAP_USING_RANDOM_TOKENS,
                                                              address,
                                                              metadata.cloneOnlyTokenMap(),
                                                              null,
                                                              numTokens,
                                                              ImmutableList.copyOf(tokens)));
    }

    public static void tokensAllocated(InetAddressAndPort address, TokenMetadata metadata,
                                       String allocationKeyspace, int numTokens, Collection<Token> tokens)
    {
        if (isEnabled(BootstrapEventType.TOKENS_ALLOCATED))
            DiagnosticEventService.instance().publish(new BootstrapEvent(BootstrapEventType.TOKENS_ALLOCATED,
                                                              address,
                                                              metadata.cloneOnlyTokenMap(),
                                                              allocationKeyspace,
                                                              numTokens,
                                                              ImmutableList.copyOf(tokens)));
    }

    private static boolean isEnabled(BootstrapEventType type)
    {
        return DiagnosticEventService.instance().isEnabled(BootstrapEvent.class, type);
    }

    public enum BootstrapEventType
    {
        BOOTSTRAP_USING_SPECIFIED_TOKENS,
        BOOTSTRAP_USING_RANDOM_TOKENS,
        TOKENS_ALLOCATED
    }


    public BootstrapEventType getType()
    {
        return type;
    }

    public Object getSource()
    {
        return address;
    }

    public Map<String, Serializable> toMap()
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        ret.put("tokenMetadata", String.valueOf(tokenMetadata));
        ret.put("allocationKeyspace", allocationKeyspace);
        ret.put("numTokens", numTokens);
        ret.put("tokens", String.valueOf(tokens));
        return ret;
    }
}
