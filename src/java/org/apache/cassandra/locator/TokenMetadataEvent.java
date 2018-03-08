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

import java.io.Serializable;
import java.util.HashMap;

import org.apache.cassandra.diag.DiagnosticEvent;
import org.apache.cassandra.diag.DiagnosticEventService;

/**
 * Events related to {@link TokenMetadata} changes.
 */
public class TokenMetadataEvent extends DiagnosticEvent
{

    public enum TokenMetadataEventType
    {
        PENDING_RANGE_CALCULATION_STARTED,
        PENDING_RANGE_CALCULATION_COMPLETED,
    }

    private final TokenMetadataEventType type;
    private final TokenMetadata tokenMetadata;
    private final String keyspace;

    private TokenMetadataEvent(TokenMetadataEventType type, TokenMetadata tokenMetadata, String keyspace)
    {
        this.type = type;
        this.tokenMetadata = tokenMetadata;
        this.keyspace = keyspace;
    }

    public static void pendingRangeCalculationStarted(TokenMetadata tokenMetadata, String keyspace)
    {
        if (DiagnosticEventService.instance().isEnabled(TokenMetadataEvent.class, TokenMetadataEventType.PENDING_RANGE_CALCULATION_STARTED))
            DiagnosticEventService.instance().publish(new TokenMetadataEvent(TokenMetadataEventType.PENDING_RANGE_CALCULATION_STARTED,
                    tokenMetadata, keyspace));
    }

    @Override
    public Enum<TokenMetadataEventType> getType()
    {
        return type;
    }

    @Override
    public Object getSource()
    {
        return tokenMetadata;
    }

    @Override
    public HashMap<String, Serializable> toMap()
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        ret.put("keyspace", keyspace);
        return ret;
    }
}
