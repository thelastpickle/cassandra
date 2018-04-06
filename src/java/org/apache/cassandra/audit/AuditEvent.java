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

package org.apache.cassandra.audit;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.diag.DiagnosticEvent;
import org.apache.cassandra.diag.DiagnosticEventService;

public class AuditEvent extends DiagnosticEvent
{
    private final AuditLogEntry entry;

    private AuditEvent(AuditLogEntry entry)
    {
        this.entry = entry;
    }

    static void create(AuditLogEntry entry)
    {
        if (isEnabled(entry.getType()))
            DiagnosticEventService.instance().publish(new AuditEvent(entry));
    }

    private static boolean isEnabled(AuditLogEntryType type)
    {
        return DiagnosticEventService.instance().isEnabled(AuditEvent.class, type);
    }

    @Override
    public Enum<?> getType()
    {
        return entry.getType();
    }

    @Override
    public Object getSource()
    {
        return null;
    }

    @Override
    public Map<String, String> toMap()
    {
        HashMap<String, String> ret = new HashMap<>();
        if (entry.getKeyspace() != null) ret.put("keyspace", entry.getKeyspace());
        if (entry.getOperation() != null) ret.put("operation", entry.getOperation());
        if (entry.getScope() != null) ret.put("scope", entry.getScope());
        if (entry.getUser() != null) ret.put("user", entry.getUser());
        return ret;
    }
}
