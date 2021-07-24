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
package org.apache.cassandra.db.virtual;


import org.apache.cassandra.audit.AuditLogManager;
import org.apache.cassandra.audit.AuditLogOptions;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;

final class AuditLogOptionsTable extends AbstractVirtualTable
{
    private static final String NAME = "name";
    private static final String AUDIT_LOGS_DIR = "audit_logs_dir";
    private static final String ARCHIVE_COMMAND = "archive_command";
    private static final String ROLL_CYCLE = "roll_cycle";
    private static final String BLOCK = "block";
    private static final String MAX_QUEUE_WEIGHT = "max_queue_weight";
    private static final String MAX_LOG_SIZE = "max_log_size";
    private static final String MAX_ARCHIVE_RETRIES = "max_archive_retries";
    private static final String ENABLED = "enabled";
    private static final String INCLUDED_KEYSPACES = "included_keyspaces";
    private static final String EXCLUDED_KEYSPACES = "excluded_keyspaces";
    private static final String INCLUDED_CATEGORIES = "included_categories";
    private static final String EXCLUDED_CATEGORIES = "excluded_categories";
    private static final String INCLUDED_USERS = "included_users";
    private static final String EXCLUDED_USERS = "excluded_users";
    private static final String LOGGER = "logger";

    AuditLogOptionsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "audit_log_options")
                           .comment("Auditing options")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(NAME, UTF8Type.instance)
                           .addRegularColumn(AUDIT_LOGS_DIR, UTF8Type.instance)
                           .addRegularColumn(ARCHIVE_COMMAND, UTF8Type.instance)
                           .addRegularColumn(ROLL_CYCLE, UTF8Type.instance)
                           .addRegularColumn(BLOCK, BooleanType.instance)
                           .addRegularColumn(MAX_QUEUE_WEIGHT, Int32Type.instance)
                           .addRegularColumn(MAX_LOG_SIZE, LongType.instance)
                           .addRegularColumn(MAX_ARCHIVE_RETRIES, Int32Type.instance)
                           .addRegularColumn(ENABLED, BooleanType.instance)
                           .addRegularColumn(INCLUDED_KEYSPACES, UTF8Type.instance)
                           .addRegularColumn(EXCLUDED_KEYSPACES, UTF8Type.instance)
                           .addRegularColumn(INCLUDED_CATEGORIES, UTF8Type.instance)
                           .addRegularColumn(EXCLUDED_CATEGORIES, UTF8Type.instance)
                           .addRegularColumn(INCLUDED_USERS, UTF8Type.instance)
                           .addRegularColumn(EXCLUDED_USERS, UTF8Type.instance)
                           .addRegularColumn(LOGGER, UTF8Type.instance)
                           .build());

    }

    public DataSet data()
    {
        AuditLogOptions options = AuditLogManager.instance.getAuditLogOptions();

        return new SimpleDataSet(metadata()).row("audit_log_options")
                .column(AUDIT_LOGS_DIR, options.audit_logs_dir)
                .column(ARCHIVE_COMMAND, options.archive_command)
                .column(ROLL_CYCLE, options.roll_cycle)
                .column(BLOCK, options.block)
                .column(MAX_QUEUE_WEIGHT, options.max_queue_weight)
                .column(MAX_LOG_SIZE, options.max_log_size)
                .column(MAX_ARCHIVE_RETRIES, options.max_archive_retries)
                .column(ENABLED, options.enabled)
                .column(INCLUDED_KEYSPACES, options.included_keyspaces)
                .column(EXCLUDED_KEYSPACES, options.excluded_keyspaces)
                .column(INCLUDED_CATEGORIES, options.included_categories)
                .column(EXCLUDED_CATEGORIES, options.excluded_categories)
                .column(INCLUDED_USERS, options.included_users)
                .column(EXCLUDED_USERS, options.excluded_users)
                .column(LOGGER, options.logger.class_name);
    }

    @Override
    public DataSet data(DecoratedKey partitionKey)
    {
        String name = UTF8Type.instance.compose(partitionKey.getKey());
        return "audit_log_options".equalsIgnoreCase(name) ? data() : new SimpleDataSet(metadata());
    }
}
