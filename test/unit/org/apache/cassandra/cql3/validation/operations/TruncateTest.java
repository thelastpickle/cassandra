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
package org.apache.cassandra.cql3.validation.operations;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.TruncateStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.TruncateException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Dispatcher;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TruncateTest extends CQLTester
{
    @Test
    public void testTruncate() throws Throwable
    {
        for (String table : new String[] { "", "TABLE" })
        {
            createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY(a, b))");

            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 1);

            flush();

            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 0, 2);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, 3);

            assertRows(execute("SELECT * FROM %s"), row(1, 0, 2), row(1, 1, 3), row(0, 0, 0), row(0, 1, 1));

            execute("TRUNCATE " + table + " %s");

            assertEmpty(execute("SELECT * FROM %s"));
        }
    }

    /**
     * A table dropped between validation and execution leaves no metadata to read, so each execution path must
     * report the unknown table rather than dereference null.
     */
    @Test
    public void testTruncateUnknownTable()
    {
        TruncateStatement statement = new TruncateStatement(new QualifiedName(KEYSPACE, "no_such_table"));
        String expected = String.format("Unknown keyspace/table %s.no_such_table", KEYSPACE);

        assertThatThrownBy(() -> statement.execute(QueryState.forInternalCalls(),
                                                  QueryOptions.DEFAULT,
                                                  Dispatcher.RequestTime.forImmediateExecution()))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage(expected);

        assertThatThrownBy(() -> statement.executeLocally(QueryState.forInternalCalls(), QueryOptions.DEFAULT))
        .isInstanceOf(TruncateException.class)
        .hasRootCauseMessage(expected);
    }
}
