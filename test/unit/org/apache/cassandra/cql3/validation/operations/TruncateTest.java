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

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.TruncateStatement;
import org.apache.cassandra.service.QueryState;
import org.mockito.Mockito;

import static org.apache.cassandra.config.CassandraRelevantProperties.TRUNCATE_STATEMENT_PROVIDER;

public class TruncateTest extends CQLTester
{
    static {
        TRUNCATE_STATEMENT_PROVIDER.setString(TestTruncateStatementProvider.class.getName());
    }

    public static boolean testTruncateProvider = false;

    @After
    public void afterTest()
    {
        testTruncateProvider = false;
    }

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

    @Test
    public void testRemoteTruncateStmt() throws Throwable
    {
        testTruncateProvider = true;
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY(a, b))");
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);

        execute("TRUNCATE TABLE %s");
        Mockito.verify(TestTruncateStatementProvider.mock)
               .executeLocally(Mockito.any(QueryState.class), Mockito.any(QueryOptions.class));
    }

    public static class TestTruncateStatementProvider implements TruncateStatement.TruncateStatementProvider
    {
        public static TruncateStatement mock = Mockito.mock(TruncateStatement.class);

        @Override
        public TruncateStatement createTruncateStatement(String queryString, QualifiedName name)
        {
            if (TruncateTest.testTruncateProvider)
                return mock;
            else
                return new TruncateStatement(queryString, name);
        }
    }
}
