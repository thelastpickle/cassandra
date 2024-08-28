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

package org.apache.cassandra.index.sai.cql;

import java.util.Arrays;

import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.analyzer.AnalyzerEqOperatorSupport;
import org.apache.cassandra.index.sai.analyzer.filter.BuiltInAnalyzers;
import org.assertj.core.api.Assertions;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class LuceneAnalyzerTest extends SAITester
{
    @Test
    public void testQueryAnalyzer() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {" +
                    "'index_analyzer': '{\n" +
                    "\t\"tokenizer\":{\"name\":\"ngram\", \"args\":{\"minGramSize\":\"2\", \"maxGramSize\":\"3\"}}," +
                    "\t\"filters\":[{\"name\":\"lowercase\"}]\n" +
                    "}'," +
                    "'query_analyzer': '{\n" +
                    "\t\"tokenizer\":{\"name\":\"whitespace\"},\n" +
                    "\t\"filters\":[{\"name\":\"porterstem\"}]\n" +
                    "}'};");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'the query')");

        flush();

        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'query'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val = 'query'").size());
    }

    @Test
    public void testStandardQueryAnalyzer()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {" +
                    "'index_analyzer': 'standard'};");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES (1, 'some row')");
        execute("INSERT INTO %s (id, val) VALUES (2, 'a different row')");
        execute("INSERT INTO %s (id, val) VALUES (3, 'a row with some and different but not together')");
        execute("INSERT INTO %s (id, val) VALUES (4, 'a row with some different together')");
        execute("INSERT INTO %s (id, val) VALUES (5, 'a row with some Different together but not same casing')");

        flush();

        // The query is parsed by the standard analyzer, so the query is tokenized by whitespace and lowercased
        // and then we do an intersection on the results and get docs that have 'some' and 'different'
        assertRows(execute("SELECT id FROM %s WHERE val : 'Some different'"), row(5), row(4), row(3));
        assertRows(execute("SELECT id FROM %s WHERE val : 'some different'"), row(5), row(4), row(3));
    }

    @Test
    public void testQueryAnalyzerBuiltIn() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {" +
                    "'index_analyzer': 'standard', 'query_analyzer': 'lowercase'};");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES (1, 'the query')");
        execute("INSERT INTO %s (id, val) VALUES (2, 'my test Query')");
        execute("INSERT INTO %s (id, val) VALUES (3, 'The Big Dog')");

        // Some in sstable and some in memory
        flush();

        execute("INSERT INTO %s (id, val) VALUES (4, 'another QUERY')");
        execute("INSERT INTO %s (id, val) VALUES (5, 'the fifth insert')");
        execute("INSERT INTO %s (id, val) VALUES (6, 'MY LAST ENTRY')");

        // Shows that the query term is lowercased to match all 'query' terms in the index
        UntypedResultSet resultSet = execute("SELECT id FROM %s WHERE val : 'QUERY'");
        assertRows(resultSet, row(1), row(2), row(4));

        // add whitespace in front of query term and since it isn't tokenized by whitespace, we get no results
        resultSet = execute("SELECT id FROM %s WHERE val : ' query'");
        assertRows(resultSet);

        // similarly, phrases do not match because index tokenized by whitespace (among other things) but the query
        // is not
        resultSet = execute("SELECT id FROM %s WHERE val : 'the query'");
        assertRows(resultSet);
    }

    @Test
    public void testDifferentIndexAndQueryAnalyzersWhenAppliedDuringPostFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, c1 text)");
        // This test verifies a bug fix where the query analyzer was incorrectly used in place of the index analyzer.
        // The analyzers are selected in conjunction with the column values and the query. Specifically,
        // the index analyzer includes a lowercase filter but the query analyzer does not.
        createIndex("CREATE CUSTOM INDEX ON %s(c1) USING 'StorageAttachedIndex' WITH OPTIONS =" +
                    "{'index_analyzer': 'standard', 'query_analyzer': 'whitespace'}");
        waitForIndexQueryable();

        // The standard analyzer maps this to just one output 'the', but the query analyzer would map this to 'THE'
        execute("INSERT INTO %s (pk, c1) VALUES (?, ?)", 1, "THE");

        UntypedResultSet resultSet = execute("SELECT pk FROM %s WHERE c1 : 'the'");
        assertRows(resultSet, row(1));
    }

    @Test
    public void testCreateIndexWithQueryAnalyzerAndNoIndexAnalyzerFails() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, c1 text)");
        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(c1) USING 'StorageAttachedIndex' WITH OPTIONS = " +
                    "{'query_analyzer': 'whitespace'}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Cannot specify query_analyzer without an index_analyzer option or any combination of " +
                             "case_sensitive, normalize, or ascii options. options={query_analyzer=whitespace, target=c1}");;
    }

    @Test
    public void testCreateIndexWithNormalizersWorks() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, c1 text, c2 text, c3 text)");
        createIndex("CREATE CUSTOM INDEX ON %s(c1) USING 'StorageAttachedIndex' WITH OPTIONS = " +
                    "{'query_analyzer': 'whitespace', 'case_sensitive': false}");

        createIndex("CREATE CUSTOM INDEX ON %s(c2) USING 'StorageAttachedIndex' WITH OPTIONS = " +
                    "{'query_analyzer': 'whitespace', 'normalize': true}");

        createIndex("CREATE CUSTOM INDEX ON %s(c3) USING 'StorageAttachedIndex' WITH OPTIONS = " +
                    "{'query_analyzer': 'whitespace', 'ascii': true}");
    }

    @Test
    public void testStandardAnalyzerWithFullConfig() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': '{" +
                    "    \"tokenizer\" : {\"name\" : \"standard\"}," +
                    "    \"filters\" : [ {\"name\" : \"lowercase\"}] \n" +
                    "  }'}");
        standardAnalyzerTest();
    }

    @Test
    public void testStandardAnalyzerWithBuiltInName() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard'}");
        standardAnalyzerTest();
    }

    private void standardAnalyzerTest() throws Throwable {
        waitForIndexQueryable();
        execute("INSERT INTO %s (id, val) VALUES ('1', 'The quick brown fox jumps over the lazy DOG.')");

        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'The quick brown fox jumps over the lazy DOG.' ALLOW FILTERING").size());

        flush();
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog' OR val : 'missing'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'missing1' OR val : 'missing2'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'dog' AND val : 'missing'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog' AND val : 'lazy'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog' AND val : 'quick' AND val : 'fox'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog' AND val : 'quick' OR val : 'missing'").size());

        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog' AND (val : 'quick' OR val : 'missing')").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'missing' AND (val : 'quick' OR val : 'dog')").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog' OR (val : 'quick' AND val : 'missing')").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'missing' OR (val : 'quick' AND val : 'dog')").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'missing' OR (val : 'quick' AND val : 'missing')").size());

        // EQ operator support is reintroduced for analyzed columns, it should work as ':' operator
        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'dog'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'The quick brown fox jumps over the lazy DOG.' ALLOW FILTERING").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'dog' ALLOW FILTERING").size());
    }

    @Test
    public void testEmptyAnalyzerFailsAtCreation() {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                                             "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                                             "WITH OPTIONS = { 'index_analyzer': '{}'}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Analzyer config requires at least a tokenizer, a filter, or a charFilter, but none found. config={}");
    }

// FIXME re-enable exception detection once incompatible options have been purged from prod DBs
    @Test
    public void testIndexAnalyzerAndNonTokenizingAnalyzerFailsAtCreation() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX val_idx ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard', 'ascii': true}");
        dropIndex("DROP INDEX %s.val_idx");

        createIndex("CREATE CUSTOM INDEX val_idx ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard', 'normalize': true}");
        dropIndex("DROP INDEX %s.val_idx");

        createIndex("CREATE CUSTOM INDEX val_idx ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard', 'case_sensitive': false}");
    }

    // Technically, the NoopAnalyzer is applied, but that maps each field without modification, so any operator
    // that matches the SAI field will also match the PK field when compared later in the search (there are two phases).
    @Test
    public void testNoAnalyzerOnClusteredColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, val text, PRIMARY KEY (id, val))");

        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES (1, 'dog')");

        assertThatThrownBy(() -> execute("SELECT * FROM %s WHERE val : 'dog'"))
        .isInstanceOf(InvalidRequestException.class);;

        // Equality still works because indexed value is not analyzed, and so the search can be performed without
        // filtering.
        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'dog'").size());
    }

    @Test
    public void testStandardAnalyzerInClusteringColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, val text, PRIMARY KEY (id, val))");

        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");
        waitForIndexQueryable();

        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(val) WITH OPTIONS = { 'ascii': true }"
        )).isInstanceOf(InvalidRequestException.class);

        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                                             "WITH OPTIONS = { 'case_sesnsitive': false }"
        )).isInstanceOf(InvalidRequestException.class);

        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                                             "WITH OPTIONS = { 'normalize': true }"
        )).isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void testBogusAnalyzer() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        assertThatThrownBy(
        () -> executeNet("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'lalalalaal'}"
        )).isInstanceOf(InvalidQueryException.class);

        assertThatThrownBy(
        () -> executeNet("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = " +
                         "{'index_analyzer':'{\"tokenizer\" : {\"name\" : \"lalala\"}}'}"
        )).isInstanceOf(InvalidQueryException.class);
    }

    @Test
    public void testStopFilterNoFormat() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        executeNet("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'\n" +
                                            "\t{\"tokenizer\":{\"name\" : \"whitespace\"},\n" +
                                            "\t \"filters\":[{\"name\":\"stop\", \"args\": {\"words\": \"the,test\"}}]}'}");
        verifyStopWordsLoadedCorrectly();
    }

    @Test
    public void testStopFilterWordSet() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        executeNet("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'\n" +
                   "\t{\"tokenizer\":{\"name\" : \"whitespace\"},\n" +
                   "\t \"filters\":[{\"name\":\"stop\", \"args\": {\"words\": \"the, test\", \"format\": \"wordset\"}}]}'}");
        verifyStopWordsLoadedCorrectly();
    }

    @Test
    public void testStopFilterSnowball() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        // snowball allows multiple words on the same line--they are broken up by whitespace
        executeNet("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'\n" +
                   "\t{\"tokenizer\":{\"name\" : \"whitespace\"},\n" +
                   "\t \"filters\":[{\"name\":\"stop\", \"args\": {\"words\": \"the test\", \"format\": \"snowball\"}}]}'}");
        verifyStopWordsLoadedCorrectly();

    }

    private void verifyStopWordsLoadedCorrectly() throws Throwable
    {
        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'the big test')");

        flush();

        assertRows(execute("SELECT id FROM %s WHERE val : 'the'"));
        assertRows(execute("SELECT id FROM %s WHERE val : 'the test'"));
        assertRows(execute("SELECT id FROM %s WHERE val : 'test'"));
        assertRows(execute("SELECT id FROM %s WHERE val : 'the big'"), row("1"));
        assertRows(execute("SELECT id FROM %s WHERE val : 'big'"), row("1"));
        // the extra words shouldn't change the outcome because tokenizer is whitespace and tokens are matched then unioned
        assertRows(execute("SELECT id FROM %s WHERE val : 'test some other words'"));
    }

    @Test
    public void verifyEmptyStringIndexingBehaviorOnNonAnalyzedColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 0, "");
        flush();
        assertRows(execute("SELECT * FROM %s WHERE v = ''"));
    }

    @Test
    public void testEmptyQueryString() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'standard'}");
        waitForIndexQueryable();
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 0, "");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "some text to analyze");
        flush();
        assertRows(execute("SELECT * FROM %s WHERE v : ''"));
    }

    // The english analyzer has a default set of stop words. This test relies on "the" being one of those stop words.
    @Test
    public void testStopWordFilteringEdgeCases() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        executeNet("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' " +
                   "WITH OPTIONS = {'index_analyzer':'english'}");
        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'the test')");
        // When indexing a document with only stop words, the document should not be indexed.
        // Note: from looking at the collections implementation, these rows are filtered out before getting
        // to the NoOpAnalyzer, which would otherwise return an empty buffer, which would lead to incorrectly
        // indexing documents at the base of the trie.
        execute("INSERT INTO %s (id, val) VALUES ('2', 'the')");

        flush();

        // Ensure row is there
        assertRows(execute("SELECT id FROM %s WHERE val : 'test'"), row("1"));
        // Ensure a query with only stop words results in no rows
        assertRows(execute("SELECT id FROM %s WHERE val : 'the'"));
        // Ensure that the AND is correctly applied so that we get no results
        assertRows(execute("SELECT id FROM %s WHERE val : 'the' AND val : 'test'"));
    }

    @Test
    public void testCharfilter() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'{\n" +
                    "\t\"tokenizer\":{\"name\":\"keyword\"},\n" +
                    "\t\"charFilters\":[{\"name\":\"htmlstrip\"}]\n" +
                    "}'}");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', '<b>hello</b>')");

        flush();

        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'hello'").size());
    }

    @Test
    public void testNGramfilter() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        String ddl = "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'\n" +
                     "\t{\"tokenizer\":{\"name\":\"ngram\", \"args\":{\"minGramSize\":\"2\", \"maxGramSize\":\"3\"}}," +
                     "\t\"filters\":[{\"name\":\"lowercase\"}]}'}";
        createIndex(ddl);

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'DoG')");

        flush();

        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'do'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'og'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog'").size());
    }

    @Test
    public void testNGramfilterNoFlush() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'\n" +
                    "\t{\"tokenizer\":{\"name\":\"ngram\", \"args\":{\"minGramSize\":\"2\", \"maxGramSize\":\"3\"}}," +
                    "\t\"filters\":[{\"name\":\"lowercase\"}]}'}");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'DoG')");

        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'do'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'og'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog'").size());
    }

    @Test
    public void testEdgeNgramFilterWithOR() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {" +
                    "'index_analyzer': '{\n" +
                    "\t\"tokenizer\":{\"name\":\"standard\", \"args\":{}}," +
                    "\t\"filters\":[{\"name\":\"lowercase\", \"args\":{}}, " +
                    "{\"name\":\"edgengram\", \"args\":{\"minGramSize\":\"2\", \"maxGramSize\":\"30\"}}],\n" +
                    "\t\"charFilters\":[]" +
                    "}'};");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'MAL0133AU')");
        execute("INSERT INTO %s (id, val) VALUES ('2', 'WFS2684AU')");
        execute("INSERT INTO %s (id, val) VALUES ('3', 'FPWMCR005 Mercer High Growth Managed')");
        execute("INSERT INTO %s (id, val) VALUES ('4', 'WFS7093AU')");
        execute("INSERT INTO %s (id, val) VALUES ('5', 'WFS0565AU')");

        beforeAndAfterFlush(() -> {
            // match (:)
            assertEquals(1, execute("SELECT val FROM %s WHERE val : 'MAL0133AU'").size());
            assertEquals(1, execute("SELECT val FROM %s WHERE val : 'WFS2684AU'").size());
            assertEquals(0, execute("SELECT val FROM %s WHERE val : ''").size());
            assertEquals(2, execute("SELECT val FROM %s WHERE val : 'MAL0133AU' OR val : 'WFS2684AU'").size());
            assertEquals(1, execute("SELECT val FROM %s WHERE val : '' OR val : 'WFS2684AU'").size());
            assertEquals(0, execute("SELECT val FROM %s WHERE val : '' AND val : 'WFS2684AU'").size());

            // equals (=)
            assertEquals(1, execute("SELECT val FROM %s WHERE val = 'MAL0133AU'").size());
            assertEquals(1, execute("SELECT val FROM %s WHERE val = 'WFS2684AU'").size());
            assertEquals(0, execute("SELECT val FROM %s WHERE val = ''").size());
            assertEquals(2, execute("SELECT val FROM %s WHERE val = 'MAL0133AU' OR val = 'WFS2684AU'").size());
            assertEquals(1, execute("SELECT val FROM %s WHERE val = '' OR val = 'WFS2684AU'").size());
            assertEquals(0, execute("SELECT val FROM %s WHERE val = '' AND val = 'WFS2684AU'").size());

            // mixed match (:) and equals (=)
            assertEquals(2, execute("SELECT val FROM %s WHERE val = 'MAL0133AU' OR val : 'WFS2684AU'").size());
            assertEquals(1, execute("SELECT val FROM %s WHERE val = '' OR val : 'WFS2684AU'").size());
            assertEquals(0, execute("SELECT val FROM %s WHERE val = '' AND val : 'WFS2684AU'").size());
            assertEquals(2, execute("SELECT val FROM %s WHERE val : 'MAL0133AU' OR val = 'WFS2684AU'").size());
            assertEquals(1, execute("SELECT val FROM %s WHERE val : '' OR val = 'WFS2684AU'").size());
            assertEquals(0, execute("SELECT val FROM %s WHERE val : '' AND val = 'WFS2684AU'").size());
        });
    }

    @Test
    public void testNgramFilterWithOR() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {" +
                    "'index_analyzer': '{\n" +
                    "\t\"tokenizer\":{\"name\":\"standard\", \"args\":{}}," +
                    "\t\"filters\":[{\"name\":\"lowercase\", \"args\":{}}, " +
                    "{\"name\":\"ngram\", \"args\":{\"minGramSize\":\"2\", \"maxGramSize\":\"30\"}}],\n" +
                    "\t\"charFilters\":[]" +
                    "}'};");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'MAL0133AU')");
        execute("INSERT INTO %s (id, val) VALUES ('2', 'WFS2684AU')");
        execute("INSERT INTO %s (id, val) VALUES ('3', 'FPWMCR005 Mercer High Growth Managed')");
        execute("INSERT INTO %s (id, val) VALUES ('4', 'WFS7093AU')");
        execute("INSERT INTO %s (id, val) VALUES ('5', 'WFS0565AU')");

        beforeAndAfterFlush(() -> {
            // match (:)
            assertEquals(1, execute("SELECT val FROM %s WHERE val : 'MAL0133AU'").size());
            assertEquals(1, execute("SELECT val FROM %s WHERE val : 'WFS2684AU'").size());
            assertEquals(1, execute("SELECT val FROM %s WHERE val : '268'").size());
            assertEquals(2, execute("SELECT val FROM %s WHERE val : 'MAL0133AU' OR val : 'WFS2684AU'").size());
            assertEquals(2, execute("SELECT val FROM %s WHERE val : '133' OR val : 'WFS2684AU'").size());
            assertEquals(1, execute("SELECT val FROM %s WHERE val : 'MAL' AND val : 'AU'").size());
            assertEquals(0, execute("SELECT val FROM %s WHERE val : 'XYZ' AND val : 'AU'").size());

            // equals (=)
            assertEquals(1, execute("SELECT val FROM %s WHERE val = 'MAL0133AU'").size());
            assertEquals(1, execute("SELECT val FROM %s WHERE val = 'WFS2684AU'").size());
            assertEquals(1, execute("SELECT val FROM %s WHERE val = '268'").size());
            assertEquals(2, execute("SELECT val FROM %s WHERE val = 'MAL0133AU' OR val = 'WFS2684AU'").size());
            assertEquals(2, execute("SELECT val FROM %s WHERE val = '133' OR val = 'WFS2684AU'").size());
            assertEquals(1, execute("SELECT val FROM %s WHERE val = 'MAL' AND val = 'AU'").size());
            assertEquals(0, execute("SELECT val FROM %s WHERE val = 'XYZ' AND val = 'AU'").size());

            // mixed match (:) and equals (=)
            assertEquals(2, execute("SELECT val FROM %s WHERE val : 'MAL0133AU' OR val = 'WFS2684AU'").size());
            assertEquals(2, execute("SELECT val FROM %s WHERE val : '133' OR val = 'WFS2684AU'").size());
            assertEquals(1, execute("SELECT val FROM %s WHERE val : 'MAL' AND val = 'AU'").size());
            assertEquals(0, execute("SELECT val FROM %s WHERE val : 'XYZ' AND val = 'AU'").size());
            assertEquals(2, execute("SELECT val FROM %s WHERE val = 'MAL0133AU' OR val : 'WFS2684AU'").size());
            assertEquals(2, execute("SELECT val FROM %s WHERE val = '133' OR val : 'WFS2684AU'").size());
            assertEquals(1, execute("SELECT val FROM %s WHERE val = 'MAL' AND val : 'AU'").size());
            assertEquals(0, execute("SELECT val FROM %s WHERE val = 'XYZ' AND val : 'AU'").size());
        });
    }
    @Test
    public void testWhitespace() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS =" +
                    "{'index_analyzer':'whitespace'}");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'hello world twice the and')");

        flush();

        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'hello'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'twice'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'the'").size()); // test stop word
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'and'").size()); // test stop word
    }

    @Test
    public void testWhitespaceLowercase() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'\n" +
                    "\t{\"tokenizer\":{\"name\":\"whitespace\"}," +
                    "\t\"filters\":[{\"name\":\"lowercase\"}]}'}");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'hELlo woRlD tWice tHe aNd')");

        flush();

        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'hello'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'twice'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'the'").size()); // test stop word
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'and'").size()); // test stop word
    }

    @Test
    public void testTokenizer() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'\n" +
                    "\t{\"tokenizer\":{\"name\":\"whitespace\"}," +
                    "\t\"filters\":[{\"name\":\"porterstem\"}]}'}");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'the queries test')");

        flush();

        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'the'").size()); // stop word test
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'query'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'query' OR val : 'missing'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'queries' AND val : 'the' AND val : 'test'").size());
    }

    @Test
    public void testMixedAnalyzerMatchesAndEquality() // there are more detailed tests in AnalyzerEqOperatorSupportTest
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        String createIndexQuery = "CREATE CUSTOM INDEX ON %%s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {" +
                                  "'index_analyzer':'{\n" +
                                  "   \"tokenizer\":{\"name\":\"whitespace\"}," +
                                  "   \"filters\":[{\"name\":\"porterstem\"}]" +
                                  "}'," +
                                  "'equals_behaviour_when_analyzed': '%s'}";
        createIndex(String.format(createIndexQuery, AnalyzerEqOperatorSupport.Value.MATCH));
        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'the queries test')");

        // we'll test AND and OR, with both MATCH and EQ and the first operator in the mix
        final String conjunctionQueryMatchEq = "SELECT id FROM %s WHERE val : 'queries' AND val : 'the' AND val = 'the queries test'";
        final String conjunctionQueryEqMatch = "SELECT id FROM %s WHERE val = 'queries' AND val = 'the' AND val : 'the queries test'";
        final String disjunctionQueryMatchEq = "SELECT id FROM %s WHERE val : 'queries' OR val : 'the' OR val = 'blah, blah, blah'";
        final String disjunctionQueryEqMatch = "SELECT id FROM %s WHERE val = 'queries' OR val = 'the' OR val : 'blah, blah, blah'";

        // if the index supports EQ, the mixed queries should work as the operators are considered the same
        for (String query : Arrays.asList(conjunctionQueryMatchEq, conjunctionQueryEqMatch, disjunctionQueryMatchEq, disjunctionQueryEqMatch))
        {
            assertRows(execute(query), row("1"));
            assertRows(execute(query + "ALLOW FILTERING"), row("1"));
        }

        // recreate the index with 'equals_behaviour_when_analyzed': 'UNSUPPORTED'
        dropIndex("DROP INDEX %s." + currentIndex());
        createIndex(String.format(createIndexQuery, AnalyzerEqOperatorSupport.Value.UNSUPPORTED));
        waitForIndexQueryable();

        // If the index does not support EQ, the mixed queries should fail.
        // The error message will slightly change depending on whether EQ or MATCH are before in the query.

        Assertions.assertThatThrownBy(() -> execute(conjunctionQueryMatchEq))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(SingleColumnRestriction.AnalyzerMatchesRestriction.CANNOT_BE_MERGED_ERROR, "val"));
        Assertions.assertThatThrownBy(() -> execute(conjunctionQueryMatchEq + "ALLOW FILTERING"))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(SingleColumnRestriction.AnalyzerMatchesRestriction.CANNOT_BE_MERGED_ERROR, "val"));

        Assertions.assertThatThrownBy(() -> execute(conjunctionQueryEqMatch))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(SingleColumnRestriction.EQRestriction.CANNOT_BE_MERGED_ERROR, "val"));
        Assertions.assertThatThrownBy(() -> execute(conjunctionQueryEqMatch + "ALLOW FILTERING"))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(SingleColumnRestriction.EQRestriction.CANNOT_BE_MERGED_ERROR, "val"));

        Assertions.assertThatThrownBy(() -> execute(disjunctionQueryMatchEq))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);
        // TODO: this last test is affected by CNDB-10731. We should enable it once that is fixed.
        // assertRows(execute(disjunctionQueryMatchEq + "ALLOW FILTERING"), row("1"));

        Assertions.assertThatThrownBy(() -> execute(disjunctionQueryEqMatch))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);
        // TODO: this last test is affected by CNDB-10731. We should enable it once that is fixed.
        // assertRows(execute(disjunctionQueryEqMatch + "ALLOW FILTERING"), row("1"));
    }

    @Test
    public void testBuiltInAlyzerIndexCreation() throws Throwable
    {
        for (BuiltInAnalyzers builtInAnalyzer : BuiltInAnalyzers.values())
            testBuiltInAlyzerIndexCreationFor(builtInAnalyzer.name());
    }

    private void testBuiltInAlyzerIndexCreationFor(String builtInAnalyzerName) throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = " +
                    "{'index_analyzer':'" + builtInAnalyzerName + "'}");

        waitForIndexQueryable();
    }

    @Test
    public void testInvalidQueryOnNumericColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, some_num tinyint)");
        createIndex("CREATE CUSTOM INDEX ON %s(some_num) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (id, some_num) VALUES (1, 1)");
        flush();

        assertThatThrownBy(() -> execute("SELECT * FROM %s WHERE some_num : 1"))
        .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void testLegacyEqQueryOnNormalizedTextColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val text)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'ascii': 'true', 'case_sensitive': 'false', 'normalize': 'true'}");
        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES (1, 'Aaą')");

        beforeAndAfterFlush(() -> assertEquals(1, execute("SELECT * FROM %s WHERE val = 'aaa'").size()));
    }

    @Test
    public void testAnalyzerThatProducesTooManyBytesIsRejectedAtWriteTime()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'{" +
                    "\"tokenizer\":{\"name\":\"ngram\", \"args\":{\"minGramSize\":\"1\", \"maxGramSize\":\"26\"}},\n" +
                    "\"filters\":[{\"name\":\"lowercase\"}]}'}");

        waitForIndexQueryable();

        assertThatThrownBy(() -> execute("INSERT INTO %s (id, val) VALUES (0, 'abcdedfghijklmnopqrstuvwxyz abcdedfghijklmnopqrstuvwxyz')"))
        .hasMessage("Term's analyzed size for column val exceeds the cumulative limit for index. Max allowed size 8.000KiB.")
        .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void testInvalidNamesOnConfig()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val text)");

        // Empty config
        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'{}'}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Analzyer config requires at least a tokenizer, a filter, or a charFilter, but none found. config={}");

        var invalidCharfilters = "{\"tokenizer\" : {\"name\" : \"keyword\"},\"charfilters\" : [{\"name\" : \"htmlstrip\"}]}";

        // Invalid config name, charfilters should be charFilters
        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'" + invalidCharfilters + "'}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Invalid field name 'charfilters' in analyzer config. Valid fields are: [tokenizer, filters, charFilters]");

        // Invalid config name on query_analyzer, charfilters should be charFilters
        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'standard', 'query_analyzer':'" + invalidCharfilters + "'}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Invalid field name 'charfilters' in analyzer config. Valid fields are: [tokenizer, filters, charFilters]");

        // Invalid tokenizer name
        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'{\"tokenizer\":{\"name\" : \"invalid\"}}'}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Unknown tokenizer 'invalid'. Valid options: [");

        // Invalid filter name
        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'{\"tokenizer\":{\"name\" : \"keyword\"},\n" +
                                             "    \"filters\":[{\"name\" : \"invalid\"}]}'}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Unknown filter 'invalid'. Valid options: [");

        // Invalid charFilter name
        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'{\"tokenizer\":{\"name\" : \"keyword\"},\n" +
                                             "    \"charFilters\":[{\"name\" : \"invalid\"}]}'}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Unknown charFilter 'invalid'. Valid options: [");

        // Missing one of the params in the args field
        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'{\"tokenizer\":{\"name\" : \"keyword\"},\n" +
                                             "    \"filters\":[{\"name\" : \"synonym\", \"args\" : {\"words\" : \"as => like\"}},\n" +
                                             "    {\"name\" : \"lowercase\"}]}'}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Error configuring analyzer's filter 'synonym': Configuration Error: missing parameter 'synonyms'");


        // Missing one of the params in the args field
        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'{\"tokenizer\":{\"name\" : \"keyword\"},\n" +
                                             "    \"filters\":[{\"name\" : \"synonym\", \"args\" : {\"synonyms\" : \"as => like\", \"extraParam\": \"xyz\"}},\n" +
                                             "    {\"name\" : \"lowercase\"}]}'}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Error configuring analyzer's filter 'synonym': Unknown parameters: {extraParam=xyz}");
    }
}
