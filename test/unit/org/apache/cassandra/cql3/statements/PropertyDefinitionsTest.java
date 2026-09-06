/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.cql3.statements;

import java.util.Collections;
import java.util.HashMap;
import java.util.function.BiConsumer;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.NoSpamLogger;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.cql3.statements.PropertyDefinitions.parseBoolean;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PropertyDefinitionsTest
{
    /** Read by the {@link NoSpamLogger} clock, so a test can move time without sleeping. */
    private static long nowNanos;

    private Logger logger;
    private ListAppender<ILoggingEvent> appender;

    @BeforeClass
    public static void setUpClass()
    {
        NoSpamLogger.unsafeSetClock(() -> nowNanos);
    }

    @AfterClass
    public static void tearDownClass()
    {
        NoSpamLogger.unsafeSetClock(Clock.Global::nanoTime);
    }

    @Before
    public void setUp()
    {
        logger = (Logger) LoggerFactory.getLogger(PropertyDefinitions.class);
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
    }

    @After
    public void tearDown()
    {
        logger.detachAppender(appender);
        appender.stop();
    }

    @Test
    public void testPostiveBooleanParsing()
    {
        assertTrue(parseBoolean("prop1", "1"));
        assertTrue(parseBoolean("prop2", "true"));
        assertTrue(parseBoolean("prop3", "True"));
        assertTrue(parseBoolean("prop4", "TrUe"));
        assertTrue(parseBoolean("prop5", "yes"));
        assertTrue(parseBoolean("prop6", "Yes"));
    }

    @Test
    public void testNegativeBooleanParsing()
    {
        assertFalse(parseBoolean("prop1", "0"));
        assertFalse(parseBoolean("prop2", "false"));
        assertFalse(parseBoolean("prop3", "False"));
        assertFalse(parseBoolean("prop4", "FaLse"));
        assertFalse(parseBoolean("prop6", "No"));
    }

    @Test
    public void testGetProperty()
    {
        String key = "k";
        String value = "v";
        PropertyDefinitions pd = new PropertyDefinitions();
        pd.addProperty(key, value);
        assertEquals(value, pd.getProperty(key).toString());
    }

    @Test(expected = SyntaxException.class)
    public void testGetMissingProperty()
    {
        PropertyDefinitions pd = new PropertyDefinitions();
        pd.getProperty("missing");
    }

    @Test(expected = SyntaxException.class)
    public void testInvalidPositiveBooleanParsing()
    {
        parseBoolean("cdc", "tru");
    }

    @Test(expected = SyntaxException.class)
    public void testInvalidNegativeBooleanParsing()
    {
        parseBoolean("cdc", "fals");
    }

    @Test
    public void testAddProperty()
    {
        // string overload
        testAddProperty("v1", "v2", (pd, v) -> pd.addProperty("k", v));

        // map overload
        testAddProperty(new HashMap<String, String>(){{put("k1", "v1");}},
                        new HashMap<String, String>(){{put("k2", "v2");}},
                        (pd, v) -> pd.addProperty("k", v));

        // set of QualifiedName overload
        testAddProperty(Collections.singleton(new QualifiedName("keyspace", "v1")),
                        Collections.singleton(new QualifiedName("keyspace", "v2")),
                        (pd, v) -> pd.addProperty("k", v));
    }

    private <V> void testAddProperty(V oldValue, V newValue, BiConsumer<PropertyDefinitions, V> adder)
    {
        String key = "k";
        PropertyDefinitions pd = new PropertyDefinitions();
        adder.accept(pd, oldValue);
        Assertions.assertThat(pd.getProperty(key)).isEqualTo(oldValue);
        Assertions.assertThatThrownBy(() -> adder.accept(pd, newValue))
                  .isInstanceOf(SyntaxException.class)
                  .hasMessageContaining(String.format(PropertyDefinitions.MULTIPLE_DEFINITIONS_ERROR, key));
        Assertions.assertThat(pd.getProperty(key)).isEqualTo(oldValue);
    }

    @Test
    public void testObsoletePropertyWarnsOncePerInterval()
    {
        String name = "obsolete_property_per_interval";

        nowNanos = 0;
        validateObsoleteProperty(name);
        assertEquals(1, warningCount(name));

        // A client can repeat the statement without limit, so the second warning must wait for the interval.
        nowNanos = SECONDS.toNanos(29);
        validateObsoleteProperty(name);
        assertEquals(1, warningCount(name));

        nowNanos = SECONDS.toNanos(31);
        validateObsoleteProperty(name);
        assertEquals(2, warningCount(name));
    }

    @Test
    public void testObsoletePropertiesWarnIndependently()
    {
        String first = "obsolete_property_first";
        String second = "obsolete_property_second";

        nowNanos = 0;
        validateObsoleteProperty(first);
        validateObsoleteProperty(second);

        assertEquals(1, warningCount(first));
        assertEquals(1, warningCount(second));
    }

    private static void validateObsoleteProperty(String name)
    {
        PropertyDefinitions pd = new PropertyDefinitions();
        pd.addProperty(name, "v");
        pd.validate(Collections.emptySet(), Collections.singleton(name));
    }

    private long warningCount(String name)
    {
        return appender.list.stream()
                            .filter(event -> event.getLevel() == Level.WARN)
                            .filter(event -> PropertyDefinitions.OBSOLETE_PROPERTY_WARNING.equals(event.getMessage()))
                            .filter(event -> event.getFormattedMessage().contains(name))
                            .count();
    }
}
