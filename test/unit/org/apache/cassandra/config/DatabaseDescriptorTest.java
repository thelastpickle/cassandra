/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.config;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.ThriftConversion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(OrderedJUnit4ClassRunner.class)
public class DatabaseDescriptorTest
{
    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        DatabaseDescriptor.setDaemonInitialized();
    }

    @Test
    public void testCFMetaDataSerialization() throws ConfigurationException, InvalidRequestException
    {
        // test serialization of all defined test CFs.
        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            for (CFMetaData cfm : Schema.instance.getTablesAndViews(keyspaceName))
            {
                CFMetaData cfmDupe = ThriftConversion.fromThrift(ThriftConversion.toThrift(cfm));
                assertNotNull(cfmDupe);
                assertEquals(cfm, cfmDupe);
            }
        }
    }

    @Test
    public void testKSMetaDataSerialization() throws ConfigurationException
    {
        for (String ks : Schema.instance.getNonSystemKeyspaces())
        {
            // Not testing round-trip on the KsDef via serDe() because maps
            KeyspaceMetadata ksm = Schema.instance.getKSMetaData(ks);
            KeyspaceMetadata ksmDupe = ThriftConversion.fromThrift(ThriftConversion.toThrift(ksm));
            assertNotNull(ksmDupe);
            assertEquals(ksm, ksmDupe);
        }
    }

    // this came as a result of CASSANDRA-995
    @Test
    public void testTransKsMigration() throws ConfigurationException, IOException
    {
        SchemaLoader.cleanupAndLeaveDirs();
        Schema.instance.loadFromDisk();
        assertEquals(0, Schema.instance.getNonSystemKeyspaces().size());

        Gossiper.instance.start((int)(System.currentTimeMillis() / 1000));
        Keyspace.setInitialized();

        try
        {
            // add a few.
            MigrationManager.announceNewKeyspace(KeyspaceMetadata.create("ks0", KeyspaceParams.simple(3)));
            MigrationManager.announceNewKeyspace(KeyspaceMetadata.create("ks1", KeyspaceParams.simple(3)));

            assertNotNull(Schema.instance.getKSMetaData("ks0"));
            assertNotNull(Schema.instance.getKSMetaData("ks1"));

            Schema.instance.clearKeyspaceMetadata(Schema.instance.getKSMetaData("ks0"));
            Schema.instance.clearKeyspaceMetadata(Schema.instance.getKSMetaData("ks1"));

            assertNull(Schema.instance.getKSMetaData("ks0"));
            assertNull(Schema.instance.getKSMetaData("ks1"));

            Schema.instance.loadFromDisk();

            assertNotNull(Schema.instance.getKSMetaData("ks0"));
            assertNotNull(Schema.instance.getKSMetaData("ks1"));
        }
        finally
        {
            Gossiper.instance.stop();
        }
    }

    @Test
    public void testConfigurationLoader() throws Exception
    {
        // By default, we should load from the yaml
        Config config = DatabaseDescriptor.loadConfig();
        assertEquals("Test Cluster", config.cluster_name);
        Keyspace.setInitialized();

        // Now try custom loader
        ConfigurationLoader testLoader = new TestLoader();
        System.setProperty("cassandra.config.loader", testLoader.getClass().getName());

        config = DatabaseDescriptor.loadConfig();
        assertEquals("ConfigurationLoader Test", config.cluster_name);
    }

    public static class TestLoader implements ConfigurationLoader
    {
        public Config loadConfig() throws ConfigurationException
        {
            Config testConfig = new Config();
            testConfig.cluster_name = "ConfigurationLoader Test";
            return testConfig;
        }
    }

    static NetworkInterface suitableInterface = null;
    static boolean hasIPv4andIPv6 = false;

    /*
     * Server only accepts interfaces by name if they have a single address
     * OS X seems to always have an ipv4 and ipv6 address on all interfaces which means some tests fail
     * if not checked for and skipped
     */
    @BeforeClass
    public static void selectSuitableInterface() throws Exception {
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while(interfaces.hasMoreElements()) {
            NetworkInterface intf = interfaces.nextElement();

            System.out.println("Evaluating " + intf.getName());

            if (intf.isLoopback()) {
                suitableInterface = intf;

                boolean hasIPv4 = false;
                boolean hasIPv6 = false;
                Enumeration<InetAddress> addresses = suitableInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    if (addresses.nextElement() instanceof Inet6Address)
                        hasIPv6 = true;
                    else
                        hasIPv4 = true;
                }
                hasIPv4andIPv6 = hasIPv4 && hasIPv6;
                return;
            }
        }
    }

    @Test
    public void testRpcInterface() throws Exception
    {
        Config testConfig = DatabaseDescriptor.loadConfig();
        testConfig.rpc_interface = suitableInterface.getName();
        testConfig.rpc_address = null;
        DatabaseDescriptor.applyAddressConfig(testConfig);

        /*
         * Confirm ability to select between IPv4 and IPv6
         */
        if (hasIPv4andIPv6)
        {
            testConfig = DatabaseDescriptor.loadConfig();
            testConfig.rpc_interface = suitableInterface.getName();
            testConfig.rpc_address = null;
            testConfig.rpc_interface_prefer_ipv6 = true;
            DatabaseDescriptor.applyAddressConfig(testConfig);

            assertEquals(DatabaseDescriptor.getRpcAddress().getClass(), Inet6Address.class);

            testConfig = DatabaseDescriptor.loadConfig();
            testConfig.rpc_interface = suitableInterface.getName();
            testConfig.rpc_address = null;
            testConfig.rpc_interface_prefer_ipv6 = false;
            DatabaseDescriptor.applyAddressConfig(testConfig);

            assertEquals(DatabaseDescriptor.getRpcAddress().getClass(), Inet4Address.class);
        }
        else
        {
            /*
             * Confirm first address of interface is selected
             */
            assertEquals(DatabaseDescriptor.getRpcAddress(), suitableInterface.getInetAddresses().nextElement());
        }
    }

    @Test
    public void testListenInterface() throws Exception
    {
        Config testConfig = DatabaseDescriptor.loadConfig();
        testConfig.listen_interface = suitableInterface.getName();
        testConfig.listen_address = null;
        DatabaseDescriptor.applyAddressConfig(testConfig);

        /*
         * Confirm ability to select between IPv4 and IPv6
         */
        if (hasIPv4andIPv6)
        {
            testConfig = DatabaseDescriptor.loadConfig();
            testConfig.listen_interface = suitableInterface.getName();
            testConfig.listen_address = null;
            testConfig.listen_interface_prefer_ipv6 = true;
            DatabaseDescriptor.applyAddressConfig(testConfig);

            assertEquals(DatabaseDescriptor.getListenAddress().getClass(), Inet6Address.class);

            testConfig = DatabaseDescriptor.loadConfig();
            testConfig.listen_interface = suitableInterface.getName();
            testConfig.listen_address = null;
            testConfig.listen_interface_prefer_ipv6 = false;
            DatabaseDescriptor.applyAddressConfig(testConfig);

            assertEquals(DatabaseDescriptor.getListenAddress().getClass(), Inet4Address.class);
        }
        else
        {
            /*
             * Confirm first address of interface is selected
             */
            assertEquals(DatabaseDescriptor.getRpcAddress(), suitableInterface.getInetAddresses().nextElement());
        }
    }

    @Test
    public void testListenAddress() throws Exception
    {
        Config testConfig = DatabaseDescriptor.loadConfig();
        testConfig.listen_address = suitableInterface.getInterfaceAddresses().get(0).getAddress().getHostAddress();
        testConfig.listen_interface = null;
        DatabaseDescriptor.applyAddressConfig(testConfig);
    }

    @Test
    public void testRpcAddress() throws Exception
    {
        Config testConfig = DatabaseDescriptor.loadConfig();
        testConfig.rpc_address = suitableInterface.getInterfaceAddresses().get(0).getAddress().getHostAddress();
        testConfig.rpc_interface = null;
        DatabaseDescriptor.applyAddressConfig(testConfig);

    }

    @Test
    public void testRepairSessionSizeToggles()
    {
        int previousDepth = DatabaseDescriptor.getRepairSessionMaxTreeDepth();
        try
        {
            Assert.assertEquals(18, DatabaseDescriptor.getRepairSessionMaxTreeDepth());
            DatabaseDescriptor.setRepairSessionMaxTreeDepth(10);
            Assert.assertEquals(10, DatabaseDescriptor.getRepairSessionMaxTreeDepth());

            try
            {
                DatabaseDescriptor.setRepairSessionMaxTreeDepth(9);
                fail("Should have received a ConfigurationException for depth of 9");
            }
            catch (ConfigurationException ignored) { }
            Assert.assertEquals(10, DatabaseDescriptor.getRepairSessionMaxTreeDepth());

            try
            {
                DatabaseDescriptor.setRepairSessionMaxTreeDepth(-20);
                fail("Should have received a ConfigurationException for depth of -20");
            }
            catch (ConfigurationException ignored) { }
            Assert.assertEquals(10, DatabaseDescriptor.getRepairSessionMaxTreeDepth());

            DatabaseDescriptor.setRepairSessionMaxTreeDepth(22);
            Assert.assertEquals(22, DatabaseDescriptor.getRepairSessionMaxTreeDepth());
        }
        finally
        {
            DatabaseDescriptor.setRepairSessionMaxTreeDepth(previousDepth);
        }
    }

    @Test
    public void testApplyInitialTokensInitialTokensSetNumTokensSetAndDoesMatch() throws Exception
    {
        Config testConfig = DatabaseDescriptor.loadConfig();
        testConfig.initial_token = "0,256,1024";
        testConfig.num_tokens = 3;

        unregisterSnitchesForTokenConfigTest();

        try
        {
            DatabaseDescriptor.applyTokensConfig(testConfig);
        }
        finally
        {
            unregisterSnitchesForTokenConfigTest();
        }

        // Tests that applyConfig() does not Except when number of tokens in inital_token matches num_tokens.
        //DatabaseDescriptor.loadConfig();
    }

    @Test
    public void testApplyInitialTokensInitialTokensSetNumTokensSetAndDoesntMatch() throws Exception
    {
        Config testConfig = DatabaseDescriptor.loadConfig();
        testConfig.initial_token = "0,256,1024";
        testConfig.num_tokens = 10;

        unregisterSnitchesForTokenConfigTest();

        try
        {
            DatabaseDescriptor.applyTokensConfig(testConfig);

            Assert.fail("initial_token = 0,256,1024 and num_tokens = 10 but applyInitialTokens() did not fail!");
        }
        catch (ConfigurationException ex)
        {
            Assert.assertEquals("The number of initial tokens (by initial_token) specified (3) is different from num_tokens value (10)",
                                ex.getMessage());
        }
        finally
        {
            unregisterSnitchesForTokenConfigTest();
        }
    }

    @Test
    public void testApplyInitialTokensInitialTokensSetNumTokensNotSet() throws Exception
    {
        Config testConfig = DatabaseDescriptor.loadConfig();

        unregisterSnitchesForTokenConfigTest();

        try
        {
            testConfig.initial_token = "0,256,1024";
            testConfig.num_tokens = null;
            DatabaseDescriptor.applyTokensConfig(testConfig);
        }
        finally
        {
            unregisterSnitchesForTokenConfigTest();
        }

        Assert.assertNull(testConfig.num_tokens);
        Assert.assertEquals(3, DatabaseDescriptor.tokensFromString(testConfig.initial_token).size());
    }

    @Test
    public void testApplyInitialTokensInitialTokensNotSetNumTokensSet() throws Exception
    {
        Config testConfig = DatabaseDescriptor.loadConfig();
        testConfig.num_tokens = 3;

        unregisterSnitchesForTokenConfigTest();

        try
        {
            DatabaseDescriptor.applyTokensConfig(testConfig);
        }
        finally
        {
            unregisterSnitchesForTokenConfigTest();
        }

        Assert.assertEquals(Integer.valueOf(3), testConfig.num_tokens);
        Assert.assertTrue(DatabaseDescriptor.tokensFromString(testConfig.initial_token).isEmpty());
    }

    @Test
    public void testApplyInitialTokensInitialTokensNotSetNumTokensNotSet() throws Exception
    {
        Config testConfig = DatabaseDescriptor.loadConfig();

        unregisterSnitchesForTokenConfigTest();

        try
        {
            DatabaseDescriptor.applyTokensConfig(testConfig);
        }
        finally
        {
            unregisterSnitchesForTokenConfigTest();
        }

        Assert.assertEquals(Integer.valueOf(1), testConfig.num_tokens);
        Assert.assertTrue(DatabaseDescriptor.tokensFromString(testConfig.initial_token).isEmpty());
    }

    private void unregisterSnitchesForTokenConfigTest() throws Exception
    {
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.unregisterMBean(new ObjectName("org.apache.cassandra.db:type=DynamicEndpointSnitch"));
            mbs.unregisterMBean(new ObjectName("org.apache.cassandra.db:type=EndpointSnitchInfo"));
        }
        catch (InstanceNotFoundException ex)
        {
            // ok
        }
    }
}
