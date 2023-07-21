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

package org.apache.cassandra.security;

import java.security.Provider;
import java.security.Security;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;

import static java.lang.String.format;

public abstract class AbstractCryptoProvider
{
    public static final String FAIL_ON_MISSING_PROVIDER_KEY = "fail_on_missing_provider";
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected final boolean failOnMissingProvider;

    public AbstractCryptoProvider(Map<String, String> properties)
    {
        failOnMissingProvider = properties != null && Boolean.parseBoolean(properties.getOrDefault(FAIL_ON_MISSING_PROVIDER_KEY, "false"));
    }

    /**
     * Returns name of the provider, as returned from {@link Provider#getName()}
     *
     * @return name of the provider
     */
    public abstract String getProviderName();

    /**
     * Returns the name of the class which installs specific provider of name {@link #getProviderName()}.
     *
     * @return name of class of provider
     */
    public abstract String getProviderClassAsString();

    /**
     * Returns a runnable which installs this crypto provider.
     *
     * @return runnable which installs this provider
     */
    protected abstract Runnable installator() throws Exception;

    /**
     * Returns boolean telling if this provider was installed properly.
     *
     * @return {@code true} if provider was installed properly, {@code false} otherwise.
     */
    protected abstract boolean isHealthyInstallation() throws Exception;

    /**
     * The default installation runs {@link AbstractCryptoProvider#installator()} and after that
     * {@link AbstractCryptoProvider#isHealthyInstallation()}.
     * <p>
     * If any step fails, it will not throw an exception unless the parameter
     * {@link AbstractCryptoProvider#FAIL_ON_MISSING_PROVIDER_KEY} is {@code true}.
     */
    public void install() throws Exception
    {
        if (NoOpCryptoProvider.class.getSimpleName().equals(getProviderName()))
        {
            logger.debug("Installation of a crypto provider was skipped.");
            return;
        }

        String failureMessage = null;
        try
        {
            Class.forName(getProviderClassAsString());

            Provider[] providers = Security.getProviders();

            if (providers.length > 0 && providers[0] != null && providers[0].getName().equals(getProviderName()))
            {
                logger.debug("{} was already installed", getProviderName());
                return;
            }
            else
            {
                installator().run();
            }

            if (isHealthyInstallation())
            {
                logger.info("{} successfully passed the healthiness check", getProviderName());
            }
            else
            {
                failureMessage = format("%s is not the highest priority provider. " +
                                        "The most probable cause is that Cassandra node is not running on the same architecture " +
                                        "the provider library is for. Please place the architecture-specific library " +
                                        "for %s to the classpath and try again. ",
                                        getProviderName(),
                                        getProviderClassAsString());
            }
        }
        catch (ClassNotFoundException ex)
        {
            failureMessage = getProviderClassAsString() + " is not on the class path!";
        }
        catch (Exception e)
        {
            failureMessage = format("The installation of %s was not successful, reason: %s",
                                    getProviderClassAsString(), e.getMessage());
        }

        if (failureMessage != null)
            if (failOnMissingProvider)
                throw new ConfigurationException(failureMessage);
            else
                logger.warn(failureMessage);
    }
}
