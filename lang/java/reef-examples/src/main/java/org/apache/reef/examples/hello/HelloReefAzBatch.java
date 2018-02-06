/*
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
 */
package org.apache.reef.examples.hello;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.REEF;
import org.apache.reef.runime.azbatch.client.AzureBatchRuntimeConfiguration;
import org.apache.reef.runime.azbatch.client.AzureBatchRuntimeConfigurationCreator;
import org.apache.reef.runime.azbatch.parameters.*;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A main() for running hello REEF in Azure Batch.
 */
public final class HelloReefAzBatch {

  private final String azureBatchAccountName;
  private final String azureBatchAccountKey;
  private final String azureBatchAccountUri;
  private final String azureBatchPoolId;
  private final String azureStorageAccountName;
  private final String azureStorageAccountKey;
  private final String azureStorageContainerName;
  private final Boolean isWindows;

  private static final Logger LOG = Logger.getLogger(HelloReefAzBatch.class.getName());

  /**
   * Number of milliseconds to wait for the job to complete.
   */
  private static final int JOB_TIMEOUT = 60000; // 60 sec.

  /**
   * Builds the rutime configuration for Azure Batch.
   *
   * @return the configuration of the runtime.
   * @throws IOException
   */
  private static Configuration getEnvironmentConfiguration() throws IOException {
    return AzureBatchRuntimeConfiguration.fromEnvironment();
  }

  /**
   * Builds and returns driver configuration for HelloREEF driver.
   *
   * @return the configuration of the HelloREEF driver.
   */
  private static Configuration getDriverConfiguration() {
    return DriverConfiguration.CONF
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "HelloREEF")
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(HelloDriver.class))
        .set(DriverConfiguration.ON_DRIVER_STARTED, HelloDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, HelloDriver.EvaluatorAllocatedHandler.class)
        .build();
  }

  /**
   * Start Hello REEF job with AzBatch runtime.
   *
   * @param args command line parameters.
   * @throws InjectionException configuration error.
   * @throws IOException
   */
  public static void main(final String[] args) throws InjectionException, IOException {

    Configuration partialConfiguration = getEnvironmentConfiguration();
    final Injector injector = Tang.Factory.getTang().newInjector(partialConfiguration);
    final HelloReefAzBatch launcher = injector.getInstance(HelloReefAzBatch.class);

    launcher.launch();
  }

  public void launch() throws InjectionException {
    Configuration driverConfiguration = getDriverConfiguration();

    final Configuration runtimeConfiguration = AzureBatchRuntimeConfigurationCreator
        .GetOrCreateAzureBatchRuntimeConfiguration(this.isWindows)
        .set(AzureBatchRuntimeConfiguration.AZURE_BATCH_ACCOUNT_NAME, this.azureBatchAccountName)
        .set(AzureBatchRuntimeConfiguration.AZURE_BATCH_ACCOUNT_KEY, this.azureBatchAccountKey)
        .set(AzureBatchRuntimeConfiguration.AZURE_BATCH_ACCOUNT_URI, this.azureBatchAccountUri)
        .set(AzureBatchRuntimeConfiguration.AZURE_BATCH_POOL_ID, this.azureBatchPoolId)
        .set(AzureBatchRuntimeConfiguration.AZURE_STORAGE_ACCOUNT_NAME, this.azureStorageAccountName)
        .set(AzureBatchRuntimeConfiguration.AZURE_STORAGE_ACCOUNT_KEY, this.azureStorageAccountKey)
        .set(AzureBatchRuntimeConfiguration.AZURE_STORAGE_CONTAINER_NAME, this.azureStorageContainerName)
        .build();

    try (final REEF reef = Tang.Factory.getTang().newInjector(runtimeConfiguration).getInstance(REEF.class)) {
      reef.submit(driverConfiguration);
    }
    LOG.log(Level.INFO, "Job Submitted");
  }

  /**
   * Private constructor.
   */
  @Inject
  private HelloReefAzBatch(
      @Parameter(AzureBatchAccountName.class) final String azureBatchAccountName,
      @Parameter(AzureBatchAccountKey.class) final String azureBatchAccountKey,
      @Parameter(AzureBatchAccountUri.class) final String azureBatchAccountUri,
      @Parameter(AzureBatchPoolId.class) final String azureBatchPoolId,
      @Parameter(AzureStorageAccountName.class) final String azureStorageAccountName,
      @Parameter(AzureStorageAccountKey.class) final String azureStorageAccountKey,
      @Parameter(AzureStorageContainerName.class) final String azureStorageContainerName,
      @Parameter(IsWindows.class) final Boolean IsWindows
      ) {
    this.azureBatchAccountName = azureBatchAccountName;
    this.azureBatchAccountKey = azureBatchAccountKey;
    this.azureBatchAccountUri = azureBatchAccountUri;
    this.azureBatchPoolId = azureBatchPoolId;
    this.azureStorageAccountName = azureStorageAccountName;
    this.azureStorageAccountKey = azureStorageAccountKey;
    this.azureStorageContainerName = azureStorageContainerName;
    this.isWindows = IsWindows;
  }
}
