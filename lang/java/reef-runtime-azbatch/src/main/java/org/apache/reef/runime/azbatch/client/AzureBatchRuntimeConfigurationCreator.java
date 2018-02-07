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
package org.apache.reef.runime.azbatch.client;

import org.apache.reef.runime.azbatch.parameters.*;
import org.apache.reef.runime.azbatch.util.CommandBuilder;
import org.apache.reef.runime.azbatch.util.LinuxCommandBuilder;
import org.apache.reef.runime.azbatch.util.WindowsCommandBuilder;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;

/**
 * Class that builds the ConfigurationModule for Azure Batch runtime.
 */
public final class AzureBatchRuntimeConfigurationCreator {
  /**
   * The ConfigurationModule for Azure Batch.
   */
  private static ConfigurationModule conf;

  public static ConfigurationModule getOrCreateAzureBatchRuntimeConfiguration(final Boolean isWindows) {

    if (AzureBatchRuntimeConfigurationCreator.conf == null) {
      ConfigurationModuleBuilder builder = AzureBatchRuntimeConfigurationStatic.CONF;
      ConfigurationModule module = null;
      if (isWindows) {
        module = builder.bindImplementation(CommandBuilder.class, WindowsCommandBuilder.class).build();
      } else {
        module = builder.bindImplementation(CommandBuilder.class, LinuxCommandBuilder.class).build();
      }

      AzureBatchRuntimeConfigurationCreator.conf = new AzureBatchRuntimeConfiguration()
          .merge(module)
          .bindNamedParameter(AzureBatchAccountName.class, AzureBatchRuntimeConfiguration.AZURE_BATCH_ACCOUNT_NAME)
          .bindNamedParameter(AzureBatchAccountUri.class, AzureBatchRuntimeConfiguration.AZURE_BATCH_ACCOUNT_URI)
          .bindNamedParameter(AzureBatchAccountKey.class, AzureBatchRuntimeConfiguration.AZURE_BATCH_ACCOUNT_KEY)
          .bindNamedParameter(AzureBatchPoolId.class, AzureBatchRuntimeConfiguration.AZURE_BATCH_POOL_ID)
          .bindNamedParameter(AzureStorageAccountName.class, AzureBatchRuntimeConfiguration.AZURE_STORAGE_ACCOUNT_NAME)
          .bindNamedParameter(AzureStorageAccountKey.class, AzureBatchRuntimeConfiguration.AZURE_STORAGE_ACCOUNT_KEY)
          .bindNamedParameter(
              AzureStorageContainerName.class, AzureBatchRuntimeConfiguration.AZURE_STORAGE_CONTAINER_NAME)
          .build();
    }

    return AzureBatchRuntimeConfigurationCreator.conf;
  }

  /*
   * Private constructor since this is a utility class.
   */
  private AzureBatchRuntimeConfigurationCreator(){
  }

}
