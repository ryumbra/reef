package org.apache.reef.runime.azbatch.client;

import org.apache.reef.runime.azbatch.parameters.*;
import org.apache.reef.runime.azbatch.util.CommandBuilder;
import org.apache.reef.runime.azbatch.util.LinuxCommandBuilder;
import org.apache.reef.runime.azbatch.util.WindowsCommandBuilder;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;

public class AzureBatchRuntimeConfigurationCreator {
  /**
   * The ConfigurationModule for the local resourcemanager.
   */
  public static ConfigurationModule CONF = null;

  public static ConfigurationModule GetOrCreateAzureBatchRuntimeConfiguration
      (Boolean isWindows) {

    if (AzureBatchRuntimeConfigurationCreator.CONF == null) {
      ConfigurationModuleBuilder builder = AzureBatchRuntimeConfigurationStatic.CONF;
      ConfigurationModule module = null;
      if (isWindows) {
        module = builder.bindImplementation(CommandBuilder.class, WindowsCommandBuilder.class).build();
      } else {
        module = builder.bindImplementation(CommandBuilder.class, LinuxCommandBuilder.class).build();
      }

      AzureBatchRuntimeConfigurationCreator.CONF = new AzureBatchRuntimeConfiguration()
          .merge(module)
          .bindNamedParameter(AzureBatchAccountName.class, AzureBatchRuntimeConfiguration.AZURE_BATCH_ACCOUNT_NAME)
          .bindNamedParameter(AzureBatchAccountUri.class, AzureBatchRuntimeConfiguration.AZURE_BATCH_ACCOUNT_URI)
          .bindNamedParameter(AzureBatchAccountKey.class, AzureBatchRuntimeConfiguration.AZURE_BATCH_ACCOUNT_KEY)
          .bindNamedParameter(AzureBatchPoolId.class, AzureBatchRuntimeConfiguration.AZURE_BATCH_POOL_ID)
          .bindNamedParameter(AzureStorageAccountName.class, AzureBatchRuntimeConfiguration.AZURE_STORAGE_ACCOUNT_NAME)
          .bindNamedParameter(AzureStorageAccountKey.class, AzureBatchRuntimeConfiguration.AZURE_STORAGE_ACCOUNT_KEY)
          .bindNamedParameter(AzureStorageContainerName.class, AzureBatchRuntimeConfiguration.AZURE_STORAGE_CONTAINER_NAME)
          .build();
    }

    return AzureBatchRuntimeConfigurationCreator.CONF;
  }
}
