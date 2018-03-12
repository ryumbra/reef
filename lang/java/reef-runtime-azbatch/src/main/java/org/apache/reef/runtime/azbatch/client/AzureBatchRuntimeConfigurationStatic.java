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
package org.apache.reef.runtime.azbatch.client;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.azbatch.AzureBatchClasspathProvider;
import org.apache.reef.runtime.azbatch.AzureBatchJVMPathProvider;
import org.apache.reef.runtime.azbatch.util.batch.IAzureBatchCredentialProvider;
import org.apache.reef.runtime.azbatch.util.batch.SharedKeyBatchCredentialProvider;
import org.apache.reef.runtime.azbatch.util.storage.ICloudBlobClientProvider;
import org.apache.reef.runtime.azbatch.util.storage.StorageKeyCloudBlobProvider;
import org.apache.reef.runtime.common.client.CommonRuntimeConfiguration;
import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.client.api.JobSubmissionHandler;
import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;
import org.apache.reef.runtime.common.files.RuntimePathProvider;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.util.logging.LoggingSetup;

/**
 * The static part of the AzureBatchRuntimeConfigurationStatic.
 */
@Private
public class AzureBatchRuntimeConfigurationStatic extends ConfigurationModuleBuilder {
  static {
    LoggingSetup.setupCommonsLogging();
  }

  public static final ConfigurationModuleBuilder CONF = new AzureBatchRuntimeConfigurationStatic()
      .merge(CommonRuntimeConfiguration.CONF)
      .bindImplementation(JobSubmissionHandler.class, AzureBatchJobSubmissionHandler.class)
      .bindImplementation(IAzureBatchCredentialProvider.class, SharedKeyBatchCredentialProvider.class)
      .bindImplementation(ICloudBlobClientProvider.class, StorageKeyCloudBlobProvider.class)
      .bindImplementation(DriverConfigurationProvider.class, AzureBatchDriverConfigurationProviderImpl.class)
      .bindImplementation(RuntimeClasspathProvider.class, AzureBatchClasspathProvider.class)
      .bindImplementation(RuntimePathProvider.class, AzureBatchJVMPathProvider.class);
}
