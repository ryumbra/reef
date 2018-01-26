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
package org.apache.reef.runime.azbatch.driver;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountKey;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountName;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountUri;
import org.apache.reef.runime.azbatch.parameters.AzureBatchPoolId;
import org.apache.reef.runtime.common.client.api.JobSubmissionEvent;
import org.apache.reef.runtime.common.driver.api.ResourceLaunchEvent;
import org.apache.reef.runtime.common.driver.api.ResourceLaunchHandler;
import org.apache.reef.runtime.common.files.JobJarMaker;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.hdinsight.client.AzureUploader;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.JARFileMaker;
import org.apache.reef.util.logging.Config;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A {@link ResourceLaunchHandler} for Azure Batch.
 */
@Private
public final class AzureBatchResourceLaunchHandler implements ResourceLaunchHandler {

  private final ConfigurationSerializer configurationSerializer;
  private static final Logger LOG = Logger.getLogger(AzureBatchResourceLaunchHandler.class.getName());
  private final REEFFileNames fileNames;
  private final JobJarMaker jobJarMaker;
  private final AzureUploader azureUploader;

    private final String azureBatchAccountUri;
    private final String azureBatchAccountName;
    private final String azureBatchAccountKey;
    private final String azureBatchPoolId;

    @Inject
  AzureBatchResourceLaunchHandler(final AzureUploader azureUploader,
                                  final ConfigurationSerializer configurationSerializer,
                                  final REEFFileNames fileNames,
                                  final JobJarMaker jobJarMaker,
                                  @Parameter(AzureBatchAccountUri.class) final String azureBatchAccountUri,
                                  @Parameter(AzureBatchAccountName.class) final String azureBatchAccountName,
                                  @Parameter(AzureBatchAccountKey.class) final String azureBatchAccountKey,
                                  @Parameter(AzureBatchPoolId.class) final String azureBatchPoolId) {
      this.azureUploader = azureUploader;
      this.fileNames = fileNames;
      this.configurationSerializer = configurationSerializer;
      this.jobJarMaker = jobJarMaker;
      this.azureBatchAccountUri = azureBatchAccountUri;
      this.azureBatchAccountName = azureBatchAccountName;
      this.azureBatchAccountKey = azureBatchAccountKey;
      this.azureBatchPoolId = azureBatchPoolId;
  }

  @Override
  public void onNext(final ResourceLaunchEvent resourceLaunchEvent) {
      try {
      LOG.log(Level.INFO, "resourceLaunch. {0}", resourceLaunchEvent.toString());

          final File localStagingFolder =
                  Files.createTempDirectory(this.fileNames.getEvaluatorFolderPrefix()).toFile();

          final Configuration evaluatorConfiguration = Tang.Factory.getTang()
                  .newConfigurationBuilder(resourceLaunchEvent.getEvaluatorConf())
                  .bindNamedParameter(AzureBatchAccountUri.class,this.azureBatchAccountUri)
                  .bindNamedParameter(AzureBatchAccountName.class,this.azureBatchAccountName)
                  .bindNamedParameter(AzureBatchAccountKey.class,this.azureBatchAccountKey)
                  .bindNamedParameter(AzureBatchPoolId.class,this.azureBatchPoolId)
                  .build();

          evaluatorConfiguration.
          f = this.fileNames.getDriverConfigurationPath();
          this.configurationSerializer.fromFile()
          final File configurationFile = new File(
                  localStagingFolder, this.fileNames.getEvaluatorConfigurationName());
          this.configurationSerializer.toFile(evaluatorConfiguration, configurationFile);

          JobJarMaker.copy(resourceLaunchEvent.getFileSet(), localStagingFolder);
          final File jarFile = File.createTempFile(this.fileNames.getJobFolderPrefix(), this.fileNames.getJarFileSuffix());
          new JARFileMaker(jarFile).addChildren(localStagingFolder).close();

          final String command = getCommandString(resourceLaunchEvent);
      helper.submit(uploadedFile, command);

  } catch (final IOException e) {
        throw new RuntimeException(e);
    }
  }

    private List<String> getCommandList(final ResourceLaunchEvent resourceLaunchEvent) {

        // Task 122137: Use JavaLaunchCommandBuilder to generate command to start REEF Driver
        return Collections.unmodifiableList(Arrays.asList(
                "/bin/sh -c \"",
                "ln -sf '.' 'reef';",
                "unzip local.jar;",
                "java -Xmx256m -XX:PermSize=128m -XX:MaxPermSize=128m -classpath reef/local/*:reef/global/*",
                "-Dproc_reef org.apache.reef.runtime.common.REEFLauncher reef/local/evaluator.conf",
                "\""
        ));
    }

    /**
     * Assembles the command to execute the Driver.
     */
    private String getCommandString(final ResourceLaunchEvent resourceLaunchEvent) {
        return StringUtils.join(getCommandList(resourceLaunchEvent), ' ');
    }

}
