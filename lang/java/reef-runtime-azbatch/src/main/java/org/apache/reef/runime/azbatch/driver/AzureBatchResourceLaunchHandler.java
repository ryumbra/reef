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

import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.auth.BatchSharedKeyCredentials;
import com.microsoft.azure.batch.protocol.models.JobAddParameter;
import com.microsoft.azure.batch.protocol.models.JobManagerTask;
import com.microsoft.azure.batch.protocol.models.PoolInformation;
import com.microsoft.azure.batch.protocol.models.ResourceFile;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountKey;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountName;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountUri;
import org.apache.reef.runime.azbatch.parameters.AzureBatchPoolId;
import org.apache.reef.runtime.common.driver.api.ResourceLaunchEvent;
import org.apache.reef.runtime.common.driver.api.ResourceLaunchHandler;
import org.apache.reef.runtime.common.files.JobJarMaker;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.hdinsight.client.AzureUploader;
import org.apache.reef.runtime.hdinsight.client.yarnrest.LocalResource;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
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
    private String applicationId;

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
      this.applicationId = "HelloWorldJob";
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

          final File configurationFile = new File(this.fileNames.getEvaluatorConfigurationPath());
          this.configurationSerializer.toFile(evaluatorConfiguration, configurationFile);

/*
          JobJarMaker.copy(resourceLaunchEvent.getFileSet(), localStagingFolder);
          final File jarFile = File.createTempFile(this.fileNames.getJobFolderPrefix(), this.fileNames.getJarFileSuffix());
          new JARFileMaker(jarFile).addChildren(localStagingFolder).close();

*/
          final String command = getCommandString(resourceLaunchEvent);
          submit(configurationFile, command);
  } catch (final IOException e) {
        throw new RuntimeException(e);
    }
  }

    private void submit(File configurationFile, String command) {
        LOG.log(Level.INFO, "onResourceLaunched LaunchBatchTaskWithConf ", command);
        BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(
                    this.azureBatchAccountUri, this.azureBatchAccountName, this.azureBatchAccountKey);
        BatchClient client = BatchClient.open(cred);

                PoolInformation poolInfo = new PoolInformation();
        poolInfo.withPoolId(this.azureBatchPoolId);


        final LocalResource uploadedConfFile;
        try {
            uploadedConfFile = this.azureUploader.uploadFile(configurationFile);
        } catch (IOException e) {
            throw new RuntimeException("Unable to write configuration.", e);
        }
        final ResourceFile confSourceFile = new ResourceFile()
                    .withBlobSource(uploadedConfFile.getUrl())
                    .withFilePath(configurationFile.getPath());

                final File localJar = new File("local.jar");
        final LocalResource jarFile;
        try {
            jarFile = this.azureUploader.uploadFile(localJar);
        } catch (IOException e) {
            throw new RuntimeException("Unable to write jar file.", e);
        }
        final ResourceFile jarSourceFile = new ResourceFile()
                    .withBlobSource(jarFile.getUrl())
                    .withFilePath(localJar.getPath());

                List<ResourceFile> resources = new ArrayList<>();
        resources.add(confSourceFile);
        resources.add(jarSourceFile);

                JobManagerTask jobManagerTask = new JobManagerTask()
                    .withId(this.applicationId)
                    .withResourceFiles(resources)
                    .withCommandLine(command);

                JobAddParameter jobAddParameter = new JobAddParameter()
                    .withId(this.applicationId)
                    .withJobManagerTask(jobManagerTask)
                    .withPoolInfo(poolInfo);

        try {
            client.jobOperations().createJob(jobAddParameter);
        } catch (IOException e) {
            throw new RuntimeException("Unable to add task to job.", e);
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
