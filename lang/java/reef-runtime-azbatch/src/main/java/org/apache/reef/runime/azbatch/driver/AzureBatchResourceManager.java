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
import com.microsoft.windowsazure.storage.StorageException;
import com.microsoft.windowsazure.storage.blob.BlobProperties;
import com.microsoft.windowsazure.storage.blob.CloudBlockBlob;
import org.apache.commons.lang.StringUtils;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.evaluator.EvaluatorProcess;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountKey;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountName;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountUri;
import org.apache.reef.runime.azbatch.parameters.AzureBatchPoolId;
import org.apache.reef.runtime.common.driver.api.ResourceLaunchEvent;
import org.apache.reef.runtime.common.driver.api.ResourceRequestEvent;
import org.apache.reef.runtime.common.driver.api.RuntimeParameters;
import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorEventImpl;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceAllocationEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceEventImpl;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.runtime.hdinsight.client.AzureUploader;
import org.apache.reef.runtime.hdinsight.client.yarnrest.LocalResource;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.CollectionUtils;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.joda.time.DateTime;

import javax.inject.Inject;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A resource manager that uses threads to execute containers.
 */
@Private
@DriverSide
public final class AzureBatchResourceManager {

  private static final Logger LOG = Logger.getLogger(AzureBatchResourceManager.class.getName());
  private final EventHandler<ResourceAllocationEvent> resourceAllocationHandler;
  private final EventHandler<NodeDescriptorEvent> nodeDescriptorHandler;
  private String localAddress;
  private final REEFFileNames fileNames;
  private final ConfigurationSerializer configurationSerializer;
  private final double jvmHeapFactor;
  private final String azureBatchAccountUri;
  private final String azureBatchAccountName;
  private final String azureBatchAccountKey;
  private final String azureBatchPoolId;
  private final String applicationId;
  private final AzureUploader azureUploader;

  @Inject
  AzureBatchResourceManager(
      @Parameter(RuntimeParameters.ResourceAllocationHandler.class) final EventHandler<ResourceAllocationEvent> resourceAllocationHandler,
      @Parameter(RuntimeParameters.NodeDescriptorHandler.class) final EventHandler<NodeDescriptorEvent> nodeDescriptorHandler,
      final LocalAddressProvider localAddressProvider,
      final REEFFileNames fileNames,
      final ConfigurationSerializer configurationSerializer,
      @Parameter(JVMHeapSlack.class) final double jvmHeapSlack,
      final AzureUploader azureUploader,
      @Parameter(AzureBatchAccountUri.class) final String azureBatchAccountUri,
      @Parameter(AzureBatchAccountName.class) final String azureBatchAccountName,
      @Parameter(AzureBatchAccountKey.class) final String azureBatchAccountKey,
      @Parameter(AzureBatchPoolId.class) final String azureBatchPoolId) {
    this.resourceAllocationHandler = resourceAllocationHandler;
    this.nodeDescriptorHandler = nodeDescriptorHandler;
    this.localAddress = localAddressProvider.getLocalAddress();
    this.fileNames = fileNames;
    this.configurationSerializer = configurationSerializer;
    this.jvmHeapFactor = 1.0 - jvmHeapSlack;
    this.azureUploader = azureUploader;
    this.azureBatchAccountKey = azureBatchAccountKey;
    this.azureBatchAccountName = azureBatchAccountName;
    this.azureBatchAccountUri = azureBatchAccountUri;
    this.azureBatchPoolId = azureBatchPoolId;
    this.applicationId = "EvaluatorJob-"
        + this.azureBatchAccountName + "-"
        + (new Date()).toString()
        .replace(' ', '-')
        .replace(':', '-')
        .replace('.', '-');
  }

  public void onResourceRequested(ResourceRequestEvent resourceRequestEvent) {
    final String id = UUID.randomUUID().toString();
    final int memorySize = resourceRequestEvent.getMemorySize().get();
    this.nodeDescriptorHandler.onNext(NodeDescriptorEventImpl.newBuilder()
        .setIdentifier(id)
        .setHostName(this.localAddress)
        .setPort(1234)
        .setMemorySize(memorySize)
        .build());

    this.resourceAllocationHandler.onNext(ResourceEventImpl.newAllocationBuilder()
        .setIdentifier(id)
        .setNodeId(id)
        .setResourceMemory(memorySize)
        .setVirtualCores(1)
        .setRuntimeName(RuntimeIdentifier.RUNTIME_NAME)
        .build());
  }

  public void onResourceLaunched(ResourceLaunchEvent resourceLaunchEvent) {
    // Make the configuration file of the evaluator.
    final File evaluatorConfigurationFile = new File(this.fileNames.getEvaluatorConfigurationPath());
    try {
      this.configurationSerializer.toFile(resourceLaunchEvent.getEvaluatorConf(), evaluatorConfigurationFile);
    } catch (final IOException | BindException e) {
      throw new RuntimeException("Unable to write configuration.", e);
    }

    final List<String> command = getLaunchCommand(resourceLaunchEvent);

    try {
      LaunchBatchTaskWithConf(evaluatorConfigurationFile, command);
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "Error submitting Azure Batch request", ex);
      throw new RuntimeException(ex);
    }
  }

  private List<String> getLaunchCommand(final ResourceLaunchEvent launchRequest) {
    // TODO: Rebuild this command using JavaLaunchCommandBuilder
    return Collections.unmodifiableList(Arrays.asList(
        "/bin/sh -c \"",
        "unzip local.jar;",
        "java -Xmx256m -XX:PermSize=128m -XX:MaxPermSize=128m -classpath local/*:global/*",
        "-Dproc_reef org.apache.reef.runtime.common.REEFLauncher reef/local/evaluator.conf",
        "\""
    ));
  }

  private void LaunchBatchTaskWithConf(File evaluatorConf, List<String> command) throws IOException {
    // TODO: Reuse Job Id to submit the task and avoid using JobManagerTask
    LOG.log(Level.INFO, "onResourceLaunched LaunchBatchTaskWithConf ", StringUtils.join(command, ' '));
    BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(
        this.azureBatchAccountUri, this.azureBatchAccountName, this.azureBatchAccountKey);
    BatchClient client = BatchClient.open(cred);

    PoolInformation poolInfo = new PoolInformation();
    poolInfo.withPoolId(this.azureBatchPoolId);


    final LocalResource uploadedConfFile = this.azureUploader.uploadFile(evaluatorConf);
    final ResourceFile confSourceFile = new ResourceFile()
        .withBlobSource(uploadedConfFile.getUrl())
        .withFilePath(evaluatorConf.getPath());

    final File localJar = new File("local.jar");
    final LocalResource jarFile = this.azureUploader.uploadFile(localJar);
    final ResourceFile jarSourceFile = new ResourceFile()
        .withBlobSource(jarFile.getUrl())
        .withFilePath(localJar.getPath());

    List<ResourceFile> resources = new ArrayList<>();
    resources.add(confSourceFile);
    resources.add(jarSourceFile);

    JobManagerTask jobManagerTask = new JobManagerTask()
        .withId(this.applicationId)
        .withResourceFiles(resources)
        .withCommandLine(StringUtils.join(command, ' '));

    JobAddParameter jobAddParameter = new JobAddParameter()
        .withId(this.applicationId)
        .withJobManagerTask(jobManagerTask)
        .withPoolInfo(poolInfo);

    client.jobOperations().createJob(jobAddParameter);
  }
}
