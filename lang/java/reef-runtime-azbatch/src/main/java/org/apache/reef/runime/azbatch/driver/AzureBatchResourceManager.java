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
import com.microsoft.azure.batch.protocol.models.ResourceFile;
import com.microsoft.azure.batch.protocol.models.TaskAddParameter;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountKey;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountName;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountUri;
import org.apache.reef.runime.azbatch.util.AzureBatchFileNames;
import org.apache.reef.runime.azbatch.util.AzureStorageUtil;
import org.apache.reef.runime.azbatch.util.CommandBuilder;
import org.apache.reef.runtime.common.driver.api.ResourceLaunchEvent;
import org.apache.reef.runtime.common.driver.api.ResourceReleaseEvent;
import org.apache.reef.runtime.common.driver.api.ResourceRequestEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorEventImpl;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceEventImpl;
import org.apache.reef.runtime.common.files.*;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.remote.address.LocalAddressProvider;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A resource manager that uses threads to execute containers.
 */
@Private
@DriverSide
public final class AzureBatchResourceManager {

  private static final Logger LOG = Logger.getLogger(AzureBatchResourceManager.class.getName());
  private static final String AZ_BATCH_JOB_ID_ENV = "AZ_BATCH_JOB_ID";

  private final REEFFileNames fileNames;
  private final ConfigurationSerializer configurationSerializer;
  private final String azureBatchAccountUri;
  private final String azureBatchAccountName;
  private final String azureBatchAccountKey;
  private final String jobId;

  private final AzureStorageUtil azureStorageUtil;
  private final REEFEventHandlers reefEventHandlers;
  private final String taskId;
  private final Map<String, ResourceRequestEvent> containers = new ConcurrentHashMap<>();

  private final String localAddress;
  private final double jvmHeapFactor;
  private final JobJarMaker jobJarMaker;
  private final CommandBuilder launchCommandBuilder;

  @Inject
  AzureBatchResourceManager(
      final LocalAddressProvider localAddressProvider,
      final REEFFileNames fileNames,
      final REEFEventHandlers reefEventHandlers,
      final ConfigurationSerializer configurationSerializer,
      final AzureStorageUtil azureStorageUtil,
      final JobJarMaker jobJarMaker,
      final CommandBuilder launchCommandBuilder,
      @Parameter(AzureBatchAccountUri.class) final String azureBatchAccountUri,
      @Parameter(AzureBatchAccountName.class) final String azureBatchAccountName,
      @Parameter(AzureBatchAccountKey.class) final String azureBatchAccountKey,
      @Parameter(JVMHeapSlack.class) final double jvmHeapSlack) {
    this.localAddress = localAddressProvider.getLocalAddress();
    this.fileNames = fileNames;
    this.reefEventHandlers = reefEventHandlers;
    this.configurationSerializer = configurationSerializer;
    this.azureStorageUtil = azureStorageUtil;
    this.azureBatchAccountKey = azureBatchAccountKey;
    this.azureBatchAccountName = azureBatchAccountName;
    this.azureBatchAccountUri = azureBatchAccountUri;
    this.jobId = System.getenv(AZ_BATCH_JOB_ID_ENV);
    this.taskId = "EvaluatorTask-"
        + this.azureBatchAccountName + "-"
        + (new Date()).toString()
        .replace(' ', '-')
        .replace(':', '-')
        .replace('.', '-');
    this.jvmHeapFactor = 1.0 - jvmHeapSlack;
    this.jobJarMaker = jobJarMaker;
    this.launchCommandBuilder = launchCommandBuilder;
  }

  public void onResourceRequested(final ResourceRequestEvent resourceRequestEvent) {
    final String id = UUID.randomUUID().toString();
    final int memorySize = resourceRequestEvent.getMemorySize().get();

    // TODO: Investigate nodeDescriptorHandler usage and remove below dummy node descriptor.
    this.reefEventHandlers.onNodeDescriptor(NodeDescriptorEventImpl.newBuilder()
        .setIdentifier(id)
        .setHostName(this.localAddress)
        .setPort(1234)
        .setMemorySize(memorySize)
        .build());

    this.reefEventHandlers.onResourceAllocation(ResourceEventImpl.newAllocationBuilder()
        .setIdentifier(id)
        .setNodeId(id)
        .setResourceMemory(memorySize)
        .setVirtualCores(1)
        .setRuntimeName(RuntimeIdentifier.RUNTIME_NAME)
        .build());

    this.containers.put(id, resourceRequestEvent);
  }

  public void onResourceReleased(final ResourceReleaseEvent resourceReleaseEvent) {
    String id = resourceReleaseEvent.getIdentifier();
    LOG.log(Level.FINEST, "Got onResourceReleasedEvent of Id: {0} in AzureBatchResourceManager", id);
    this.containers.remove(id);
  }

  public void onResourceLaunched(final ResourceLaunchEvent resourceLaunchEvent) {
    // Make the configuration file of the evaluator.
    final File evaluatorConfigurationFile = new File(this.fileNames.getEvaluatorConfigurationPath());
    try {
      this.configurationSerializer.toFile(resourceLaunchEvent.getEvaluatorConf(), evaluatorConfigurationFile);
    } catch (final IOException | BindException e) {
      throw new RuntimeException("Unable to write configuration.", e);
    }

    ResourceRequestEvent container = this.containers.get(resourceLaunchEvent.getIdentifier());
    String command = this.launchCommandBuilder.buildEvaluatorCommand(
        resourceLaunchEvent, container.getMemorySize().get(), this.jvmHeapFactor);

    try {
      File jarFile = buildEvaluatorSubmissionJar(evaluatorConfigurationFile);
      launchBatchTaskWithConf(command, jarFile);
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "Error submitting Azure Batch request", ex);
      throw new RuntimeException(ex);
    }
  }

  private File buildEvaluatorSubmissionJar(final File evaluatorConfigurationFile) throws IOException {

    final Configuration evaluatorConfig = this.configurationSerializer.fromFile(evaluatorConfigurationFile);
    Set<FileResource> localFiles = new HashSet<>();
    localFiles.add(getFileResourceFromFile(evaluatorConfigurationFile, FileType.PLAIN));

    Set<FileResource> globalFiles = new HashSet<>();

    File globalFolder = new File(this.fileNames.getGlobalFolderPath());
    for (final File fileEntry : globalFolder.listFiles()) {
      globalFiles.add(getFileResourceFromFile(fileEntry, FileType.LIB));
    }

    return this.jobJarMaker.createEvaluatorSubmissionJAR(
        evaluatorConfig,
        globalFiles,
        localFiles);
  }

  private FileResource getFileResourceFromFile(final File configFile, final FileType type) {
    return FileResourceImpl.newBuilder()
        .setName(configFile.getName())
        .setPath(configFile.getPath())
        .setType(type).build();
  }

  private void launchBatchTaskWithConf(final String command, final File jarFile) throws IOException {
    BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(
        this.azureBatchAccountUri, this.azureBatchAccountName, this.azureBatchAccountKey);
    BatchClient client = BatchClient.open(cred);

    final String folderName = this.fileNames.getAzbatchJobFolderPath() + this.jobId;
    LOG.log(Level.FINE, "Creating a job folder on Azure at: {0}.", folderName);
    URI jobFolderURL = this.azureStorageUtil.createFolder(folderName);

    final URI jarFileUri = this.azureStorageUtil.uploadFile(jobFolderURL, jarFile);
    final ResourceFile jarSourceFile = new ResourceFile()
        .withBlobSource(jarFileUri.toString())
        .withFilePath(AzureBatchFileNames.TASK_JAR_FILE_NAME);

    List<ResourceFile> resources = new ArrayList<>();
    resources.add(jarSourceFile);

    TaskAddParameter taskAddParameter = new TaskAddParameter()
        .withId(this.taskId)
        .withResourceFiles(resources)
        .withCommandLine(command);

    client.taskOperations().createTask(jobId, taskAddParameter);
  }
}
