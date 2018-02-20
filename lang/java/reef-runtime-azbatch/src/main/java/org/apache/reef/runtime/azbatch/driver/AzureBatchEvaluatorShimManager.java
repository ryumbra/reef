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
package org.apache.reef.runtime.azbatch.driver;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.proto.EvaluatorShimProtocol;
import org.apache.reef.runtime.azbatch.util.AzureBatchFileNames;
import org.apache.reef.runtime.azbatch.util.AzureBatchHelper;
import org.apache.reef.runtime.azbatch.util.AzureStorageUtil;
import org.apache.reef.runtime.azbatch.util.CommandBuilder;
import org.apache.reef.runtime.azbatch.util.RemoteIdentifierParser;
import org.apache.reef.runtime.common.driver.api.ResourceLaunchEvent;
import org.apache.reef.runtime.common.driver.api.ResourceReleaseEvent;
import org.apache.reef.runtime.common.driver.api.ResourceRequestEvent;
import org.apache.reef.runtime.common.driver.evaluator.pojos.State;
import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorEventImpl;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceEventImpl;
import org.apache.reef.runtime.common.driver.resourcemanager.RuntimeStatusEventImpl;
import org.apache.reef.runtime.common.files.*;
import org.apache.reef.tang.Configuration;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.runtime.common.utils.RemoteManager;
import org.apache.reef.wake.remote.RemoteMessage;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.reef.runtime.azbatch.driver.RuntimeIdentifier.RUNTIME_NAME;

/**
 * The Driver's view of EvaluatorShims running in the cluster. The purpose of this class
 * is to start the evaluator shims and manage their life-cycle.
 */
@Private
@DriverSide
public final class AzureBatchEvaluatorShimManager
    implements EventHandler<RemoteMessage<EvaluatorShimProtocol.EvaluatorShimStatusProto>> {

  private static final Logger LOG = Logger.getLogger(AzureBatchEvaluatorShimManager.class.getName());

  private static final String AZ_BATCH_JOB_ID_ENV = "AZ_BATCH_JOB_ID";
  private static final int EVALUATOR_SHIM_MEMORY_MB = 64;

  private final Map<String, ResourceRequestEvent> outstandingResourceRequests;
  private final Map<String, String> activeResources;

  private final AutoCloseable evaluatorShimCommandChannel;

  private final AzureStorageUtil azureStorageUtil;
  private final REEFFileNames reefFileNames;
  private final AzureBatchFileNames azureBatchFileNames;
  private final RemoteManager remoteManager;
  private final AzureBatchHelper azureBatchHelper;
  private final AzureBatchEvaluatorShimConfigurationProvider evaluatorShimConfigurationProvider;
  
  private final JobJarMaker jobJarMaker;
  private final CommandBuilder launchCommandBuilder;

  private final REEFEventHandlers reefEventHandlers;

  @Inject
  AzureBatchEvaluatorShimManager(
      final AzureStorageUtil azureStorageUtil,
      final REEFFileNames reefFileNames,
      final AzureBatchFileNames azureBatchFileNames,
      final RemoteManager remoteManager,
      final REEFEventHandlers reefEventHandlers,
      final CommandBuilder launchCommandBuilder,
      final AzureBatchHelper azureBatchHelper,
      final JobJarMaker jobJarMaker,
      final AzureBatchEvaluatorShimConfigurationProvider evaluatorShimConfigurationProvider) {
    this.azureStorageUtil = azureStorageUtil;
    this.reefFileNames = reefFileNames;
    this.azureBatchFileNames = azureBatchFileNames;
    this.remoteManager = remoteManager;

    this.reefEventHandlers = reefEventHandlers;

    this.launchCommandBuilder = launchCommandBuilder;

    this.azureBatchHelper = azureBatchHelper;
    this.jobJarMaker = jobJarMaker;

    this.evaluatorShimConfigurationProvider = evaluatorShimConfigurationProvider;

    this.outstandingResourceRequests = new ConcurrentHashMap<>();
    this.activeResources = new ConcurrentHashMap<>();

    this.evaluatorShimCommandChannel = remoteManager
        .registerHandler(EvaluatorShimProtocol.EvaluatorShimStatusProto.class, this);
  }

  public void onResourceRequested(final String containerId, final ResourceRequestEvent resourceRequestEvent) {
    try {
      createAzureBatchTask(containerId);
      LOG.log(Level.FINER, "AzureBatchShimManager onResourceRequested created AzureBatch task");
      this.outstandingResourceRequests.put(containerId, resourceRequestEvent);
      this.updateRuntimeStatus();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onNext(final RemoteMessage<EvaluatorShimProtocol.EvaluatorShimStatusProto> statusMessage) {

    EvaluatorShimProtocol.EvaluatorShimStatusProto message = statusMessage.getMessage();
    String containerId = message.getContainerId();
    String remoteId = message.getRemoteIdentifier();

    LOG.log(Level.INFO, "Got a status message from evaluator shim = {0} with containerId = {1} and status = {2}.",
        new String[] {remoteId, containerId, message.getStatus().toString()});

    if (message.getStatus() != EvaluatorShimProtocol.EvaluatorShimStatus.ONLINE) {
      LOG.log(Level.SEVERE, "Unexpected status returned from the evaluator shim: {0}. Ignoring the message.",
          message.getStatus().toString());
      return;
    }

    synchronized (this.outstandingResourceRequests) {
      ResourceRequestEvent resourceRequestEvent = this.outstandingResourceRequests.get(containerId);

      if (resourceRequestEvent == null) {
        LOG.log(Level.WARNING, "Received an evaluator shim status message from an unknown "
            + "evaluator shim. Container id = {0}, remote id = {1}.", new String[] {containerId, remoteId});
      } else {
        LOG.log(Level.FINEST, "Notifying REEF of a new node: {0}", remoteId);
        this.reefEventHandlers.onNodeDescriptor(NodeDescriptorEventImpl.newBuilder()
            .setIdentifier(RemoteIdentifierParser.parseNodeId(remoteId))
            .setHostName(RemoteIdentifierParser.parseIp(remoteId))
            .setPort(RemoteIdentifierParser.parsePort(remoteId))
            .setMemorySize(resourceRequestEvent.getMemorySize().get())
            .build());

        LOG.log(Level.FINEST, "Firing a new ResourceAllocationEvent for remoteId = {0}.", remoteId);
        this.reefEventHandlers.onResourceAllocation(
            ResourceEventImpl.newAllocationBuilder()
                .setIdentifier(containerId)
                .setNodeId(RemoteIdentifierParser.parseNodeId(remoteId))
                .setResourceMemory(resourceRequestEvent.getMemorySize().get())
                .setVirtualCores(resourceRequestEvent.getVirtualCores().get())
                .setRuntimeName(RuntimeIdentifier.RUNTIME_NAME)
                .build());
      }

      this.outstandingResourceRequests.remove(containerId);
      this.activeResources.put(containerId, remoteId);
    }

    this.updateRuntimeStatus();
  }

  public void onResourceLaunched(final ResourceLaunchEvent resourceLaunchEvent,
                                 final String command,
                                 final String evaluatorConfigurationString) {

    String resourceRemoteId = this.activeResources.get(resourceLaunchEvent.getIdentifier());

    EventHandler<EvaluatorShimProtocol.EvaluatorShimControlProto> handler = this.remoteManager
        .getHandler(resourceRemoteId, EvaluatorShimProtocol.EvaluatorShimControlProto.class);

    LOG.log(Level.INFO, "Sending a command to the Evaluator shim to start the evaluator.");
    handler.onNext(
        EvaluatorShimProtocol.EvaluatorShimControlProto
            .newBuilder()
            .setCommand(EvaluatorShimProtocol.EvaluatorShimCommand.LAUNCH_EVALUATOR)
            .setEvaluatorLaunchCommand(command)
            .setEvaluatorConfigString(evaluatorConfigurationString)
            .build());
    this.updateRuntimeStatus();
  }

  public void onResourceReleased(final ResourceReleaseEvent resourceReleaseEvent) {

    synchronized (this.activeResources) {
      String remoteId = this.activeResources.get(resourceReleaseEvent.getIdentifier());

      if (!this.activeResources.containsKey(resourceReleaseEvent.getIdentifier())) {
        LOG.log(Level.WARNING, "Received a ResourceReleaseEvent for an unknown resource id = {0}.",
            resourceReleaseEvent.getIdentifier());
      } else {
        EventHandler<EvaluatorShimProtocol.EvaluatorShimControlProto> handler = this.remoteManager.getHandler(remoteId,
                EvaluatorShimProtocol.EvaluatorShimControlProto.class);

        LOG.log(Level.INFO, "Sending a TERMINATE command to the evaluator shim with remoteId = {0}.", remoteId);
        handler.onNext(
            EvaluatorShimProtocol.EvaluatorShimControlProto
                .newBuilder()
                .setCommand(EvaluatorShimProtocol.EvaluatorShimCommand.TERMINATE)
                .build());
      }

      this.activeResources.remove(resourceReleaseEvent.getIdentifier());
    }

    this.updateRuntimeStatus();
  }

  public void onClose() {
    try {
      this.evaluatorShimCommandChannel.close();
    } catch (Exception e) {
      LOG.log(Level.WARNING, "An unexpected exception while closing the Evaluator Shim Command channel: {0}", e);
      throw new RuntimeException(e);
    }
  }

  private void updateRuntimeStatus() {
    this.reefEventHandlers.onRuntimeStatus(RuntimeStatusEventImpl.newBuilder()
        .setName(RUNTIME_NAME)
        .setState(State.RUNNING)
        .setOutstandingContainerRequests(this.outstandingResourceRequests.size())
        .build());
  }

  private String getEvaluatorShimLaunchCommand() {
    return this.launchCommandBuilder.buildEvaluatorShimCommand(EVALUATOR_SHIM_MEMORY_MB,
        this.azureBatchFileNames.getEvaluatorShimConfigurationPath());
  }

  private FileResource getFileResourceFromFile(final File configFile, final FileType type) {
    return FileResourceImpl.newBuilder()
        .setName(configFile.getName())
        .setPath(configFile.getPath())
        .setType(type).build();
  }

  private void createAzureBatchTask(final String taskId) throws IOException {

    final File jarFile = writeShimJarFile(taskId);

    final String folderName = this.azureBatchFileNames.getStorageJobFolder() + this.getAzureBatchJobId();
    LOG.log(Level.FINE, "Creating a job folder on Azure at: {0}.", folderName);
    URI jobFolderURL = this.azureStorageUtil.createFolder(folderName);

    LOG.log(Level.FINE, "Uploading {0} to {0}.", new Object[] {folderName, jobFolderURL});
    final URI jarFileUri = this.azureStorageUtil.uploadFile(jobFolderURL, jarFile);

    final String command = getEvaluatorShimLaunchCommand();
    this.azureBatchHelper.submitTask(getAzureBatchJobId(), taskId, jarFileUri, command);
  }

  private File writeShimJarFile(final String azureBatchTaskId) {
    try {
      final Configuration shimConfig = this.evaluatorShimConfigurationProvider.getConfiguration(azureBatchTaskId);

      Set<FileResource> localFiles = new HashSet<>();
      Set<FileResource> globalFiles = new HashSet<>();

      final File globalFolder = new File(this.reefFileNames.getGlobalFolderPath());

      final File[] filesInGlobalFolder = globalFolder.listFiles();
      for (final File fileEntry : filesInGlobalFolder != null ? filesInGlobalFolder : new File[] {}) {
        globalFiles.add(getFileResourceFromFile(fileEntry, FileType.LIB));
      }

      return this.jobJarMaker.createJAR(
          shimConfig,
          globalFiles,
          localFiles,
          this.azureBatchFileNames.getEvaluatorShimConfigurationName());
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "Failed to build JAR file", ex);
      throw new RuntimeException(ex);
    }
  }

  private String getAzureBatchJobId() {
    return System.getenv(AZ_BATCH_JOB_ID_ENV);
  }
}
