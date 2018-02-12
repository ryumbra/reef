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

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runime.azbatch.util.CommandBuilder;
import org.apache.reef.runtime.common.driver.api.ResourceLaunchEvent;
import org.apache.reef.runtime.common.driver.api.ResourceReleaseEvent;
import org.apache.reef.runtime.common.driver.api.ResourceRequestEvent;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A resource manager that uses threads to execute containerRequests.
 */
@Private
@DriverSide
public final class AzureBatchResourceManager {

  private static final Logger LOG = Logger.getLogger(AzureBatchResourceManager.class.getName());

  private final Map<String, ResourceRequestEvent> containerRequests;
  private final Set<String> activeContainerIds;
  private final ConfigurationSerializer configurationSerializer;
  private final AzureBatchEvaluatorShimManager evaluatorShimManager;
  private final AzureBatchTaskStatusAlarmHandler azureBatchTaskStatusAlarmHandler;

  private final double jvmHeapFactor;
  private final CommandBuilder launchCommandBuilder;
  private final AtomicInteger containerCount;

  @Inject
  AzureBatchResourceManager(
      final ConfigurationSerializer configurationSerializer,
      final CommandBuilder launchCommandBuilder,
      final AzureBatchEvaluatorShimManager evaluatorShimManager,
      final AzureBatchTaskStatusAlarmHandler azureBatchTaskStatusAlarmHandler,
      @Parameter(JVMHeapSlack.class) final double jvmHeapSlack) {
    this.configurationSerializer = configurationSerializer;
    this.evaluatorShimManager = evaluatorShimManager;
    this.jvmHeapFactor = 1.0 - jvmHeapSlack;
    this.launchCommandBuilder = launchCommandBuilder;
    this.containerRequests = new ConcurrentHashMap<>();
    this.activeContainerIds = Collections.synchronizedSet(new HashSet<String>());
    this.containerCount = new AtomicInteger(0);
    this.azureBatchTaskStatusAlarmHandler = azureBatchTaskStatusAlarmHandler;
  }

  public void onResourceRequested(final ResourceRequestEvent resourceRequestEvent) {
    LOG.log(Level.FINEST, "Got ResourceRequestEvent in AzureBatchResourceManager,");
    for (int r = 0; r < resourceRequestEvent.getResourceCount(); r++) {
      final String containerId = generateContainerId();
      this.containerRequests.put(containerId, resourceRequestEvent);
      this.containerCount.incrementAndGet();
      this.azureBatchTaskStatusAlarmHandler.enableAlarm();
      this.evaluatorShimManager.onResourceRequested(containerId, resourceRequestEvent);
    }
  }

  public void onResourceReleased(final ResourceReleaseEvent resourceReleaseEvent) {
    String id = resourceReleaseEvent.getIdentifier();
    LOG.log(Level.FINEST, "Got ResourceReleasedEvent for Id: {0} in AzureBatchResourceManager", id);
    if (!this.activeContainerIds.remove(id)) {
      LOG.log(Level.WARNING,
          "Attempting to remove non-existent activeContainer for Id: {0} in AzureBatchResourceManager", id);
    }

    ResourceRequestEvent removedEvent = this.containerRequests.remove(id);
    if (removedEvent == null) {
      LOG.log(Level.WARNING,
          "Ignoring attempt to remove non-existent containerRequest for Id: {0} in AzureBatchResourceManager", id);
    } else {
      int currentContainerCount = this.containerCount.decrementAndGet();
      if (currentContainerCount <= 0) {
        this.azureBatchTaskStatusAlarmHandler.disableAlarm();
      }
    }

    this.evaluatorShimManager.onResourceReleased(resourceReleaseEvent);
  }

  public void onResourceLaunched(final ResourceLaunchEvent resourceLaunchEvent) {
    String id = resourceLaunchEvent.getIdentifier();
    LOG.log(Level.FINEST, "Got ResourceLaunchEvent for Id: {0} in AzureBatchResourceManager", id);
    final int evaluatorMemory = this.containerRequests.get(id).getMemorySize().get();
    String launchCommand = this.launchCommandBuilder.buildEvaluatorCommand(resourceLaunchEvent,
        evaluatorMemory, this.jvmHeapFactor);
    String evaluatorConfigurationString = this.configurationSerializer.toString(resourceLaunchEvent.getEvaluatorConf());
    this.activeContainerIds.add(id);
    // TODO[At this point we are only sending the evaluator config and launch command to the shim. AllocatedEvaluator]
    // TODO[object also has "addFile" and "addLibrary" methods, so maybe we'll need to package the JAR for the]
    // TODO[evaluator, too, and not assume that both Evaluator and Shim can share the same JAR.]
    this.evaluatorShimManager.onResourceLaunched(resourceLaunchEvent, launchCommand, evaluatorConfigurationString);
  }

  public Boolean isContainerActive(final String containerId) {
    return this.activeContainerIds.contains(containerId);
  }

  private String generateContainerId() {
    return UUID.randomUUID().toString();
  }
}
