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

import com.microsoft.azure.batch.protocol.models.CloudTask;
import org.apache.reef.runtime.azbatch.parameters.AzureBatchTaskStatusCheckPeriod;
import org.apache.reef.runtime.azbatch.util.AzureBatchHelper;
import org.apache.reef.runtime.azbatch.util.TaskStatusMapper;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEventImpl;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.runtime.common.driver.evaluator.pojos.State;

import javax.inject.Inject;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class that gets that status of the tasks from Azure Batch for the job that is currently in progress
 * and notifies REEF of the status.
 */
final class AzureBatchTaskStatusAlarmHandler implements EventHandler<Alarm> {

  private final AzureBatchHelper azureBatchHelper;
  private final InjectionFuture<AzureBatchResourceManagerStartHandler> azureBatchResourceManagerStartHandler;
  private final InjectionFuture<AzureBatchResourceManager> azureBatchResourceManager;
  private final InjectionFuture<REEFEventHandlers> reefEventHandlers;
  private final int taskStatusCheckPeriod;
  private boolean isAlarmScheduled;
  private Clock clock;

  private static final Logger LOG = Logger.getLogger(AzureBatchTaskStatusAlarmHandler.class.getName());

  @Inject
  private AzureBatchTaskStatusAlarmHandler(
      final InjectionFuture<REEFEventHandlers> reefEventHandlers,
      final AzureBatchHelper azureBatchHelper,
      final InjectionFuture<AzureBatchResourceManager> azureBatchResourceManager,
      final InjectionFuture<AzureBatchResourceManagerStartHandler> azureBatchResourceManagerStartHandler,
      @Parameter(AzureBatchTaskStatusCheckPeriod.class) final int taskStatusCheckPeriod,
      final Clock clock) {
    this.reefEventHandlers = reefEventHandlers;
    this.azureBatchHelper = azureBatchHelper;
    this.azureBatchResourceManager = azureBatchResourceManager;
    this.azureBatchResourceManagerStartHandler = azureBatchResourceManagerStartHandler;
    this.clock = clock;
    this.taskStatusCheckPeriod = taskStatusCheckPeriod;
  }

  @Override
  public void onNext(final Alarm alarm) {
    String jobId = this.azureBatchHelper.getAzureBatchJobId();
    List<CloudTask> allTasks = this.azureBatchHelper.getTaskStatusForJob(jobId);

    if (this.isAlarmScheduled()) {
      this.scheduleAlarm();
    }

    // Report status if the task has an associated active container.
    LOG.log(Level.FINER, "Found {0} tasks from job id {1}", new Object[]{allTasks.size(), jobId});
    for (CloudTask task : allTasks) {
      State reefTaskState = TaskStatusMapper.getReefTaskState(task);
      LOG.log(Level.FINEST, "status for Task Id: {0} is [Azure Batch Status]:{1}, [REEF status]:{2}",
          new Object[]{task.id(), task.state().toString(), reefTaskState});
      if (this.azureBatchResourceManager.get().isContainerActive(task.id())) {
        ResourceStatusEvent resourceStatusEvent = ResourceStatusEventImpl.newBuilder()
            .setIdentifier(task.id())
            .setState(reefTaskState)
            .build();
        LOG.log(Level.FINEST, "Reporting status for Task Id: {0} is [Azure Batch Status]:{1}, [REEF status]:{2}",
            new Object[]{task.id(), task.state().toString(), reefTaskState});
        this.reefEventHandlers.get().onResourceStatus(resourceStatusEvent);
      } else {
        LOG.log(Level.FINEST,
            "No active container in resource manager for Task: {0}. Not sending update.", task.id());
      }
    }
  }

  /**
   * Enable the period alarm to send status updates.
   */
  public synchronized void enableAlarm() {
    this.isAlarmScheduled = true;
    this.scheduleAlarm();
  }

  /**
   * Disable the period alarm to send status updates.
   */
  public synchronized void disableAlarm() {
    this.isAlarmScheduled = false;
  }

  private synchronized boolean isAlarmScheduled() {
    return this.isAlarmScheduled;
  }

  private void scheduleAlarm() {
    this.clock.scheduleAlarm(this.taskStatusCheckPeriod, this);
  }
}
