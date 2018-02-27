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
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.azbatch.parameters.AzureBatchTaskStatusCheckPeriod;
import org.apache.reef.runtime.azbatch.util.AzureBatchHelper;
import org.apache.reef.runtime.azbatch.util.TaskStatusMapper;
import org.apache.reef.runtime.common.driver.evaluator.EvaluatorManager;
import org.apache.reef.runtime.common.driver.evaluator.Evaluators;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEventImpl;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
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
@Private
@DriverSide
final class AzureBatchTaskStatusAlarmHandler implements EventHandler<Alarm> {

  private final AzureBatchHelper azureBatchHelper;
  private final InjectionFuture<REEFEventHandlers> reefEventHandlers;
  private final int taskStatusCheckPeriod;
  private boolean isAlarmEnabled;
  private final Evaluators evaluators;
  private final Clock clock;

  private static final Logger LOG = Logger.getLogger(AzureBatchTaskStatusAlarmHandler.class.getName());

  @Inject
  private AzureBatchTaskStatusAlarmHandler(
      final InjectionFuture<REEFEventHandlers> reefEventHandlers,
      final AzureBatchHelper azureBatchHelper,
      final Evaluators evaluators,
      final Clock clock,
      @Parameter(AzureBatchTaskStatusCheckPeriod.class) final int taskStatusCheckPeriod) {
    this.reefEventHandlers = reefEventHandlers;
    this.azureBatchHelper = azureBatchHelper;
    this.evaluators = evaluators;
    this.clock = clock;
    this.taskStatusCheckPeriod = taskStatusCheckPeriod;
  }

  @Override
  public void onNext(final Alarm alarm) {
    String jobId = this.azureBatchHelper.getAzureBatchJobId();
    List<CloudTask> allTasks = this.azureBatchHelper.getTaskStatusForJob(jobId);

    if (this.isAlarmEnabled()) {
      this.scheduleAlarm();
    }

    // Report status if the task has an associated active container.
    LOG.log(Level.FINER, "Found {0} tasks from job id {1}", new Object[]{allTasks.size(), jobId});
    for (CloudTask task : allTasks) {
      State reefTaskState = TaskStatusMapper.getReefTaskState(task);
      LOG.log(Level.FINEST, "status for Task Id: {0} is [Azure Batch Status]:{1}, [REEF status]:{2}",
          new Object[]{task.id(), task.state().toString(), reefTaskState});

      Optional<EvaluatorManager> optionalEvaluatorManager = this.evaluators.get(task.id());
      if (!optionalEvaluatorManager.isPresent()) {
        LOG.log(Level.FINE, "No Evaluator found for Azure Batch task id = {0}.", task.id());
      } else if (optionalEvaluatorManager.get().isClosedOrClosing()) {
        LOG.log(Level.FINE, "Evaluator id = {0} is closed.", task.id());
      } else {
        LOG.log(Level.FINE, "Reporting status for Task Id: {0} is [Azure Batch Status]:{1}, [REEF status]:{2}",
            new Object[]{task.id(), task.state().toString(), reefTaskState});
        ResourceStatusEvent resourceStatusEvent = ResourceStatusEventImpl.newBuilder()
            .setIdentifier(task.id())
            .setState(reefTaskState)
            .build();
        this.reefEventHandlers.get().onResourceStatus(resourceStatusEvent);
      }
    }
  }

  /**
   * Enable the period alarm to send status updates.
   */
  public synchronized void enableAlarm() {
    if (!this.isAlarmEnabled) {
      this.isAlarmEnabled = true;
      this.scheduleAlarm();
    } else {
      LOG.log(Level.FINE, "Alarm is already scheduled.");
    }
  }

  /**
   * Disable the period alarm to send status updates.
   */
  public synchronized void disableAlarm() {
    this.isAlarmEnabled = false;
  }

  private synchronized boolean isAlarmEnabled() {
    return this.isAlarmEnabled;
  }

  private void scheduleAlarm() {
    this.clock.scheduleAlarm(this.taskStatusCheckPeriod, this);
  }
}
