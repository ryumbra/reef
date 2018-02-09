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

import com.microsoft.azure.batch.protocol.models.CloudTask;
import org.apache.reef.runime.azbatch.util.AzureBatchHelper;
import org.apache.reef.runime.azbatch.util.TaskStatusMapper;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEventImpl;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.runtime.common.driver.evaluator.pojos.State;

import javax.inject.Inject;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Class that gets that status of the tasks from Azure Batch for the job that is currently in progress
 * and notifies REEF of the status.
 */
final class TaskStatusAlarmHandler implements EventHandler<Alarm>, AutoCloseable {

  private final AzureBatchHelper azureBatchHelper;
  private final InjectionFuture<AzureBatchResourceManager> azureBatchResourceManager;
  private final InjectionFuture<REEFEventHandlers> reefEventHandlers;

  private static final Logger LOG = Logger.getLogger(AzureBatchResourceLaunchHandler.class.getName());

  @Override
  public void onNext(final Alarm alarm) {
    String jobId = this.azureBatchHelper.getAzureBatchJobId();
    List<CloudTask> allTasks = this.azureBatchHelper.getTaskStatusForJob(jobId);

    // Reschedule alarm again if there are container requests.
    if (this.azureBatchResourceManager.get().containerRequestCount() > 0) {
      LOG.log(Level.FINEST, "Reschedule timer since there are container requests", jobId);
      this.reefEventHandlers.get().scheduleAlarm();
    } else {
      LOG.log(Level.INFO, "Not rescheduling timer since there are no container requests", jobId);
    }

    // Report status if the task has an associated active container.
    LOG.log(Level.FINER, "Found {0} tasks from job id {1}", new Object[]{allTasks.size(), jobId});
    LOG.log(Level.FINEST, "active container list: {0}",
        this.azureBatchResourceManager.get().activeContainerList());
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

  @Inject
  private TaskStatusAlarmHandler(
      final InjectionFuture<REEFEventHandlers> reefEventHandlers,
      final AzureBatchHelper azureBatchHelper,
      final InjectionFuture<AzureBatchResourceManager> azureBatchResourceManager) {
    this.reefEventHandlers = reefEventHandlers;
    this.azureBatchHelper = azureBatchHelper;
    this.azureBatchResourceManager = azureBatchResourceManager;
  }

  @Override
  public void close() throws Exception {
    this.azureBatchHelper.close();
    this.reefEventHandlers.get().close();
  }
}
