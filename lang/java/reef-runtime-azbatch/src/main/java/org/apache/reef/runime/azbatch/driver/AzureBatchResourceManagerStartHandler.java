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
import org.apache.reef.runtime.common.driver.api.ResourceManagerStartHandler;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.wake.time.runtime.event.RuntimeStart;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handler of RuntimeStart for the Azure Batch Runtime.
 */
@Private
@DriverSide
public final class AzureBatchResourceManagerStartHandler implements ResourceManagerStartHandler {

  private static final Logger LOG = Logger.getLogger(AzureBatchResourceManagerStartHandler.class.getName());
  private final EventHandler<Alarm> taskStatusAlarmHandler;
  private final Clock clock;
  private final int taskStatusCheckPeriod;

  @Inject
  AzureBatchResourceManagerStartHandler(
      @Parameter(AzureBatchTaskStatusCheckPeriod.class) final int taskStatusCheckPeriod,
      final AzureBatchTaskStatusAlarmHandler taskStatusAlarmHandler,
      final Clock clock) {
    this.taskStatusCheckPeriod = taskStatusCheckPeriod;
    this.taskStatusAlarmHandler = taskStatusAlarmHandler;
    this.clock = clock;
  }

  @Override
  public void onNext(final RuntimeStart runtimeStart) {
    LOG.log(Level.FINE, "Azure batch runtime has been started...");
    this.scheduleAlarm();
  }

  public void scheduleAlarm() {
    this.clock.scheduleAlarm(this.taskStatusCheckPeriod, this.taskStatusAlarmHandler);
  }
}
