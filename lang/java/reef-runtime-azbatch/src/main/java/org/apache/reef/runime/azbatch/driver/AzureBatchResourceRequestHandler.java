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
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.common.driver.api.ResourceRequestEvent;
import org.apache.reef.runtime.common.driver.api.ResourceRequestHandler;
import org.apache.reef.runtime.common.driver.evaluator.pojos.State;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceEventImpl;
import org.apache.reef.runtime.common.driver.resourcemanager.RuntimeStatusEventImpl;
import org.apache.reef.runtime.yarn.driver.REEFEventHandlers;
import org.apache.reef.runtime.yarn.driver.RuntimeIdentifier;

import javax.inject.Inject;

/**
 * A {@link ResourceRequestHandler} for Azure Batch.
 */
@Private
public class AzureBatchResourceRequestHandler implements ResourceRequestHandler {

    private final REEFEventHandlers reefEventHandlers;

  @Inject
  AzureBatchResourceRequestHandler(final REEFEventHandlers reefEventHandlers) {
      this.reefEventHandlers = reefEventHandlers;
  }

  @Override
  public void onNext(final ResourceRequestEvent resourceRequestEvent) {
    // do nothing as Azure batch pool is already provisioned. So nothing left to do here.

      this.reefEventHandlers.onResourceAllocation(ResourceEventImpl.newAllocationBuilder()
              .setIdentifier("id")
              .setNodeId("nodeId")
              .setResourceMemory(resourceRequestEvent.getMemorySize().get())
              .setVirtualCores(resourceRequestEvent.getVirtualCores().get())
              .setRackName("niceRack")
              .setRuntimeName(RuntimeIdentifier.RUNTIME_NAME)
              .build());

      this.updateRuntimeStatus(resourceRequestEvent);

  }

    private void updateRuntimeStatus(final ResourceRequestEvent resourceRequestEvent) {

        final RuntimeStatusEventImpl.Builder builder = RuntimeStatusEventImpl.newBuilder()
                .setName(RuntimeIdentifier.RUNTIME_NAME)
                .setState(State.RUNNING)
                .setOutstandingContainerRequests(resourceRequestEvent.getResourceCount());

        builder.addContainerAllocation(resourceRequestEvent.toString());

        this.reefEventHandlers.onRuntimeStatus(builder.build());
    }

}
