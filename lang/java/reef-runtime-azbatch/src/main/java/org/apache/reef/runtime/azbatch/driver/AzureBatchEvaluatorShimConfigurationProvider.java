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

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.azbatch.evaluator.EvaluatorShimConfiguration;
import org.apache.reef.runtime.common.utils.RemoteManager;
import org.apache.reef.tang.Configuration;

import javax.inject.Inject;

/**
 * Configuration provider for the Azure Batch Evaluator Shim.
 */
@Private
public class AzureBatchEvaluatorShimConfigurationProvider {

  private final RemoteManager remoteManager;

  @Inject
  AzureBatchEvaluatorShimConfigurationProvider(
      final RemoteManager remoteManager) {
    this.remoteManager = remoteManager;
  }

  public Configuration getConfiguration(final String azureBatchTaskId) {
    return EvaluatorShimConfiguration.CONF
        .set(EvaluatorShimConfiguration.DRIVER_REMOTE_IDENTIFIER, this.remoteManager.getMyIdentifier())
        .set(EvaluatorShimConfiguration.CONTAINER_IDENTIFIER, azureBatchTaskId)
        .build();
  }
}
