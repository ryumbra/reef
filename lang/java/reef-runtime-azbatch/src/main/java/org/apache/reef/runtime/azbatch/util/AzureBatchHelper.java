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
package org.apache.reef.runtime.azbatch.util;

import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.auth.BatchSharedKeyCredentials;
import com.microsoft.azure.batch.protocol.models.*;

import org.apache.reef.runtime.azbatch.client.AzureBatchJobSubmissionHandler;
import org.apache.reef.runtime.azbatch.parameters.AzureBatchAccountKey;
import org.apache.reef.runtime.azbatch.parameters.AzureBatchAccountName;
import org.apache.reef.runtime.azbatch.parameters.AzureBatchAccountUri;
import org.apache.reef.runtime.azbatch.parameters.AzureBatchPoolId;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A {@link AzureBatchHelper} for Azure Batch.
 */
public class AzureBatchHelper {

  private static final Logger LOG = Logger.getLogger(AzureBatchJobSubmissionHandler.class.getName());

  private final AzureBatchFileNames azureBatchFileNames;

  /*
   * Environment variable that contains the Azure Batch jobId.
   */
  private static final String AZ_BATCH_JOB_ID_ENV = "AZ_BATCH_JOB_ID";

  private final String azureBatchAccountUri;
  private final String azureBatchAccountName;
  private final String azureBatchAccountKey;
  private final String azureBatchPoolId;
  private final BatchClient client;
  private final PoolInformation poolInfo;

  @Inject
  public AzureBatchHelper(
      final AzureBatchFileNames azureBatchFileNames,
      @Parameter(AzureBatchAccountUri.class) final String azureBatchAccountUri,
      @Parameter(AzureBatchAccountName.class) final String azureBatchAccountName,
      @Parameter(AzureBatchAccountKey.class) final String azureBatchAccountKey,
      @Parameter(AzureBatchPoolId.class) final String azureBatchPoolId) {
    this.azureBatchFileNames = azureBatchFileNames;
    this.azureBatchAccountUri = azureBatchAccountUri;
    this.azureBatchAccountName = azureBatchAccountName;
    this.azureBatchAccountKey = azureBatchAccountKey;
    this.azureBatchPoolId = azureBatchPoolId;

    BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(
        this.azureBatchAccountUri, this.azureBatchAccountName, this.azureBatchAccountKey);
    this.client = BatchClient.open(cred);

    this.poolInfo = new PoolInformation();
    poolInfo.withPoolId(this.azureBatchPoolId);
  }

  public void submitJob(final String applicationId, final URI jobJarSasUri, final String command) throws IOException {
    ResourceFile jarResourceFile = new ResourceFile()
        .withBlobSource(jobJarSasUri.toString())
        .withFilePath(this.azureBatchFileNames.getTaskJarFileName());

    JobManagerTask jobManagerTask = new JobManagerTask()
        .withRunExclusive(false)
        .withId(applicationId)
        .withResourceFiles(Collections.singletonList(jarResourceFile))
        .withCommandLine(command);

    LOG.log(Level.INFO, "Job Manager (aka driver) task command: " + command);

    JobAddParameter jobAddParameter = new JobAddParameter()
        .withId(applicationId)
        .withJobManagerTask(jobManagerTask)
        .withPoolInfo(poolInfo);

    client.jobOperations().createJob(jobAddParameter);
  }

  public void submitTask(final String jobId, final String taskId, final URI jobJarSasUri, final String command)
      throws IOException {

    final ResourceFile jarSourceFile = new ResourceFile()
        .withBlobSource(jobJarSasUri.toString())
        .withFilePath(this.azureBatchFileNames.getTaskJarFileName());

    final List<ResourceFile> resources = new ArrayList<>();
    resources.add(jarSourceFile);

    LOG.log(Level.INFO, "Evaluator task command: " + command);

    final TaskAddParameter taskAddParameter = new TaskAddParameter()
        .withId(taskId)
        .withResourceFiles(resources)
        .withCommandLine(command);

    this.client.taskOperations().createTask(jobId, taskAddParameter);
  }

  public List<CloudTask> getTaskStatusForJob(final String jobId) {
    List<CloudTask> tasks = null;
    try {
      tasks = client.taskOperations().listTasks(jobId);
      LOG.log(Level.INFO, "Task status for job: {0} returned {1} tasks", new Object[]{jobId, tasks.size()});
    } catch (IOException | BatchErrorException ex) {
      LOG.log(Level.SEVERE, "Exception when fetching Task status for job: {0}. Exception [{1}]:[2]",
          new Object[] {jobId, ex.getMessage(), ex.getStackTrace()});
    }

    return tasks;
  }

  public String getAzureBatchJobId() {
    return System.getenv(AZ_BATCH_JOB_ID_ENV);
  }
}
