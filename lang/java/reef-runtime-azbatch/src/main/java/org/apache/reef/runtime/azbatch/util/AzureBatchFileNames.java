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

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.common.files.REEFFileNames;

import javax.inject.Inject;

/**
 * The Azure Batch task JAR name.
 */
@Private
public final class AzureBatchFileNames {

  private static final String STORAGE_JOB_FOLDER_PATH = "apps/reef/jobs/";
  private static final String TASK_JAR_FILE_NAME = "local.jar";
  private static final String EVALUATOR_RESOURCE_FILES_JAR_NAME = "resources.jar";

  private static final String EVALUATOR_SHIM_CONFIGURATION_NAME = "shim.conf";
  private static final String TEXTFILE_EXTENSION = ".txt";

  private final REEFFileNames reefFileNames;

  @Inject
  private AzureBatchFileNames(final REEFFileNames reefFileNames) {
    this.reefFileNames = reefFileNames;
  }

  public String getStorageJobFolder(final String jobId) {
    return STORAGE_JOB_FOLDER_PATH + jobId;
  }

  public String getEvaluatorStdErrFilename() {
    return this.reefFileNames.getEvaluatorStderrFileName() + TEXTFILE_EXTENSION;
  }

  public String getEvaluatorStdOutFilename() {
    return this.reefFileNames.getEvaluatorStdoutFileName() + TEXTFILE_EXTENSION;
  }

  public String getEvaluatorShimConfigurationPath() {
    return this.reefFileNames.getLocalFolderPath() + "/" + EVALUATOR_SHIM_CONFIGURATION_NAME;
  }

  public String getEvaluatorResourceFilesJarName() {
    return EVALUATOR_RESOURCE_FILES_JAR_NAME;
  }

  public String getEvaluatorShimConfigurationName() {
    return EVALUATOR_SHIM_CONFIGURATION_NAME;
  }

  public static String getTaskJarFileName() {
    return TASK_JAR_FILE_NAME;
  }

}

