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

import org.apache.commons.lang.StringUtils;
import org.apache.reef.runtime.azbatch.evaluator.EvaluatorShimLauncher;
import org.apache.reef.runtime.common.REEFLauncher;
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.files.RuntimePathProvider;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Build the launch command for Java REEF processes for Azure Batch Windows pools.
 */
public class WindowsCommandBuilder extends AbstractCommandBuilder {

  private static final Class LAUNCHER_CLASS = REEFLauncher.class;
  private static final Class SHIM_LAUNCHER_CLASS = EvaluatorShimLauncher.class;
  private static final List<String> COMMAND_LIST_PREFIX = Collections.unmodifiableList(
      Arrays.asList(
          "Add-Type -AssemblyName System.IO.Compression.FileSystem; ",
          "[System.IO.Compression.ZipFile]::ExtractToDirectory(\\\"$env:AZ_BATCH_TASK_WORKING_DIR\\" +
              AzureBatchFileNames.TASK_JAR_FILE_NAME + "\\\", " +
              "\\\"$env:AZ_BATCH_TASK_WORKING_DIR\\reef\\\"); ")
  );
  private static final char CLASSPATH_SEPARATOR_CHAR = ';';
  private static final String OS_COMMAND_FORMAT = "powershell.exe /c \"%s\";";

  @Inject
  WindowsCommandBuilder(
      final ClasspathProvider classpathProvider,
      final RuntimePathProvider runtimePathProvider,
      final REEFFileNames reefFileNames) {
    super(LAUNCHER_CLASS, SHIM_LAUNCHER_CLASS, COMMAND_LIST_PREFIX, OS_COMMAND_FORMAT,
        classpathProvider, runtimePathProvider, reefFileNames);
  }

  @Override
  protected String getDriverClasspath() {
    return String.format("'%s'", StringUtils.join(
        super.classpathProvider.getDriverClasspath(), CLASSPATH_SEPARATOR_CHAR));
  }

  @Override
  protected String getEvaluatorShimClasspath() {
    return String.format("'%s'", StringUtils.join(
        super.classpathProvider.getEvaluatorClasspath(), CLASSPATH_SEPARATOR_CHAR));
  }
}
