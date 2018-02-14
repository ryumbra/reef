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
package org.apache.reef.examples.hello;

import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Driver code for the Hello REEF Application.
 */
@Unit
public final class HelloDriver {

  private static final Logger LOG = Logger.getLogger(HelloDriver.class.getName());

  private final EvaluatorRequestor requestor;

  /**
   * Job driver constructor - instantiated via TANG.
   *
   * @param requestor evaluator requestor object used to create new evaluator containers.
   */
  @Inject
  private HelloDriver(final EvaluatorRequestor requestor) {
    this.requestor = requestor;
    LOG.log(Level.FINE, "Instantiated 'HelloDriver'");
  }

  /**
   * Handles the StartTime event: Request as single Evaluator.
   */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "HelloDriver has been started. Requesting an evaluator.");
      HelloDriver.this.requestor.newRequest()
          .setNumber(1)
          .setMemory(64)
          .setNumberOfCores(1)
          .submit();
      LOG.log(Level.INFO, "Requested Evaluator.");
    }
  }

  /**
   * Handles AllocatedEvaluator: Submit the HelloTask.
   */
  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Submitting HelloREEF task to AllocatedEvaluator: {0}", allocatedEvaluator);
      final Configuration taskConfiguration = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, "HelloREEFTask")
          .set(TaskConfiguration.TASK, HelloTask.class)
          .build();

      try {
        allocatedEvaluator.addFile(createTempFile());
        allocatedEvaluator.addLibrary(createTempFile());
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to write evaluator resource file.");
        throw new RuntimeException(e);
      }

      allocatedEvaluator.submitTask(taskConfiguration);
    }
  }

  private File createTempFile() throws IOException {
    File outFile = File.createTempFile("driver-test-file", ".txt");
    try (OutputStreamWriter fileWriter = new OutputStreamWriter(new FileOutputStream(outFile), "UTF-8")) {
      fileWriter.write("Hello world from the driver...");
    }

    return outFile;
  }
}
