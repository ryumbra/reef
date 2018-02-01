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
package org.apache.reef.runime.azbatch.evaluator;

import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.proto.EvaluatorShimProtocol;
import org.apache.reef.runime.azbatch.parameters.ContainerIdentifier;
import org.apache.reef.runtime.common.evaluator.parameters.DriverRemoteIdentifier;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.utils.RemoteManager;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.RemoteMessage;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Evaluator shim class.
 * This class sends a message back to the driver upon starting and listens for
 * commands to launch an evaluator. Once the evaluator completes, it will listen
 * for a command to terminate.
 */
@Private
@EvaluatorSide
public final class EvaluatorShim
    implements EventHandler<RemoteMessage<EvaluatorShimProtocol.EvaluatorShimControlProto>> {
  private static final Logger LOG = Logger.getLogger(EvaluatorShim.class.getName());

  private final RemoteManager remoteManager;
  private final REEFFileNames fileNames;
  private final ConfigurationSerializer configurationSerializer;

  private final String driverRemoteId;
  private final String containerId;

  private final EventHandler<EvaluatorShimProtocol.EvaluatorShimStatusProto> evaluatorShimStatusChannel;
  private final AutoCloseable evaluatorShimCommandChannel;

  private final ExecutorService evaluatorLaunchExecutorService;

  private Process evaluatorProcess;
  private Integer evaluatorProcessExitValue;

  @Inject
  EvaluatorShim(final REEFFileNames fileNames,
                final ConfigurationSerializer configurationSerializer,
                final RemoteManager remoteManager,
                @Parameter(DriverRemoteIdentifier.class)
                final String driverRemoteId,
                @Parameter(ContainerIdentifier.class)
                final String containerId) {
    this.fileNames = fileNames;
    this.configurationSerializer = configurationSerializer;

    this.driverRemoteId = driverRemoteId;
    this.containerId = containerId;

    this.remoteManager = remoteManager;
    this.evaluatorShimStatusChannel = this.remoteManager.getHandler(this.driverRemoteId,
        EvaluatorShimProtocol.EvaluatorShimStatusProto.class);

    this.evaluatorShimCommandChannel = this.remoteManager
        .registerHandler(EvaluatorShimProtocol.EvaluatorShimControlProto.class, this);

    this.evaluatorLaunchExecutorService = Executors.newSingleThreadExecutor();
  }

  public void run() {
    LOG.log(Level.FINEST, "Entering EvaluatorShim.run().");
    this.onStart();
  }

  public void stop() {
    this.onStop();
  }

  @Override
  public void onNext(final RemoteMessage<EvaluatorShimProtocol.EvaluatorShimControlProto> remoteMessage) {
    final EvaluatorShimProtocol.EvaluatorShimCommand command = remoteMessage.getMessage().getCommand();
    if (command == EvaluatorShimProtocol.EvaluatorShimCommand.EVALUATOR_LAUNCH) {
      LOG.log(Level.INFO, "Received a command to launch the Evaluator.");
      EvaluatorShim.this.onEvaluatorLaunch(remoteMessage.getMessage().getEvaluatorLaunchCommand(),
          remoteMessage.getMessage().getEvaluatorConfigString());
    } else if (command == EvaluatorShimProtocol.EvaluatorShimCommand.TERMINATE) {
      LOG.log(Level.INFO, "Received a command to terminate.");
      EvaluatorShim.this.onStop();
    } else {
      LOG.log(Level.WARNING, "An unknown command was received by the EvaluatorShim: {0}.",
          command);
      throw new IllegalArgumentException("An unknown command was received by the EvaluatorShim.");
    }
  }

  private void onStart() {
    LOG.log(Level.FINEST, "Entering EvaluatorShim.onStart().");

    LOG.log(Level.INFO, "Reporting back to the driver with Shim Status = {0}",
        EvaluatorShimProtocol.EvaluatorShimStatus.ONLINE);
    this.evaluatorShimStatusChannel.onNext(
        EvaluatorShimProtocol.EvaluatorShimStatusProto
            .newBuilder()
            .setRemoteIdentifier(this.remoteManager.getMyIdentifier())
            .setContainerId(this.containerId)
            .setStatus(EvaluatorShimProtocol.EvaluatorShimStatus.ONLINE)
            .build());
  }

  private void onStop() {
    LOG.log(Level.FINER, "Entering EvaluatorShim.onStop().");

    if (this.evaluatorProcess != null) {
      try {
        LOG.log(Level.INFO, "Waiting for the evaluator process to exit.");
        this.evaluatorProcess.waitFor();

        LOG.log(Level.INFO, "Destroying the Evaluator Process.");
        this.evaluatorProcess.destroy();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    LOG.log(Level.INFO, "Shutting down the evaluatorLaunchExecutorService.");
    this.evaluatorLaunchExecutorService.shutdown();

    try {
      LOG.log(Level.INFO, "Closing EvaluatorShim Control channel.");
      this.evaluatorShimCommandChannel.close();
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "An unexpected exception occurred while attempting to close the EvaluatorShim " +
          "control channel.");
      throw new RuntimeException(e);
    }

    try {
      LOG.log(Level.INFO, "Closing the Remote Manager.");
      this.remoteManager.close();
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to close the RemoteManager with the following exception: {0}.", e);
    }

    LOG.log(Level.INFO, "Exiting EvaluatorShim.onStop().");
  }

  private void onEvaluatorLaunch(final String launchCommand, final String evaluatorConfigString) {
    LOG.log(Level.FINEST, "Entering EvaluatorShim.onEvaluatorLaunch().");

    File evaluatorConfigurationFile = new File("local/evaluator.conf");
    LOG.log(Level.FINER, "Persisting evaluator config at: {0}", evaluatorConfigurationFile.getAbsolutePath());

    try {
      boolean newFileCreated = evaluatorConfigurationFile.createNewFile();
      LOG.log(Level.FINE,
          newFileCreated ? "Created a new file for persisting evaluator configuration at {0}."
              : "Using existing file for persisting evaluator configuration at {0}.",
          evaluatorConfigurationFile.getAbsolutePath());

      Configuration evaluatorConfiguration = this.configurationSerializer.fromString(evaluatorConfigString);
      this.configurationSerializer.toFile(evaluatorConfiguration, evaluatorConfigurationFile);
    } catch (final IOException | BindException e) {
      LOG.log(Level.SEVERE, "An unexpected exception occurred while attempting to deserialize and write " +
          "Evaluator configuration file. {0}", e);
      throw new RuntimeException("Unable to write configuration.", e);
    }

    LOG.log(Level.INFO, "Launching the evaluator by invoking the following command: " + launchCommand);

    this.evaluatorLaunchExecutorService.submit(new Thread() {
      public void run() {
        try {
          final List<String> command = Arrays.asList(launchCommand.split(" "));
          EvaluatorShim.this.evaluatorProcess = new ProcessBuilder()
              .command(command)
              .redirectError(new File(fileNames.getEvaluatorStderrFileName() + ".txt"))
              .redirectOutput(new File(fileNames.getEvaluatorStdoutFileName() + ".txt"))
              .start();

          EvaluatorShim.this.evaluatorProcessExitValue = EvaluatorShim.this.evaluatorProcess.waitFor();
          LOG.log(Level.INFO, "Evaluator process completed with exit value: {0}.",
              EvaluatorShim.this.evaluatorProcessExitValue);
        } catch (IOException | InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }
}
