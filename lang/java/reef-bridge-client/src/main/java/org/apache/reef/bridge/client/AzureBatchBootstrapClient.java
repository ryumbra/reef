package org.apache.reef.bridge.client;

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

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.reef.annotations.audience.Interop;
import org.apache.reef.reef.bridge.client.avro.AvroAzureBatchJobSubmissionParameters;
import org.apache.reef.reef.bridge.client.avro.AvroLocalJobSubmissionParameters;
import org.apache.reef.runtime.azbatch.client.AzureBatchRuntimeConfigurationProvider;
import org.apache.reef.runtime.azbatch.parameters.*;
import org.apache.reef.runtime.common.REEFLauncher;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is a bootstrap launcher for YARN for submission from C#. It allows for Java Driver
 * configuration generation directly on the Driver without need of Java dependency if REST
 * submission is used. Note that the name of the class must contain "REEFLauncher" for the time
 * being in order for the Interop code to discover the class.
 */
@Interop(CppFiles = "DriverLauncher.cpp")
public final class AzureBatchBootstrapClient {
  private static final Logger LOG = Logger.getLogger(org.apache.reef.bridge.client.AzureBatchBootstrapClient.class.getName());

  public static void main(final String[] args) throws IOException, InjectionException {
    LOG.log(Level.INFO, "Entering BootstrapLauncher.main().");

    if (args.length != 1) {
      final StringBuilder sb = new StringBuilder();
      sb.append("[ ");
      for (String arg : args) {
        sb.append(arg);
        sb.append(" ");
      }

      sb.append("]");

      final String message = "Bootstrap launcher should have one configuration file input, specifying the" +
          "job submission parameters to be deserialized to create the YarnDriverConfiguration on the fly." +
          " Current args are " + sb.toString();

      throw fatal(message, new IllegalArgumentException(message));
    }

    LOG.log(Level.INFO, "Entering BootstrapLauncher.main(). {0}", args[0]);

    try {

      final File partialConfigFile = new File(args[0]);
      final Configuration partialConfig = generateConfigurationFromJobSubmissionParameters(partialConfigFile);
      final Injector injector = Tang.Factory.getTang().newInjector(partialConfig);
      final AzureBatchRuntimeConfigurationProvider runtimeConfigurationProvider =
          injector.getInstance(AzureBatchRuntimeConfigurationProvider.class);

      final AzureBatchBootstrapDriverConfigGenerator azureBatchBootstrapDriverConfigGenerator =
          Tang.Factory.getTang().newInjector(runtimeConfigurationProvider.getAzureBatchRuntimeConfiguration())
              .getInstance(AzureBatchBootstrapDriverConfigGenerator.class);
      REEFLauncher.main(new String[]{azureBatchBootstrapDriverConfigGenerator.writeDriverConfigurationFile(args[0])});
    } catch (final Exception exception) {
      if (!(exception instanceof RuntimeException)) {
        throw fatal("Failed to initialize configurations.", exception);
      }

      throw exception;
    }
  }

  private static Configuration generateConfigurationFromJobSubmissionParameters(File params) throws IOException {
    final AvroAzureBatchJobSubmissionParameters avroAzureBatchJobSubmissionParameters;
    try (final FileInputStream fileInputStream = new FileInputStream(params)) {
      final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(
          AvroAzureBatchJobSubmissionParameters.getClassSchema(), fileInputStream);
      final SpecificDatumReader<AvroAzureBatchJobSubmissionParameters> reader =
          new SpecificDatumReader<>(AvroAzureBatchJobSubmissionParameters.class);
      avroAzureBatchJobSubmissionParameters = reader.read(null, decoder);
    }
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(AzureBatchAccountKey.class, avroAzureBatchJobSubmissionParameters.getAzureBatchAccountKey().toString())
        .bindNamedParameter(AzureBatchAccountName.class, avroAzureBatchJobSubmissionParameters.getAzureBatchAccountName().toString())
        .bindNamedParameter(AzureBatchAccountUri.class, avroAzureBatchJobSubmissionParameters.getAzureBatchAccountUri().toString())
        .bindNamedParameter(AzureBatchPoolId.class, avroAzureBatchJobSubmissionParameters.getAzureBatchPoolId().toString())
        .bindNamedParameter(AzureStorageAccountKey.class, avroAzureBatchJobSubmissionParameters.getAzureBatchAccountKey().toString())
        .bindNamedParameter(AzureStorageAccountName.class, avroAzureBatchJobSubmissionParameters.getAzureStorageAccountName().toString())
        .bindNamedParameter(AzureStorageContainerName.class, avroAzureBatchJobSubmissionParameters.getAzureStorageContainerName().toString())
        .bindNamedParameter(IsWindows.class, avroAzureBatchJobSubmissionParameters.getAzureBatchIsWindows().toString()).build();
  }

  private static RuntimeException fatal(final String msg, final Throwable t) {
    LOG.log(Level.SEVERE, msg, t);
    return new RuntimeException(msg, t);
  }

  private AzureBatchBootstrapClient() {
  }
}
