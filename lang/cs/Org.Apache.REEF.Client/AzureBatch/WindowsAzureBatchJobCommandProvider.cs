// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
// 
//   http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System.Collections.Generic;
using System.Text;
using Org.Apache.REEF.Client.AzureBatch;
using Org.Apache.REEF.Client.AzureBatch.Parameters;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.AzureBatch
{
    /// <summary>
    /// WindowsAzureBatchJobCommandProvider is .NET implementation of `org.apache.reef.runtime.common.launch.JavaLaunchCommandBuilder`
    /// This class provides the command to be submitted to RM for execution of .NET driver running on Windows environment.
    /// </summary>
    internal sealed class WindowsAzureBatchJobCommandProvider : IAzureBatchJobCommandProvider
    {
        private static readonly string JavaExe = @"%JAVA_HOME%/bin/java";

        private static readonly string JvmOptionsPermSize = @"-XX:PermSize=128m";

        private static readonly string ProcReefProperty = @"-Dproc_reef";
        private static readonly string JavaLoggingProperty =
            @"-Djava.util.logging.config.class=org.apache.reef.util.logging.Config";
        private static readonly string LauncherClassName = @"org.apache.reef.bridge.client.REEFLauncher";
        private readonly REEFFileNames _fileNames;
        private readonly bool _enableDebugLogging;
        private readonly string _driverStdoutFilePath;
        private readonly string _driverStderrFilePath;

        [Inject]
        private WindowsAzureBatchJobCommandProvider(
            [Parameter(typeof(EnableDebugLogging))] bool enableDebugLogging,
            [Parameter(typeof(DriverStdoutFilePath))] string driverStdoutFilePath,
            [Parameter(typeof(DriverStderrFilePath))] string driverStderrFilePath,
            REEFFileNames fileNames)
        {
            _enableDebugLogging = enableDebugLogging;
            _fileNames = fileNames;
            _driverStdoutFilePath = driverStdoutFilePath;
            _driverStderrFilePath = driverStderrFilePath;
        }

        /// <summary>
        /// Builds the command to be submitted to AzureBatchRM
        /// </summary>
        /// <returns>Command string</returns>
        public string GetJobSubmissionCommand()
        {
            var sb = new StringBuilder();
            sb.Append(_fileNames.GetBridgeExePath());
            sb.Append(" " + JavaExe);
            sb.Append(" " + JvmOptionsPermSize);

            sb.Append(" " + ProcReefProperty);
            if (_enableDebugLogging)
            {
                sb.Append(" " + JavaLoggingProperty);
            }

            sb.Append(" " + LauncherClassName);
            sb.Append(" " + _fileNames.GetJobSubmissionParametersFile());
            sb.Append(" " +
                      string.Format("{0}/{1}/{2}",
                          _fileNames.GetReefFolderName(),
                          _fileNames.GetLocalFolderName(),
                          _fileNames.GetAppSubmissionParametersFile()));
            sb.Append(" " + string.Format("1> {0} 2> {1}", _driverStdoutFilePath, _driverStderrFilePath));
            return sb.ToString();
        }
    }
}