﻿// Licensed to the Apache Software Foundation (ASF) under one
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

using System;
using System.Text;
using Org.Apache.REEF.Common.Files;

namespace Org.Apache.REEF.Client.AzureBatch.Util
{
    internal abstract class AbstractCommandBuilder : ICommandBuilder
    {
        private const string JavaExe = @"java";
        private const string JvmOptionsPermSize = @"-XX:PermSize=128m";
        private const string JvmOptionsMaxPermSizeFormat = @"-XX:MaxPermSize=128m";
        private const string JvmOptionsMaxMemoryAllocationPoolSizeFormat = @"-Xmx{0}m";
        private const string ClassPathToken = @"-classpath";
        private const string ProcReefProperty = @"-Dproc_reef";
        private const string LibraryPathToken = @"-Djava.libaray.path";
        //// private const string LauncherClassName = @"org.apache.reef.runtime.common.REEFLauncher";
        private const string LauncherClassName = @"org.apache.reef.bridge.client.AzureBatchBootstrapREEFLauncher";
        protected readonly REEFFileNames _fileNames;
        protected readonly string _osCommandFormat;
        protected readonly string _commandPrefix;
        protected readonly AzureBatchFileNames _azureBatchFileNames;

        protected AbstractCommandBuilder(
            REEFFileNames fileNames,
            AzureBatchFileNames azureBatchFileNames,
            string commandPrefix,
            string osCommandFormat)
        {
            _fileNames = fileNames;
            _osCommandFormat = osCommandFormat;
            _commandPrefix = commandPrefix;
            _azureBatchFileNames = azureBatchFileNames;
        }

        public string BuildDriverCommand(int driverMemory)
        {
            var sb = new StringBuilder();
            sb.Append(_fileNames.GetBridgeExePath())
              .Append(" " + JavaExe)
              .Append(" " + string.Format(JvmOptionsMaxMemoryAllocationPoolSizeFormat, driverMemory))
              .Append(" " + JvmOptionsPermSize)
              .Append(" " + JvmOptionsMaxPermSizeFormat)
              .Append(" " + ClassPathToken)
              .Append(" " + GetDriverClasspath())
              //// .Append(" " + LibraryPathToken + "=" + GetLibarayClasspath())
              .Append(" " + ProcReefProperty)
              .Append(" " + LauncherClassName)
              //// .Append(" " + _fileNames.GetClrDriverConfigurationPath().Replace("\\", "/");
              .Append(" " + string.Format("{0}/{1}", _fileNames.GetReefFolderName(), _fileNames.GetJobSubmissionParametersFile()));
            /**
             * .Append(" " +
                    string.Format("{0}/{1}/{2}",
                        _fileNames.GetReefFolderName(),
                        _fileNames.GetLocalFolderName(),
                        _fileNames.GetAppSubmissionParametersFile()));
            **/
            return string.Format(_osCommandFormat, _commandPrefix + sb.ToString());
        }

        /// <summary>
        /// Returns the driver classpath string which is compatible with the intricacies of the OS.
        /// </summary>
        /// <returns>classpath parameter string.</returns>
        protected abstract string GetDriverClasspath();

        protected abstract string GetLibarayClasspath();
    }
}
