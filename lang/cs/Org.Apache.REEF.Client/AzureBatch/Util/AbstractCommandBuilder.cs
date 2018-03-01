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

using System;
using System.Text;
using Org.Apache.REEF.Common.Files;

namespace Org.Apache.REEF.Client.AzureBatch.Util
{
    internal abstract class AbstractCommandBuilder : ICommandBuilder
    {
        private const string JAVA_EXE = @"java";
        private const string JVM_OPTIONS_PERM_SIZE = @"-XX:PermSize=128m";
        private const string JVM_OPTIONS_MAX_PERM_SIZE_FORMAT = @"-XX:MaxPermSize=128m";
        private const string JVM_OPTIONS_MAX_MEMORY_ALLOCATION_POOL_SIZE_FORMAT = @"-Xmx{0}m";
        private const string CLASS_PATH_TOKEN = @"-classpath";
        private const string PROC_REEF_PROPERTY = @"-Dproc_reef";
        private const string LAUNCHER_CLASS_NAME = @"org.apache.reef.runtime.common.REEFLauncher";
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
            sb.Append(" " + JAVA_EXE)
              .Append(" " + string.Format(JVM_OPTIONS_MAX_MEMORY_ALLOCATION_POOL_SIZE_FORMAT, driverMemory))
              .Append(" " + JVM_OPTIONS_PERM_SIZE)
              .Append(" " + JVM_OPTIONS_MAX_PERM_SIZE_FORMAT)
              .Append(" " + CLASS_PATH_TOKEN)
              .Append(" " + GetDriverClasspath())
              .Append(" " + PROC_REEF_PROPERTY)
              .Append(" " + LAUNCHER_CLASS_NAME)
              .Append(" " + _fileNames.GetDriverConfigurationPath()).Replace("\\", "/");
            return string.Format(_osCommandFormat, _commandPrefix + sb.ToString());
        }

        /// <summary>
        /// Returns the driver classpath string which is compatible with the intricacies of the OS.
        /// </summary>
        /// <returns>classpath parameter string.</returns>
        protected abstract string GetDriverClasspath();
    }
}
