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

using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Avro.AzureBatch;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Client.YARN;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using System.IO;
using System.Linq;

namespace Org.Apache.REEF.Client.AzureBatch.Util
{
    internal sealed class JobJarMaker
    {
        private readonly IResourceArchiveFileGenerator _resourceArchiveFileGenerator;
        private readonly DriverFolderPreparationHelper _driverFolderPreparationHelper;
        private readonly AzureBatchREEFDotNetParamSerializer _paramSerializer;

        [Inject]
        JobJarMaker(
            IResourceArchiveFileGenerator resourceArchiveFileGenerator,
            DriverFolderPreparationHelper driverFolderPreparationHelper,
            AzureBatchREEFDotNetParamSerializer paramSerializer)
        {
            _resourceArchiveFileGenerator = resourceArchiveFileGenerator;
            _driverFolderPreparationHelper = driverFolderPreparationHelper;
            _paramSerializer = paramSerializer;
        }

        /// <summary>
        /// Creates a JAR file for the job submission.
        /// </summary>
        /// <param name="jobRequest">Job request received from the client code.</param>
        /// <returns>A string path to file.</returns>
        public string CreateJobSubmissionJAR(JobRequest jobRequest)
        {
            string localDriverFolderPath = CreateDriverFolder(jobRequest.JobIdentifier);
            _driverFolderPreparationHelper.PrepareDriverFolder(jobRequest.AppParameters, localDriverFolderPath);
            _paramSerializer.SerializeJobFile(jobRequest.JobParameters, localDriverFolderPath);

            return _resourceArchiveFileGenerator.CreateArchiveToUpload(localDriverFolderPath);
        }

        private string CreateDriverFolder(string jobId)
        {
            return Path.GetFullPath(Path.Combine(Path.GetTempPath(), string.Join("-", "reef", jobId)) + "\\");
        }
    }
}
