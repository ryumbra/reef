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
using Org.Apache.REEF.Client.Avro;
using Org.Apache.REEF.Client.Avro.AzureBatch;
using Org.Apache.REEF.Client.AzureBatch.Parameters;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Annotations;
using System.IO;

namespace Org.Apache.REEF.Client.AzureBatch.Util
{
    internal sealed class JobJarMaker
    {
        private readonly IResourceArchiveFileGenerator _resourceArchiveFileGenerator;
        private readonly DriverFolderPreparationHelper _driverFolderPreparationHelper;
        private readonly AzureBatchREEFDotNetParamSerializer _paramSerializer;
        private readonly AvroAzureBatchJobSubmissionParameters _avroAzureBatchJobSubmissionParameters;
        private readonly REEFFileNames _fileNames;

        [Inject]
        JobJarMaker(
            IResourceArchiveFileGenerator resourceArchiveFileGenerator,
            DriverFolderPreparationHelper driverFolderPreparationHelper,
            AzureBatchREEFDotNetParamSerializer paramSerializer,
            REEFFileNames fileNames,
            [Parameter(typeof(AzureBatchAccountKey))] string azureBatchAccountKey,
            [Parameter(typeof(AzureBatchAccountName))] string azureBatchAccountName,
            [Parameter(typeof(AzureBatchAccountUri))] string azureBatchAccountUri,
            [Parameter(typeof(AzureBatchPoolId))] string azureBatchPoolId,
            [Parameter(typeof(AzureStorageAccountKey))] string azureStorageAccountKey,
            [Parameter(typeof(AzureStorageAccountName))] string azureStorageAccountName,
            [Parameter(typeof(AzureStorageContainerName))] string azureStorageContainerName)
        {
            _resourceArchiveFileGenerator = resourceArchiveFileGenerator;
            _driverFolderPreparationHelper = driverFolderPreparationHelper;
            _paramSerializer = paramSerializer;
            _fileNames = fileNames;
            _avroAzureBatchJobSubmissionParameters = new AvroAzureBatchJobSubmissionParameters();
            _avroAzureBatchJobSubmissionParameters.AzureBatchAccountKey = azureBatchAccountKey;
            _avroAzureBatchJobSubmissionParameters.AzureBatchAccountName = azureBatchAccountName;
            _avroAzureBatchJobSubmissionParameters.AzureBatchAccountUri = azureBatchAccountUri;
            _avroAzureBatchJobSubmissionParameters.AzureBatchPoolId = azureBatchPoolId;
            _avroAzureBatchJobSubmissionParameters.AzureStorageAccountKey = azureStorageAccountKey;
            _avroAzureBatchJobSubmissionParameters.AzureStorageAccountName = azureStorageAccountName;
            _avroAzureBatchJobSubmissionParameters.AzureStorageContainerName = azureStorageContainerName;
            _avroAzureBatchJobSubmissionParameters.AzureBatchIsWindows = true;
        }

        /// <summary>
        /// Creates a JAR file for the job submission.
        /// </summary>
        /// <param name="jobRequest">Job request received from the client code.</param>
        /// <returns>A string path to file.</returns>
        public string CreateJobSubmissionJAR(JobRequest jobRequest)
        {
            var bootstrapJobArgs = new AvroJobSubmissionParameters
            {
                jobId = jobRequest.JobIdentifier,
                //// This is dummy in Azure Batch, as it does not use jobSubmissionFolder in Azure Batch.
                jobSubmissionFolder = Path.PathSeparator.ToString()
            };
            _avroAzureBatchJobSubmissionParameters.sharedJobSubmissionParameters = bootstrapJobArgs;
            string localDriverFolderPath = CreateDriverFolder(jobRequest.JobIdentifier);
            _driverFolderPreparationHelper.PrepareDriverFolder(jobRequest.AppParameters, localDriverFolderPath);
            _paramSerializer.SerializeJobFile(localDriverFolderPath, _avroAzureBatchJobSubmissionParameters);

            return _resourceArchiveFileGenerator.CreateArchiveToUpload(localDriverFolderPath);
        }

        private string CreateDriverFolder(string jobId)
        {
            return Path.GetFullPath(Path.Combine(Path.GetTempPath(), string.Join("-", "reef", jobId)) + Path.DirectorySeparatorChar);
        }
    }
}
