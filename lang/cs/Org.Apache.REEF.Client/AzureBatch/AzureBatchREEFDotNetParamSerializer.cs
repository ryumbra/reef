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

using System.IO;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Avro.AzureBatch;
using Org.Apache.REEF.Common.Avro;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.YARN
{
    /// <summary>
    /// Job/application parameter file serializer for <see cref="YarnREEFDotNetClient"/>.
    /// </summary>
    internal sealed class AzureBatchREEFDotNetParamSerializer
    {
        private readonly REEFFileNames _fileNames;
        private readonly AvroAzureBatchJobSubmissionParameters _avroAzureBatchJobSubmissionParameters;

        [Inject]
        private AzureBatchREEFDotNetParamSerializer(REEFFileNames fileNames,
            AvroAzureBatchJobSubmissionParameters avroAzureBatchJobSubmissionParameters)
        {
            _fileNames = fileNames;
            _avroAzureBatchJobSubmissionParameters = avroAzureBatchJobSubmissionParameters;
        }

        /// <summary>
        /// Serializes the job parameters to job-submission-params.json.
        /// </summary>
        internal void SerializeJobFile(JobParameters jobParameters, string localDriverFolderPath)
        {
            var serializedArgs = SerializeJobArgsToBytes();

            var submissionJobArgsFilePath = Path.Combine(
                new string[] 
                {
                    localDriverFolderPath,
                    _fileNames.GetReefFolderName(),
                    _fileNames.GetJobSubmissionParametersFile()
                });

            using (var jobArgsFileStream = new FileStream(submissionJobArgsFilePath, FileMode.CreateNew))
            {
                jobArgsFileStream.Write(serializedArgs, 0, serializedArgs.Length);
            }
        }

        internal byte[] SerializeJobArgsToBytes()
        {
            return AvroJsonSerializer<AvroAzureBatchJobSubmissionParameters>.ToBytes(_avroAzureBatchJobSubmissionParameters);
        }
    }
}
