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
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.AzureBatch.Storage
{
    [DefaultImplementation(typeof(AzureStorageUploader))]
    internal interface IStorageUploader
    {
        /// <summary>
        /// Uploads a given file to the given destination folder in Azure Storage.
        /// </summary>
        /// <param name="destination">Destination in Azure Storage where given file will be uploaded.</param>
        /// <param name="filePath">Path to the file to be uploaded.</param>
        /// <returns>Storage SAS URI for uploaded file.</returns>
        Task<Uri> UploadFile(string destination, string filePath);
    }
}