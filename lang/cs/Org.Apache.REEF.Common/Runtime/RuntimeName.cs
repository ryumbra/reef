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

namespace Org.Apache.REEF.Common.Runtime
{
    /// <summary>
    /// This enum reflects runtime name values as they defined on teh Java side.
    /// </summary>
    public enum RuntimeName
    {
        /// <summary>
        /// Same value as org.apache.reef.runtime.local.driver.RuntimeIdentifier.RUNTIME_NAME
        /// </summary>
        Local,

        /// <summary>
        /// Same value as org.apache.reef.runtime.yarn.driver.RuntimeIdentifier.RUNTIME_NAME
        /// </summary>
        Yarn,

        /// <summary>
        /// Same value as org.apache.reef.runtime.mesos.driver.RuntimeIdentifier.RUNTIME_NAME
        /// </summary>
        Mesos,

        /// <summary>
        /// Same value as org.apache.reef.runtime.mesos.driver.RuntimeIdentifier.RUNTIME_NAME
        /// </summary>
        AzBatch,

        /// <summary>
        /// Default value for the enum
        /// </summary>
        Default
    }
}
