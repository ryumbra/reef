﻿using System;

namespace Org.Apache.REEF.Client.DotNet.AzureBatch
{
    public class CreateJobOptions
    {
        public string JobId { get; set; }

        public int? Priority { get; set; }

        public int? MaxRetryCount { get; set; }

        public bool? CreateJobManager { get; set; }

        public TimeSpan? MaxWallClockTime { get; set; }

        public string PoolId { get; set; }

        public CreateJobManagerOptions JobManagerOptions { get; set; }
    }
}
