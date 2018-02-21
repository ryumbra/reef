using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Client.DotNet.AzureBatch
{
    public class CreateJobManagerOptions
    {
        public string JobManagerId { get; set; }

        public string CommandLine { get; set; }

        public int? MaxTaskRetryCount { get; set; }

        public TimeSpan? MaxTaskWallClockTime { get; set; }

        public TimeSpan? RetentionTime { get; set; }

        public bool? KillOnCompletion { get; set; }
    }
}