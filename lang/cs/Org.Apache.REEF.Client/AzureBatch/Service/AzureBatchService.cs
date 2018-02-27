using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Org.Apache.REEF.Client.AzureBatch.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using BatchSharedKeyCredential = Microsoft.Azure.Batch.Auth.BatchSharedKeyCredentials;

namespace Org.Apache.REEF.Client.DotNet.AzureBatch
{
    public class AzureBatchService : IDisposable
    {
        public BatchSharedKeyCredential Credentials { get; private set; }
        public string PoolId { get; private set; }

        private BatchClient Client { get; set; }
        private readonly IRetryPolicy retryPolicy;
        private bool disposed;

        [Inject]
        public AzureBatchService(
            [Parameter(typeof(AzureBatchAccountUri))] string azureBatchAccountUri,
            [Parameter(typeof(AzureBatchAccountName))] string azureBatchAccountName,
            [Parameter(typeof(AzureBatchAccountKey))] string azureBatchAccountKey,
            [Parameter(typeof(AzureBatchPoolId))] string azureBatchPoolId)
        {
            BatchSharedKeyCredential credentials = new BatchSharedKeyCredential(azureBatchAccountUri, azureBatchAccountName, azureBatchAccountKey);

            this.Client = BatchClient.Open(credentials);
            this.Credentials = credentials;
            this.retryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(5), 3);
            this.PoolId = azureBatchPoolId;

            this.Client.CustomBehaviors.Add(new RetryPolicyProvider(this.retryPolicy));
        }

        /// <summary>
        /// Dispose of this object and all members
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~AzureBatchService()
        {
            this.Dispose(false);
        }

        /// <summary>
        /// Disposes of this object
        /// </summary>
        private void Dispose(bool disposing)
        {
            if (this.disposed)
            {
                return;
            }

            if (disposing)
            {
                this.Client.Dispose();
            }

            this.disposed = true;
        }

        #region Job related operations

        public void CreateJob(string jobId, Uri resourceFile, string commandLine)
        {
            CloudJob unboundJob = this.Client.JobOperations.CreateJob();
            unboundJob.Id = jobId;

            PoolInformation poolInformation = new PoolInformation();
            poolInformation.PoolId = this.PoolId;
            unboundJob.PoolInformation = poolInformation;

            JobManagerTask jobManager = new JobManagerTask()
            {
                CommandLine = commandLine,
                Id = jobId,
                ResourceFiles = resourceFile != null ?
                    new List<ResourceFile>() { new ResourceFile(resourceFile.AbsoluteUri, "local.jar") } :
                    new List<ResourceFile>()
            };

            unboundJob.JobManagerTask = jobManager;
            unboundJob.Commit();
        }

        public CloudJob GetJob(string jobId, DetailLevel detailLevel)
        {
            using (Task<CloudJob> getJobTask = this.GetJobAsync(jobId, detailLevel))
            {
                getJobTask.Wait();
                return getJobTask.Result;
            }
        }

        public Task<CloudJob> GetJobAsync(string jobId, DetailLevel detailLevel)
        {
            return this.Client.JobOperations.GetJobAsync(jobId, detailLevel);
        }

        #endregion
    }
}