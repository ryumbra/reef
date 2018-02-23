using System;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Org.Apache.REEF.Client.AzureBatch.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using BatchSharedKeyCredential = Microsoft.Azure.Batch.Auth.BatchSharedKeyCredentials;

namespace Org.Apache.REEF.Client.DotNet.AzureBatch
{
    public class BatchService : IDisposable
    {
        public BatchSharedKeyCredential Credentials { get; private set; }
        public string PoolId { get; private set; }

        private BatchClient Client { get; set; }
        private readonly IRetryPolicy retryPolicy;
        private bool disposed;

        [Inject]
        public BatchService(
            [Parameter(typeof(AzureBatchAccountUri))] string azureBatchAccountUri,
            [Parameter(typeof(AzureBatchAccountName))] string azureBatchAccountName,
            [Parameter(typeof(AzureBatchAccountKey))] string azureBatchAccountKey,
            [Parameter(typeof(AzureBatchPoolId))] string azureBatchPoolId)
        {
            BatchSharedKeyCredentials credentials = new BatchSharedKeyCredentials(azureBatchAccountUri, azureBatchAccountName, azureBatchAccountKey);

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

        ~BatchService()
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

        public void CreateJob(string jobId, string commandLine)
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
            };

            unboundJob.JobManagerTask = jobManager;

            unboundJob.Commit();
        }

        public CloudJob GetJob(string jobId, DetailLevel detailLevel)
        {
            using (System.Threading.Tasks.Task<CloudJob> getJobTask = this.GetJobAsync(jobId, detailLevel))
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

        #region Task related operations

        public CloudTask GetTask(string jobId, string taskId, DetailLevel detailLevel)
        {
            using (Task<CloudTask> getTaskTask = this.GetTaskAsync(jobId, taskId, detailLevel))
            {
                getTaskTask.Wait();
                return getTaskTask.Result;
            }
        }

        public Task<CloudTask> GetTaskAsync(string jobId, string taskId, DetailLevel detailLevel)
        {
            return this.Client.JobOperations.GetTaskAsync(jobId, taskId, detailLevel);
        }

        /// <summary>
        /// Adds a task.
        /// </summary>
        /// <param name="options">The options describing the task to add.</param>
        /// <returns></returns>
        public async Task AddTaskAsync(AddTaskOptions options)
        {
            CloudTask unboundTask = new CloudTask(options.TaskId, options.CommandLine);
            if (options.IsMultiInstanceTask)
            {
                unboundTask.MultiInstanceSettings = new MultiInstanceSettings(options.BackgroundCommand, options.InstanceNumber);
                unboundTask.MultiInstanceSettings.CommonResourceFiles = options.CommonResourceFiles.ConvertAll(f => new ResourceFile(f.BlobSource, f.FilePath));
            }
            unboundTask.UserIdentity = new UserIdentity(new AutoUserSpecification(
                elevationLevel: options.RunElevated ? ElevationLevel.Admin : ElevationLevel.NonAdmin));
            unboundTask.Constraints = new TaskConstraints(null, null, options.MaxTaskRetryCount);
            unboundTask.ResourceFiles = options.ResourceFiles.ConvertAll(f => new ResourceFile(f.BlobSource, f.FilePath));
            await this.Client.JobOperations.AddTaskAsync(options.JobId, unboundTask);
        }
        #endregion
    }
}