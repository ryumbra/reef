using System;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.AzureBatch.Util
{
    /// <summary>
    /// Access to the various places things go according to the REEF Azure Batch runtime.
    /// </summary>
    internal sealed class AzureBatchFileNames
    {
        private const string STORAGE_JOB_FOLDER_PATH = "apps/reef/jobs/";
        private const string TASL_JAS_FILE_NAME = "local.jar";
        private readonly REEFFileNames _reefFileNames;

        [Inject]
        private AzureBatchFileNames(REEFFileNames reefFileNames)
        {
            _reefFileNames = reefFileNames;
        }

        /// <summary>
        /// </summary>
        /// <param name="jobId">Job Submission Id</param>
        /// <returns>The relative path to the folder storing the job assets.</returns>
        public string getStorageJobFolder(string jobId)
        {
            return STORAGE_JOB_FOLDER_PATH + jobId;
        }

        /// <summary>
        /// </summary>
        /// <returns>The name under which the task jar will be stored.</returns>
        public static string getTaskJarFileName()
        {
            return TASL_JAS_FILE_NAME;
        }
    }
}
