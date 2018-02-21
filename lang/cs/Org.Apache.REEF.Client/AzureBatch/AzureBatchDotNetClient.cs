using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using System;
using System.Threading.Tasks;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Client.AzureBatch.Parameters;

namespace Org.Apache.REEF.Client.DotNet.AzureBatch
{
    class AzureBatchDotNetClient : IREEFClient
    {
        /// <summary>
        /// The class name that contains the Java counterpart for this client.
        /// </summary>
        private const string JavaClassName = "org.apache.reef.bridge.client.YarnJobSubmissionClient";

        private static readonly Logger Logger = Logger.GetLogger(typeof(AzureBatchDotNetClient));
        private readonly IInjector _injector;
        private readonly DriverFolderPreparationHelper _driverFolderPreparationHelper;
        private readonly IJavaClientLauncher _javaClientLauncher;
        private readonly REEFFileNames _fileNames;
        private readonly JobRequestBuilderFactory _jobRequestBuilderFactory;
        private readonly BatchService _batchService;

        [Inject]
        private AzureBatchDotNetClient(
            IInjector injector,
            DriverFolderPreparationHelper driverFolderPreparationHelper,
            REEFFileNames fileNames,
            JobRequestBuilderFactory jobRequestBuilderFactory,
            BatchService batchService
            )
        {
            _injector = injector;
            _fileNames = fileNames;
            _driverFolderPreparationHelper = driverFolderPreparationHelper;
            _jobRequestBuilderFactory = jobRequestBuilderFactory;
            _batchService = batchService;
        }

        public Task<FinalState> GetJobFinalStatus(string appId)
        {
            throw new NotImplementedException();
        }

        public JobRequestBuilder NewJobRequestBuilder()
        {
            throw new NotImplementedException();
        }

        public Task SubmitAsync(JobRequest jobRequest)
        {
            string jobId = jobRequest.JobIdentifier;


        }

        public IJobSubmissionResult SubmitAndGetJobStatus(JobRequest jobRequest)
        {
            throw new NotImplementedException();
        }

        public void Launch(JobRequest jobRequest)
        {

        }
    }
}
