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
using Org.Apache.REEF.Client.AzureBatch;

namespace Org.Apache.REEF.Client.DotNet.AzureBatch
{
    class AzureBatchDotNetClient : IREEFClient
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(AzureBatchDotNetClient));
        private readonly IInjector _injector;
        private readonly DriverFolderPreparationHelper _driverFolderPreparationHelper;
        private readonly REEFFileNames _fileNames;
        private readonly JobRequestBuilderFactory _jobRequestBuilderFactory;
        private readonly BatchService _batchService;

        [Inject]
        private AzureBatchDotNetClient(
            IInjector injector,
            DriverFolderPreparationHelper driverFolderPreparationHelper,
            REEFFileNames fileNames,
            JobRequestBuilderFactory jobRequestBuilderFactory,
            BatchService batchService)
        {
            _injector = injector;
            _fileNames = fileNames;
            _driverFolderPreparationHelper = driverFolderPreparationHelper;
            _jobRequestBuilderFactory = jobRequestBuilderFactory;
            _batchService = batchService;
        }

        public JobRequestBuilder NewJobRequestBuilder()
        {
            return _jobRequestBuilderFactory.NewInstance();
        }

        public Task<FinalState> GetJobFinalStatus(string appId)
        {
            throw new NotImplementedException();
        }

        public void Submit(JobRequest jobRequest)
        {
            var configModule = AzureBatchClientConfiguration.ConfigurationModule;
            string jobId = jobRequest.JobIdentifier;
            string commandLine = GetCommand(jobRequest.JobParameters);
            _batchService.CreateJob(jobId, commandLine);
        }

        private string GetCommand(JobParameters jobParameters)
        {
            var commandProviderConfigModule = AzureBatchCommandProviderConfiguration.ConfigurationModule;
            if (jobParameters.JavaLogLevel == JavaLoggingSetting.Verbose)
            {
                commandProviderConfigModule = commandProviderConfigModule
                    .Set(AzureBatchCommandProviderConfiguration.JavaDebugLogging, true.ToString().ToLowerInvariant());
            }

            if (jobParameters.StdoutFilePath.IsPresent())
            {
                commandProviderConfigModule = commandProviderConfigModule
                    .Set(AzureBatchCommandProviderConfiguration.DriverStdoutFilePath, jobParameters.StdoutFilePath.Value);
            }

            if (jobParameters.StderrFilePath.IsPresent())
            {
                commandProviderConfigModule = commandProviderConfigModule
                    .Set(AzureBatchCommandProviderConfiguration.DriverStderrFilePath, jobParameters.StderrFilePath.Value);
            }

            var azureBatchJobCommandProvider = _injector.ForkInjector(commandProviderConfigModule.Build())
                .GetInstance<IAzureBatchJobCommandProvider>();

            var command = azureBatchJobCommandProvider.GetJobSubmissionCommand();

            Log.Log(Level.Verbose, "Command for Azure Batch: {0}", command);
            return command;
        }

        public IJobSubmissionResult SubmitAndGetJobStatus(JobRequest jobRequest)
        {
            Submit(jobRequest);
            return null;
        }
    }
}
