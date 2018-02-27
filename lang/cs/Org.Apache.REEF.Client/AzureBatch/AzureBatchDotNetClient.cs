using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using System;
using System.Threading.Tasks;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Client.AzureBatch.Storage;
using Org.Apache.REEF.Client.AzureBatch;
using Org.Apache.REEF.Client.AzureBatch.Util;

namespace Org.Apache.REEF.Client.DotNet.AzureBatch
{
    class AzureBatchDotNetClient : IREEFClient
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(AzureBatchDotNetClient));
        private readonly IInjector _injector;
        private readonly IResourceArchiveFileGenerator _resourceArchiveFileGenerator;
        private readonly DriverFolderPreparationHelper _driverFolderPreparationHelper;
        private readonly REEFFileNames _fileNames;
        private readonly IStorageUploader _storageUploader;
        private readonly JobRequestBuilderFactory _jobRequestBuilderFactory;
        private readonly AzureBatchService _batchService;

        [Inject]
        private AzureBatchDotNetClient(
            IInjector injector,
            IResourceArchiveFileGenerator resourceArchiveFileGenerator,
            DriverFolderPreparationHelper driverFolderPreparationHelper,
            IStorageUploader storageUploader,
            REEFFileNames fileNames,
            JobRequestBuilderFactory jobRequestBuilderFactory,

            AzureBatchService batchService)
        {
            _injector = injector;
            _resourceArchiveFileGenerator = resourceArchiveFileGenerator;
            _fileNames = fileNames;
            _driverFolderPreparationHelper = driverFolderPreparationHelper;
            _storageUploader = storageUploader;
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

            _batchService.CreateJob(jobId, null, commandLine);
        }

        private string GetCommand(JobParameters jobParameters)
        {
            var commandProviderConfigModule = AzureBatchCommandBuilderConfiguration.ConfigurationModule;
            if (jobParameters.JavaLogLevel == JavaLoggingSetting.Verbose)
            {
                commandProviderConfigModule = commandProviderConfigModule
                    .Set(AzureBatchCommandBuilderConfiguration.JavaDebugLogging, true.ToString().ToLowerInvariant());
            }

            if (jobParameters.StdoutFilePath.IsPresent())
            {
                commandProviderConfigModule = commandProviderConfigModule
                    .Set(AzureBatchCommandBuilderConfiguration.DriverStdoutFilePath, jobParameters.StdoutFilePath.Value);
            }

            if (jobParameters.StderrFilePath.IsPresent())
            {
                commandProviderConfigModule = commandProviderConfigModule
                    .Set(AzureBatchCommandBuilderConfiguration.DriverStderrFilePath, jobParameters.StderrFilePath.Value);
            }

            var azureBatchJobCommandBuilder = _injector.ForkInjector(commandProviderConfigModule.Build())
                .GetInstance<ICommandBuilder>();

            var command = azureBatchJobCommandBuilder.BuildDriverCommand();

            Log.Log(Level.Verbose, "Command for Azure Batch: {0}", command);
            return command;
        }

        public IJobSubmissionResult SubmitAndGetJobStatus(JobRequest jobRequest)
        {
            Submit(jobRequest);

            // TODO[fix this null]
            return null;
        }
    }
}
