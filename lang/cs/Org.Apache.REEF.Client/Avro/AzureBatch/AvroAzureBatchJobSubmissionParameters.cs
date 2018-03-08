using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Org.Apache.REEF.Client.AzureBatch.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Client.Avro.AzureBatch
{
    /// <summary>
    /// Used to serialize and deserialize Avro record 
    /// org.apache.reef.reef.bridge.client.avro.AvroLocalJobSubmissionParameters.
    /// This is a (mostly) auto-generated class. 
    /// For instructions on how to regenerate, please view the README.md in the same folder.
    /// </summary>
    [Private]
    [DataContract(Namespace = "org.apache.reef.reef.bridge.client.avro")]
    public sealed class AvroAzureBatchJobSubmissionParameters
    {
        private const string JsonSchema = @"{""type"":""record"",""name"":""org.apache.reef.reef.bridge.client.avro.AvroAzureBatchJobSubmissionParameters"",""doc"":""Job submission parameters used by the Azure Batch runtime"",""fields"":[{""name"":""sharedJobSubmissionParameters"",""type"":{""type"":""record"",""name"":""org.apache.reef.reef.bridge.client.avro.AvroJobSubmissionParameters"",""doc"":""General cross-language job submission parameters shared by all runtimes"",""fields"":[{""name"":""jobId"",""type"":""string""},{""name"":""jobSubmissionFolder"",""type"":""string""}]}},{""name"":""AzureBatchAccountKey"",""type"":""string""},{""name"":""AzureBatchAccountName"",""type"":""string""},{""name"":""AzureBatchAccountUri"",""type"":""string""},{""name"":""AzureBatchPoolId"",""type"":""string""},{""name"":""AzureStorageAccountKey"",""type"":""string""},{""name"":""AzureStorageAccountName"",""type"":""string""},{""name"":""AzureStorageContainerName"",""type"":""string""},{""name"":""AzureBatchIsWindows"",""type"":""boolean""}]}";

        /// <summary>
        /// Gets the schema.
        /// </summary>
        public static string Schema
        {
            get
            {
                return JsonSchema;
            }
        }

        /// <summary>
        /// Gets or sets the sharedJobSubmissionParameters field.
        /// </summary>
        [DataMember]
        public AvroJobSubmissionParameters sharedJobSubmissionParameters { get; set; }

        /// <summary>
        /// Gets or sets the sharedJobSubmissionParameters field.
        /// </summary>
        [DataMember]
        public string AzureBatchAccountKey { get; set; }

        /// <summary>
        /// Gets or sets the driverStdoutFilePath field.
        /// </summary>
        [DataMember]
        public string AzureBatchAccountName { get; set; }

        /// <summary>
        /// Gets or sets the driverStderrFilePath field.
        /// </summary>
        [DataMember]
        public string AzureBatchAccountUri { get; set; }

        [DataMember]
        public string AzureBatchPoolId { get; set; }

        [DataMember]
        public string AzureStorageAccountKey { get; set; }

        [DataMember]
        public string AzureStorageAccountName { get; set; }

        [DataMember]
        public string AzureStorageContainerName { get; set; }

        [DataMember]
        public bool AzureBatchIsWindows { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="AvroAzureBatchJobSubmissionParameters"/> class.
        /// </summary>
        public AvroAzureBatchJobSubmissionParameters()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AvroAzureBatchJobSubmissionParameters"/> class.
        /// </summary>
        /// <param name="sharedJobSubmissionParameters">The sharedJobSubmissionParameters.</param>
        /// <param name="driverStdoutFilePath">The driverStdoutFilePath.</param>
        /// <param name="driverStderrFilePath">The drverStderrFilePath.</param>
        public AvroAzureBatchJobSubmissionParameters(
            string azureBatchAccountKey,
            string azureBatchAccountName,
            string azureBatchAccountUri,
            string azureBatchPoolId,
            string azureStorageAccountKey,
            string azureStorageAccountName,
            string azureStorageContainerName,
            bool azureBatchIsWindows)
        {
            AzureBatchAccountKey = azureStorageAccountKey;
            AzureBatchAccountName = azureBatchAccountName;
            AzureBatchAccountUri = azureBatchAccountUri;
            AzureBatchPoolId = azureBatchPoolId;
            AzureStorageAccountKey = azureStorageAccountKey;
            AzureStorageAccountName = azureStorageAccountName;
            AzureStorageContainerName = azureStorageContainerName;
            AzureBatchIsWindows = azureBatchIsWindows;
        }
        /*
        [Inject]
        public AvroAzureBatchJobSubmissionParameters(
            [Parameter(typeof(AzureBatchAccountKey))] string azureBatchAccountKey,
            [Parameter(typeof(AzureBatchAccountName))] string azureBatchAccountName,
            [Parameter(typeof(AzureBatchAccountUri))] string azureBatchAccountUri,
            [Parameter(typeof(AzureBatchPoolId))] string azureBatchPoolId,
            [Parameter(typeof(AzureStorageAccountKey))] string azureStorageAccountKey,
            [Parameter(typeof(AzureStorageAccountName))] string azureStorageAccountName,
            [Parameter(typeof(AzureStorageContainerName))] string azureStorageContainerName)
        {
            AzureBatchAccountKey = azureStorageAccountKey;
            AzureBatchAccountName = azureBatchAccountName;
            AzureBatchAccountUri = azureBatchAccountUri;
            AzureBatchPoolId = azureBatchPoolId;
            AzureStorageAccountKey = azureStorageAccountKey;
            AzureStorageAccountName = azureStorageAccountName;
            AzureStorageContainerName = azureStorageContainerName;
            AzureBatchIsWindows = true;
        }*/
    }
}
