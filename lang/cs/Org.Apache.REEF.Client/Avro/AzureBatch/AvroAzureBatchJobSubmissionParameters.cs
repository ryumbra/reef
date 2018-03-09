using System.Runtime.Serialization;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Client.Avro.AzureBatch
{
    /// <summary>
    /// Used to serialize and deserialize Avro record 
    /// org.apache.reef.reef.bridge.client.avro.AvroAzureBatchJobSubmissionParameters.
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
        /// Gets or sets the AzureBatchAccountKey field.
        /// </summary>
        [DataMember]
        public string AzureBatchAccountKey { get; set; }

        /// <summary>
        /// Gets or sets the AzureBatchAccountName field.
        /// </summary>
        [DataMember]
        public string AzureBatchAccountName { get; set; }

        /// <summary>
        /// Gets or sets the AzureBatchAccountUri field.
        /// </summary>
        [DataMember]
        public string AzureBatchAccountUri { get; set; }

        /// <summary>
        /// Gets or sets the AzureBatchPoolId field.
        /// </summary>
        [DataMember]
        public string AzureBatchPoolId { get; set; }

        /// <summary>
        /// Gets or sets the AzureStorageAccountKey field.
        /// </summary>
        [DataMember]
        public string AzureStorageAccountKey { get; set; }

        /// <summary>
        /// Gets or sets the AzureStorageAccountName field.
        /// </summary>
        [DataMember]
        public string AzureStorageAccountName { get; set; }

        /// <summary>
        /// Gets or sets the AzureStorageContainerName field.
        /// </summary>
        [DataMember]
        public string AzureStorageContainerName { get; set; }

        /// <summary>
        /// Gets or sets the AzureBatchIsWindows field.
        /// </summary>
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
        /// <param name="azureBatchAccountKey">The azureBatchAccountKey.</param>
        /// <param name="azureBatchAccountName">The azureBatchAccountName.</param>
        /// <param name="azureBatchAccountUri">The azureBatchAccountUri.</param>
        /// <param name="azureBatchPoolId">The azureBatchPoolId.</param>
        /// <param name="azureStorageAccountKey">The azureStorageAccountKey.</param>
        /// <param name="azureStorageAccountName">The azureStorageAccountName.</param>
        /// <param name="azureStorageContainerName">The azureStorageContainerName.</param>
        /// <param name="azureBatchIsWindows">The azureBatchIsWindows.</param>
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
    }
}
