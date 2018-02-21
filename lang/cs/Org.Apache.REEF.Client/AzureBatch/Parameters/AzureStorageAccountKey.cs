using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.AzureBatch.Parameters
{
    [NamedParameter(Documentation = "The Azure Storage Account Key")]
    public class AzureStorageAccountKey : Name<string>
    {
    }
}