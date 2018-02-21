using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.AzureBatch.Parameters
{
    [NamedParameter(Documentation = "The Azure Storage Account Name")]
    public class AzureStorageAccountName : Name<string>
    {
    }
}