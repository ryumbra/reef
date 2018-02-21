using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.AzureBatch.Parameters
{
    [NamedParameter(Documentation = "The Azure Storage Container Name")]
    public class AzureStorageContainerName : Name<string>
    {
    }
}