using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.AzureBatch.Parameters
{
    [NamedParameter(Documentation = "The Azure Batch Account Key")]
    public class AzureBatchAccountKey : Name<string>
    {
    }
}