using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.AzureBatch.Parameters
{
    [NamedParameter(Documentation = "The Azure Batch Account Name")]
    public class AzureBatchAccountName : Name<string>
    {
    }
}