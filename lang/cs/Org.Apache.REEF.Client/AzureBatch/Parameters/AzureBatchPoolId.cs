using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.AzureBatch.Parameters
{
    [NamedParameter(Documentation = "The Azure Batch Pool Id")]
    public class AzureBatchPoolId : Name<string>
    {
    }
}