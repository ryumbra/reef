using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.AzureBatch.Parameters
{
    [NamedParameter(Documentation = "The Azure Batch account URI")]
    public class AzureBatchAccountUri : Name<string>
    {
    }
}