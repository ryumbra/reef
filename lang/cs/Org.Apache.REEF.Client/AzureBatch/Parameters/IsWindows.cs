using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.AzureBatch.Parameters
{
    [NamedParameter(Documentation = "Are the Azure Batch VMs linux or Windows based")]
    public class IsWindows : Name<bool>
    {
    }
}