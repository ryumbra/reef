using Org.Apache.REEF.Client.AzureBatch.Util;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Xunit;

namespace Org.Apache.REEF.Client.Tests
{
    public class AzureBatchCommandBuilder
    {
        [Fact]
        public void LinuxCommandBuilderDriverTest()
        {
            // Prepare
            const int driverMemory = 100;
            AbstractCommandBuilder builder = TestContext.GetLinuxCommandBuilder();
            string expected = "/bin/sh -c \"unzip local.jar -d 'reef/'; java -Xmx100m -XX:PermSize=128m " +
                            "-XX:MaxPermSize=128m -classpath " +
                            "reef/local/*:reef/global/* " +
                            "-Dproc_reef org.apache.reef.runtime.common.REEFLauncher reef/local/driver.conf\"";

            // Action
            string actual = builder.BuildDriverCommand(driverMemory);

            // Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void WindowsCommandBuilderDriverTest()
        {
            // Prepare
            const int driverMemory = 100;
            AbstractCommandBuilder builder = TestContext.GetWindowsCommandBuilder();
            string expected = "powershell.exe /c \"Add-Type -AssemblyName System.IO.Compression.FileSystem; " +
                            "[System.IO.Compression.ZipFile]::ExtractToDirectory(\\\"$env:AZ_BATCH_TASK_WORKING_DIR\\local.jar\\\", " +
                            "\\\"$env:AZ_BATCH_TASK_WORKING_DIR\\reef\\\"); java -Xmx100m -XX:PermSize=128m " +
                            "-XX:MaxPermSize=128m -classpath " +
                            "'reef/local/*;reef/global/*;' " +
                            "-Dproc_reef org.apache.reef.runtime.common.REEFLauncher reef/local/driver.conf\";";

            // Action
            string actual = builder.BuildDriverCommand(driverMemory);

            // Assert
            Assert.Equal(expected, actual);
        }

        private class TestContext
        {
            public static AbstractCommandBuilder GetWindowsCommandBuilder()
            {
                IInjector injector = TangFactory.GetTang().NewInjector();
                return injector.GetInstance<WindowsCommandBuilder>();
            }

            public static AbstractCommandBuilder GetLinuxCommandBuilder()
            {
                IInjector injector = TangFactory.GetTang().NewInjector();
                return injector.GetInstance<LinuxCommandBuilder>();
            }
        }
    }
}
