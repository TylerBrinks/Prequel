using Prequel.Engine.IO.Azure;

namespace Prequel.Tests.Models;

public class ModelTests
{
    [Fact]
    public void BlobConnectionOptions_Initializes()
    {
       Assert.NotNull(new BlobConnectionOptions
        {
            CollectionName = "name",
            ConnectionString = "cs"
        });
    }
}
