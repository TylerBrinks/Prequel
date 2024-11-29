using Prequel.Engine.Source.Memory;

namespace Prequel.Tests.IO;

public class StreamTests
{
    [Fact]
    public async Task InMemoryStream_Cannot_Write()
    {
        var stream = new InMemoryStream(new byte[1]);

        await Assert.ThrowsAsync<NotImplementedException>(async () =>  await stream.GetWriteStreamAsync());
    }
}
