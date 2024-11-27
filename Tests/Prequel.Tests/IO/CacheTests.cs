using Moq;
using Prequel.Engine.Caching;
using Prequel.Engine.Core.Data;
using Prequel.Engine.IO;

namespace Prequel.Tests.IO;

public class CachedTests
{
    //[Fact]
    //public async Task CachedSourceStream_Skips_Caching()
    //{
    //    var data = new List<object?[]> { new object?[] { 1, 2, 3 } };
    //    var reader = new Mock<IDataSourceReader>();
    //    reader.Setup(r => r.QueryAsync(It.IsAny<QueryContext>(), It.IsAny<CancellationToken>())).Returns(data.ToAsyncEnumerable());

    //    var cache = new MemoryCachedDataSourceReader(reader.Object, new CacheOptions());

    //    // First call is not cached
    //    await foreach(var row in cache.QueryAsync(new QueryContext()))
    //    {
    //    }

    //    // Second call should be cached
    //    await foreach (var row in cache.QueryAsync(new QueryContext()))
    //    {
    //    }

    //    reader.Verify(r => r.QueryAsync(It.IsAny<QueryContext>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
    //}

    [Fact]
    public async Task CachedSourceStream_Caches_Reader_Output()
    {
        var data = new List<object?[]> { new object?[] { 1, 2, 3 } };
        var reader = new Mock<ISchemaDataSourceReader>();
        reader.Setup(r => r.ReadSourceAsync(It.IsAny<QueryContext>(), It.IsAny<CancellationToken>())).Returns(data.ToAsyncEnumerable());

        var cache = new MemoryCachedDataSourceReader(reader.Object);

        // First call is not cached
        await foreach (var row in cache.ReadSourceAsync(new QueryContext()))
        {
        }

        // Second call should be cached
        await foreach (var row in cache.ReadSourceAsync(new QueryContext()))
        {
        }

        reader.Verify(r => r.ReadSourceAsync(It.IsAny<QueryContext>(), It.IsAny<CancellationToken>()), Times.Once);
    }
}