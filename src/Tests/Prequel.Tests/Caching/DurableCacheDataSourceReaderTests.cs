using Prequel.Engine.Caching;
using Prequel.Tests.Fakes;
using Moq;
using Prequel.Data;
using Prequel.Engine.IO;

namespace Prequel.Tests.Caching;

public class DurableCacheDataSourceReaderTests
{
    [Fact]
    public async Task DurableCacheReader_Reads_Cache_And_Ingors_Source()
    {
        var sourceReader = new FakeDataSourceReader([[1]]);

        var cache = new DurableCacheDataSourceReader(sourceReader, new CacheOptions());

        await foreach (var _ in cache.ReadSourceAsync(new QueryContext()))
        {
        }

        Assert.True(sourceReader.SourceRead);
    }

    [Fact]
    public async Task DurableCacheReader_Reads_Source_And_Ingors_Cache()
    {
        var sourceReader = new FakeDataSourceReader([[1]]);
        var cacheReader = new Mock<IDataSourceReader>();
        cacheReader.Setup(c => c.ReadSourceAsync(It.IsAny<QueryContext>(), It.IsAny<CancellationToken>()))
            .Returns((new List<object?[]>
            {
                new object?[] { 1 }
            }).ToAsyncEnumerable());

        var provider = new Mock<ICacheProvider>();
        provider.Setup(c => c.ExistsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync(true);

        var options = new CacheOptions
        {
            DurableCacheKey = "key",
            GetDataReader = _ => cacheReader.Object,
            CacheProvider = provider.Object,
        };
        var cache = new DurableCacheDataSourceReader(sourceReader, options);

        await foreach (var _ in cache.ReadSourceAsync(new QueryContext()))
        {
        }

        Assert.False(sourceReader.SourceRead);
    }
}