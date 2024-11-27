using Prequel.Engine.Caching;
using Prequel.Engine.IO;

namespace Prequel.Tests.Fakes;

public class FakeOutputCache : ICacheProvider
{
    private readonly bool _exists;

    public FakeOutputCache(bool exists = false)
    {
        _exists = exists;
    }

    public Task<bool> ExistsAsync(string key, CancellationToken cancellation = default)
    {
        return Task.FromResult(_exists);
    }

    public IFileStream GetFileStream(string path)
    {
        return new FakeFileStream();
    }
}