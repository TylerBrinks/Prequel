using Prequel.Engine.IO;

namespace Prequel.Engine.Caching.File;

public class LocalFileCacheProvider(string cacheDirectory) : ICacheProvider
{
    public Task<bool> ExistsAsync(string key, CancellationToken cancellation = default)
    {
        var directory = cacheDirectory.TrimEnd('\\', '/');
        return Task.FromResult(System.IO.File.Exists($"{directory}\\{key}"));
    }

    public IFileStream GetFileStream(string path)
    {
        var directory = cacheDirectory.TrimEnd('\\', '/');
        return new LocalFileStream($"{directory}\\{path}");
    }
}
