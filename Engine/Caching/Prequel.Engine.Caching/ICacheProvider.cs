using Prequel.Engine.IO;

namespace Prequel.Engine.Caching;

public interface ICacheProvider
{
    Task<bool> ExistsAsync(string key, CancellationToken cancellation = default!);
    IFileStream GetFileStream(string path);
}