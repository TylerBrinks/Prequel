using Prequel.Engine.IO;

namespace Prequel.Tests.Fakes;

public class FakeFileStream : IFileStream
{
    private readonly MemoryStream _memoryStream = new();

    public async Task<Stream> GetReadStreamAsync(CancellationToken cancellation = default)
    {
        if (!_memoryStream.CanSeek)
        {
            return new MemoryStream(_memoryStream.ToArray());
        }

        _memoryStream.Seek(0, SeekOrigin.Begin);
        return await Task.FromResult(_memoryStream);
    }

    public async Task<byte[]> ReadAllBytesAsync(CancellationToken cancellation = default)
    {
        var array = _memoryStream.ToArray();
        return await Task.FromResult(array);
    }

    public async Task<Stream> GetWriteStreamAsync(CancellationToken cancellation = default)
    {
        return await GetReadStreamAsync(cancellation);
    }
}