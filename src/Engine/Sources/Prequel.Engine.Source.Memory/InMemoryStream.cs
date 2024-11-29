using Prequel.Engine.IO;

namespace Prequel.Engine.Source.Memory;

/// <summary>
/// In-memory file stream used to read data out
/// of temporary in-memory storage
/// </summary>
public class InMemoryStream : IFileStream
{
    private readonly byte[] _bytes;

    public InMemoryStream(byte[] bytes)
    {
        _bytes = bytes;
    }

    public InMemoryStream(Stream stream)
    {
        var buffer = new byte[stream.Length];
        _ = stream.Read(buffer, 0, buffer.Length);
        _bytes = buffer;
    }

    /// <summary>
    /// Gets a memory stream containing the stored file's byte data
    /// </summary>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Memory stream</returns>
    public Task<Stream> GetReadStreamAsync(CancellationToken cancellation = default!)
    {
        return Task.FromResult<Stream>(new MemoryStream(_bytes));
    }
    /// <summary>
    /// Writes a file to an in-memory byte array
    /// </summary>
    /// <param name="cancellation"></param>
    /// <returns></returns>
    public Task<byte[]> ReadAllBytesAsync(CancellationToken cancellation = default!)
    {
        return Task.FromResult(_bytes);
    }
    /// <summary>
    /// Reads a file from an in-memory byte array
    /// </summary>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Writable stream</returns>
    /// <exception cref="NotImplementedException"></exception>
    public Task<Stream> GetWriteStreamAsync(CancellationToken cancellation = default)
    {
        throw new NotImplementedException("In memory stream is not writable");
    }
}