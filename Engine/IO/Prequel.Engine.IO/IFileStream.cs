namespace Prequel.Engine.IO;

/// <summary>
/// Defines operations for reading file streams
/// </summary>
public interface IFileStream
{
    Task<Stream> GetReadStreamAsync(CancellationToken cancellation = default!);
    Task<byte[]> ReadAllBytesAsync(CancellationToken cancellation = default!);
    Task<Stream> GetWriteStreamAsync(CancellationToken cancellation = default!);
}