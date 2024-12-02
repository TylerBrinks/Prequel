namespace Prequel.Engine.IO;

/// <summary>
/// IFileStream implementation for reading files stored on disk 
/// </summary>
/// <remarks>
/// Creates a new local file source instance for reading files on disk
/// </remarks>
/// <param name="filePath">Full path to the file</param>
public class LocalFileStream(string filePath) : IFileStream
{
    /// <summary>
    /// Gets a file stream for reading a physical file
    /// </summary>
    /// <returns>Awaitable stream</returns>
    public async Task<Stream> GetReadStreamAsync(CancellationToken cancellation = default!)
    {
        return await Task.FromResult(new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read));
    }
    /// <summary>
    /// Gets all bytes from a file on disk.
    /// </summary>
    /// <returns>Awaitable byte array</returns>
    public async Task<byte[]> ReadAllBytesAsync(CancellationToken cancellation = default!)
    {
        return await File.ReadAllBytesAsync(filePath, cancellation);
    }
    /// <summary>
    /// Writes a file to physical disk storage
    /// </summary>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Writable stream</returns>
    /// <exception cref="NotImplementedException"></exception>
    public async Task<Stream> GetWriteStreamAsync(CancellationToken cancellation = default)
    {
        return await Task.FromResult(File.OpenWrite(filePath));
    }

    public override string ToString() => $"Local file stream: path={filePath}";
}