namespace Prequel.Engine.IO;

/// <summary>
/// IDirectory implementation for reading files stored on disk
/// in a specific directory
/// </summary>
public class LocalDirectory(string directoryPath) : IDirectory
{
    private string[]? _files;

    /// <summary>
    /// Builds IFileStream instances from all files in a directory.
    /// </summary>
    /// <param name="extension">File extension to filter files of a given type</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Awaitable list of IFleStream instances</returns>
    public async Task<IEnumerable<IFileStream>> GetFilesWithExtensionAsync(string extension, CancellationToken cancellation = default!)
    {
        _files ??= Directory.GetFiles(directoryPath).Where(f => f.ToLower().EndsWith(extension)).ToArray();

        var streams = _files.Select(f => (IFileStream)new LocalFileStream(f));

        return await Task.FromResult(streams);
    }
}