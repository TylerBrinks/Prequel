using Prequel.Engine.IO;

namespace Prequel.Model.Execution.Files;

/// <summary>
/// Local disk stream provider
/// </summary>
public class LocalFileStreamProvider : FileStreamProvider
{
    public required string FilePath { get; set; }
    /// <summary>
    /// Gets a stream from a file saved on disk
    /// </summary>
    /// <returns>Local file IFileStream instance</returns>
    public override IFileStream GetFileStream()
    {
        return new LocalFileStream(FilePath);
    }
}