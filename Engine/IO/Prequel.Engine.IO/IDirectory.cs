namespace Prequel.Engine.IO;

/// <summary>
/// Defines operations for reading file from a directory
/// </summary>
public interface IDirectory
{
    Task<IEnumerable<IFileStream>> GetFilesWithExtensionAsync(string extension, CancellationToken cancellation = default!);
}