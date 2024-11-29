using Prequel.Engine.IO;

namespace Prequel.Model.Execution.Files;

/// <summary>
/// File stream provider for reading file bytes from the storage provider
/// </summary>
public abstract class FileStreamProvider
{
    public abstract IFileStream GetFileStream();
}