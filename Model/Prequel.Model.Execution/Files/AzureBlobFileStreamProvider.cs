using Prequel.Engine.IO.Azure;
using Prequel.Engine.IO;

namespace Prequel.Model.Execution.Files;

/// <summary>
/// Azure file connection data table
/// </summary>
public class AzureBlobFileStreamProvider : FileStreamProvider
{
    public required string BlobName { get; set; }
    public required string CollectionName { get; set; }
    public required string ConnectionString { get; set; }
    /// <summary>
    /// Builds a stream from a file stored as an Azure storage blob
    /// </summary>
    /// <returns>AWS bucket IFileStream instance</returns>
    public override IFileStream GetFileStream()
    {
        return new AzureBlobFileStream(BlobName, new BlobConnectionOptions
        {
            CollectionName = CollectionName,
            ConnectionString = ConnectionString
        });
    }
}