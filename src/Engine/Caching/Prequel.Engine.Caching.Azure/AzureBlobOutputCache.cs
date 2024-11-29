using Azure.Storage.Blobs;
using Prequel.Engine.IO;
using Prequel.Engine.IO.Azure;

namespace Prequel.Engine.Caching.Azure;

/// <summary>
/// Azure provider for creating file streams in Azure Blob Storage
/// </summary>
/// <param name="options">Azure blob connection options</param>
public class AzureBlobCacheProvider(BlobConnectionOptions options) : ICacheProvider
{
    /// <summary>
    /// Checks if a given blob exists in Azure blob storage
    /// </summary>
    /// <param name="blobName">Full name of the blob</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>True if the blob exists; otherwise false</returns>
    public async Task<bool> ExistsAsync(string blobName, CancellationToken cancellation = default!)
    {
        var containerClient = new BlobContainerClient(options.ConnectionString, options.CollectionName);
        var blobClient = containerClient.GetBlobClient(blobName);
        return (await blobClient.ExistsAsync(cancellation)).Value;
    }
    /// <summary>
    /// Gets a stream for reading or writing to Azure blob storage
    /// </summary>
    /// <param name="path">Path to the blob</param>
    /// <returns>IFileStream instance</returns>
    public IFileStream GetFileStream(string path)
    {
        return new AzureBlobFileStream(path, options);
    }
}