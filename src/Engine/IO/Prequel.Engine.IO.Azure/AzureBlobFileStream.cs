using Azure.Storage.Blobs;
using System.Diagnostics.CodeAnalysis;

namespace Prequel.Engine.IO.Azure;

[ExcludeFromCodeCoverage]
public class AzureBlobFileStream(string blobName, BlobConnectionOptions options) : IFileStream
{
    private readonly BlobContainerClient _containerClient = new(options.ConnectionString, options.CollectionName);

    //TODO null check context and option properties

    /// <summary>
    /// Gets a memory stream for reading an Azure blob item
    /// </summary>
    /// <returns>Awaitable stream</returns>
    public async Task<Stream> GetReadStreamAsync(CancellationToken cancellation = default!)
    {
        var blobClient = _containerClient.GetBlobClient(blobName);
        var memoryStream = new MemoryStream();

        await blobClient.DownloadToAsync(memoryStream, cancellation);
        memoryStream.Seek(0, SeekOrigin.Begin);

        return memoryStream;
    }
    /// <summary>
    /// Gets all bytes from an Azure blob item
    /// </summary>
    /// <returns>Awaitable byte array</returns>
    public async Task<byte[]> ReadAllBytesAsync(CancellationToken cancellation = default!)
    {
        return ((MemoryStream)await GetReadStreamAsync(cancellation)).ToArray();
    }
    /// <summary>
    /// Writes a file to physical disk storage
    /// </summary>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Writable stream</returns>
    /// <exception cref="NotImplementedException"></exception>
    public async Task<Stream> GetWriteStreamAsync(CancellationToken cancellation = default)
    {
        var blobClient = _containerClient.GetBlobClient(blobName);
        return await blobClient.OpenWriteAsync(true, cancellationToken: cancellation);
    }
    public override string ToString() => $"Azure Blob Stream blob={blobName}, collection={options.CollectionName}";
}


//public class AzureBlobDirectory : IDirectory
//{
//    private readonly BlobContainerClient _containerClient;

//    public AzureBlobDirectory(BlobContainerClient client)
//    {
//        _containerClient = client;
//    }

//    public async Task<IEnumerable<IFileStream>> GetFilesWithExtensionAsync(string extension, CancellationToken cancellation = default)
//    {
//        var blobs = _containerClient.GetBlobsAsync(BlobTraits.All, cancellationToken: cancellation);
//        var streams = new List<IFileStream>();

//        await foreach (var blob in blobs)
//        {
//            streams.Add(new AzureBlobFileStream(blob.Name, _containerClient));
//        }

//        return streams;
//    }
//}