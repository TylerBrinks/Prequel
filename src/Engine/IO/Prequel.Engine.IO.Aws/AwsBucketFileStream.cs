using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Transfer;
using System.Diagnostics.CodeAnalysis;

namespace Prequel.Engine.IO.Aws;

[ExcludeFromCodeCoverage]
public class AwsBucketFileStream(string bucketName, string key, BucketConnectionOptions options) : IFileStream
{
    private readonly AmazonS3Client _containerClient = new(new BasicAWSCredentials(options.AccessKey, options.SecretKey));

    //TODO: null check options and option properties

    /// <summary>
    /// Gets a memory stream for reading an Azure blob item
    /// </summary>
    /// <returns>Awaitable stream</returns>
    public async Task<Stream> GetReadStreamAsync(CancellationToken cancellation = default!)
    {
        var transfer = new TransferUtility(_containerClient);
        var stream = await transfer.OpenStreamAsync(bucketName, key, cancellation);
        return stream;
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
        return await GetReadStreamAsync(cancellation);
    }

    public override string ToString() => $"AWS Bucket Stream bucket={bucketName}, key={key}";
}