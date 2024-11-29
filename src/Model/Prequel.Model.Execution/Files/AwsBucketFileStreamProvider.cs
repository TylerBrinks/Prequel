using Prequel.Engine.IO.Aws;
using Prequel.Engine.IO;

namespace Prequel.Model.Execution.Files;

/// <summary>
/// AWS S3 bucket file connection data table
/// </summary>
public class AwsBucketFileStreamProvider : FileStreamProvider
{
    public required string BucketName { get; set; }
    public required string Key { get; set; }
    public required string AccessKey { get; set; }
    public required string SecretKey { get; set; }
    /// <summary>
    /// Builds a stream from a file stored as an AWS S3 blob
    /// </summary>
    /// <returns>AWS bucket IFileStream instance</returns>
    public override IFileStream GetFileStream()
    {
        return new AwsBucketFileStream(BucketName, Key, new BucketConnectionOptions
        {
            AccessKey = AccessKey,
            SecretKey = SecretKey
        });
    }
}