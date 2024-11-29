using System.Diagnostics.CodeAnalysis;

namespace Prequel.Engine.IO.Aws;

[ExcludeFromCodeCoverage]
public class BucketConnectionOptions
{
    public required string AccessKey { get; set; }
    public required string SecretKey { get; set; }
}