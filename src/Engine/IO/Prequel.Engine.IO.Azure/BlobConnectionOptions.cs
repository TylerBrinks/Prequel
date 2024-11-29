using System.Diagnostics.CodeAnalysis;

namespace Prequel.Engine.IO.Azure;

[ExcludeFromCodeCoverage]
public class BlobConnectionOptions
{
    public required string ConnectionString { get; set; }
    public required string CollectionName { get; set; }
}