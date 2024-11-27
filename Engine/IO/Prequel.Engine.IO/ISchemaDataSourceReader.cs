using Prequel.Engine.Core.Data;

namespace Prequel.Engine.IO;

/// <summary>
/// Defines operations for reading data with schema support form a configured source
/// </summary>
public interface ISchemaDataSourceReader : IDataSourceReader
{
    ValueTask<Schema> QuerySchemaAsync(CancellationToken cancellation = default!);
}
