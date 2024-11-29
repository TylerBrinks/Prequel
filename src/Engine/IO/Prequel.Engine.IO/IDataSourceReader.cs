using Prequel.Data;

namespace Prequel.Engine.IO;

/// <summary>
/// Defines operations for reading data with schema support form a configured source
/// </summary>
public interface IDataSourceReader
{
    IAsyncEnumerable<object?[]> ReadSourceAsync(QueryContext queryContext, CancellationToken cancellation = default!);
}
