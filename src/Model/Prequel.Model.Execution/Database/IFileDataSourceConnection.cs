using Prequel.Engine.Caching;
using Prequel.Engine.Core.Data;

namespace Prequel.Model.Execution.Database;

/// <summary>
/// Defines a connection to a file-based data source.
/// </summary>
public interface IFileDataSourceConnection : IDataSourceConnection
{
    /// <summary>
    /// Builds a data table from a model schema and query context
    /// </summary>
    /// <param name="name">Data table name</param>
    /// <param name="query">SQL query</param>
    /// <param name="schemaDefinition">Table schema definition</param>
    /// <param name="cacheOptions">Caching provider</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Awaitable data table task</returns>
    ValueTask<DataTable> BuildAsync(
        string name,
        string query,
        DataTableSchema schemaDefinition,
        CacheOptions cacheOptions,
        CancellationToken cancellation = default!);
}