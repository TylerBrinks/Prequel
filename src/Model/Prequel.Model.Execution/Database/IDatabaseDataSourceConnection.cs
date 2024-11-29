using Prequel.Engine.Caching;
using Prequel.Data;

namespace Prequel.Model.Execution.Database;

/// <summary>
/// Defines operations needed to build a database connection that
/// returns data in the form of a DataTable instance
/// </summary>
public interface IDatabaseDataSourceConnection : IDataSourceConnection
{
    string ConnectionString { get; }

    ValueTask<DataTable> BuildAsync(
        string name,
        string query,
        DataTableSchema schemaDefinition,
        CacheOptions cacheOptions,
        CancellationToken cancellation);
}