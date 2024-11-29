using Prequel.Engine.Source.Execution;
using Prequel.Engine.Caching;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Source.Database;

namespace Prequel.Model.Execution.Database;

/// <summary>
/// Defines properties needed to connect to a standard RDBMS data provider
/// </summary>
public abstract class DatabaseDataSourceConnection : DataSourceConnection, IDatabaseDataSourceConnection
{
    public required string Server { get; init; }
    public string? Port { get; init; }
    public required string Catalog { get; init; }
    public required string UserId { get; init; }
    public required string Password { get; init; }
    public abstract string ConnectionString { get; }

    /// <summary>
    /// Creates a DatabaseReader specific to the database provider
    /// </summary>
    /// <param name="query">SQL query</param>
    /// <returns>DatabaseReader instance</returns>
    public abstract DatabaseDataSourceReader CreateReader(string query);

    /// <summary>
    /// Builds a data table using the modeled schema that will query
    /// the specific RDBMS data source 
    /// </summary>
    /// <param name="name">Data table name</param>
    /// <param name="query">database sql query</param>
    /// <param name="schemaDefinition"></param>
    /// <param name="cacheOptions">Output cache instance</param>
    /// <param name="cancellation"></param>
    /// <returns>Database data table</returns>
    public virtual ValueTask<DataTable> BuildAsync(
        string name,
        string query,
        DataTableSchema schemaDefinition,
        CacheOptions cacheOptions,
        CancellationToken cancellation = default!)
    {
        return ValueTask.FromResult((DataTable)new ExecutableDataTable(
            name,
            schemaDefinition.ToSchema(),
            CreateReader(query),
            cacheOptions));
    }
}