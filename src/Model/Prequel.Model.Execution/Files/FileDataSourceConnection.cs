using Prequel.Engine.Source.Execution;
using Prequel.Engine.Caching;
using Prequel.Data;
using Prequel.Engine.Source.File;
using Prequel.Model.Execution.Database;

namespace Prequel.Model.Execution.Files;

/// <summary>
/// Connection that limits the number of records processes during a query
/// </summary>
public abstract class FileDataSourceConnection : DataSourceConnection
{
    public int InferMax { get; set; } = 100;
    public required FileStreamProvider FileStreamProvider { get; set; }
    public virtual required string Alias { get; set; }

    /// <summary>
    /// Creates a FileReader specific to the file provider
    /// </summary>
    /// <param name="name">Reader name</param>
    /// <param name="query">SQL query</param>
    /// <param name="schema">Query schema</param>
    /// <param name="cacheOptions">Caching options</param>
    /// <returns>FileReader instance</returns>
    public abstract FileQueryDataSourceReader CreateReader(
        string name,
        string query,
        Schema schema,
        CacheOptions cacheOptions);

    /// <summary>
    /// Builds a data table from an CSV file
    /// </summary>
    /// <param name="name">Data table name</param>
    /// <param name="query">SQL query</param>
    /// <param name="schemaDefinition">Schema of the file's fields</param>
    /// <param name="cacheOptions">Caching options</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Data table</returns>
    public ValueTask<DataTable> BuildAsync(
        string name,
        string query,
        DataTableSchema schemaDefinition,
        CacheOptions cacheOptions,
        CancellationToken cancellation = default!)
    {
        var schema = schemaDefinition.ToSchema();

        return ValueTask.FromResult((DataTable)new ExecutableDataTable(
            name,
            schema,
            CreateReader(Alias, query, schema, cacheOptions),
            cacheOptions));
    }
}