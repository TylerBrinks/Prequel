using Prequel.Engine.Caching;
using Prequel.Engine.Core.Data;
using Prequel.Model.Execution.Database;

namespace Prequel.Model.Execution;

/// <summary>
/// Factory class for building a data table from the 
///results of a query against a file-based data source
/// </summary>
public class FileDataTableReference : DataTableReference
{
    public required string Query { get; set; }
    /// <summary>
    /// Builds a data table from a file read and SQL query operation.
    /// </summary>
    /// <param name="cacheOptions">Caching options</param>
    /// <param name="cancellation">Cancellation token</param>
    /// <returns>Data table instance</returns>
    public override async ValueTask<DataTable> BuildAsync(CacheOptions cacheOptions, CancellationToken cancellation = default!)
    {
        var fileConnection = (IFileDataSourceConnection)Connection;

        return await fileConnection.BuildAsync(Name, Query, Schema, cacheOptions, cancellation);
    }
}