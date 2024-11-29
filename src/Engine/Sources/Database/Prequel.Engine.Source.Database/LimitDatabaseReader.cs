using System.Data.Common;
using Prequel.Data;

namespace Prequel.Engine.Source.Database;

/// <summary>
/// Data adapter that queries databases supporting 'LIMIT N' query syntax
/// </summary>
public class LimitDatabaseReader(string query, Func<DbConnection> connectionFactory)
    : DatabaseDataSourceReader(query, connectionFactory)
{
    protected const string SchemaQueryWrapper = "SELECT * FROM ({0}) as _schema LIMIT 0";

    /// <summary>
    /// Queries a single record from a SQL query and returns a schema object
    /// crated from the reader's field definitions.
    /// </summary>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Schema object</returns>
    public override async ValueTask<Schema> QuerySchemaAsync(CancellationToken cancellation = default)
    {
        return await QuerySchemaAsync(string.Format(SchemaQueryWrapper, Query.TrimEnd(';')), cancellation);
    }
}