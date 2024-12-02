using System.Data.Common;
using Prequel.Data;
using Prequel.Engine.Source.Database;

namespace Prequel.Engine.Source.MsSql;

/// <summary>
/// MS Sql database reader
/// </summary>
public class MsSqlDatabaseReader(string query, Func<DbConnection> connectionFactory)
    : DatabaseDataSourceReader(query, connectionFactory)
{
    private const string SchemaQueryWrapper = "SELECT TOP 0 * FROM ({0}) as _schema";

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

    public override string ToString() => "MS Sql database reader";
}