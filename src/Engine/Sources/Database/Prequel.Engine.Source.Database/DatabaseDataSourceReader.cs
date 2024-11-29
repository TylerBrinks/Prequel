using System.Data;
using System.Runtime.CompilerServices;
using Prequel.Data;
using Prequel.Metrics;
using Prequel.Engine.IO;

namespace Prequel.Engine.Source.Database;

/// <summary>
/// Data source reader used to read data from a RDBMS server
/// </summary>
/// <param name="query">Provider-specific SQL query</param>
/// <param name="connectionFactory">Connection factory instance</param>
public abstract class DatabaseDataSourceReader(string query, Func<IDbConnection> connectionFactory)
    : ISchemaDataSourceReader
{
    protected readonly string Query = query;
    protected readonly Func<IDbConnection> ConnectionFactory = connectionFactory;

    /// <summary>
    /// Query a database and returns the resulting database reader object.
    /// </summary>
    /// <param name="queryContext">Options for controlling the query output</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Awaitable task</returns>
    public virtual async IAsyncEnumerable<object?[]> ReadSourceAsync(QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        using var connection = ConnectionFactory();
        using var command = connection.CreateCommand();
        await connection.OpenAsync(cancellation);
        command.CommandText = Query;
        using var reader = await command.ExecuteReaderAsync(cancellation);

        var fieldCount = reader.FieldCount;
        var recordCount = 0;

        using var step = queryContext.Profiler.Step("Data Source Reader, Database data source, Read source");

        // Limit results to the configured maximum
        while (await reader.ReadAsync(cancellation))
        {
            step.IncrementRowCount();

            var results = new object[fieldCount];
            reader.GetValues(results);
            yield return results;

            // Limit results to the configured maximum
            if (queryContext.MaxResults > 0 && recordCount++ > queryContext.MaxResults)
            {
                yield break;
            }
        }
    }
    /// <summary>
    /// Queries a database and builds a schema from the returned field types
    /// </summary>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Query output schema</returns>
    public abstract ValueTask<Schema> QuerySchemaAsync(CancellationToken cancellation = default!);
    /// <summary>
    /// Reads a DbDataReader object and builds a schema from the field list
    /// </summary>
    /// <param name="reader">dbDataReader instance</param>
    /// <returns>Schema object</returns>
    protected virtual Schema ReadSchema(IDataReader reader)
    {
        var fieldCount = reader.FieldCount;
        var fields = new List<QualifiedField>(fieldCount);

        for (var i = 0; i < fieldCount; i++)
        {
            fields.Add(new QualifiedField(reader.GetName(i), reader.GetFieldType(i).GetColumnType()));
        }

        return new Schema(fields);
    }
    /// <summary>
    /// Executes a query against a database attempting to limit rows to
    /// zero so the schema can be inferred without the cost of returning
    /// any part of the data set.
    /// </summary>
    /// <param name="schemaMetadataQuery">Query for looking up schema metadata</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Schema object</returns>
    protected virtual ValueTask<Schema> QuerySchemaAsync(string schemaMetadataQuery, CancellationToken cancellation = default!)
    {
        using var connection = ConnectionFactory();
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = schemaMetadataQuery;
        var reader = command.ExecuteReader();

        return ValueTask.FromResult(ReadSchema(reader));
    }
}