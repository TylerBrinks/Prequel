using System.Runtime.CompilerServices;
using Prequel.Data;
using Prequel.Metrics;
using Prequel.Engine.IO;

namespace Prequel.Engine.Source.File;

public class FileQueryDataSourceReader : ISchemaDataSourceReader
{
    protected readonly string Name;
    protected readonly string Query;
    protected readonly Execution.ExecutionContext Context = new();

    public FileQueryDataSourceReader(string name, string query, PhysicalFileDataTable dataTable)
    {
        Name = name;
        Query = query;
        DataTable = dataTable;
        Context.RegisterDataTable(dataTable);
    }

    public PhysicalFileDataTable DataTable { get; init; }

    /// <summary>
    /// Query a database and returns the resulting database reader object.
    /// </summary>
    /// <param name="queryContext">Options for controlling the query output</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Awaitable task</returns>
    public virtual async IAsyncEnumerable<object?[]> ReadSourceAsync(
        QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        using var step = queryContext.Profiler.Step("Data Source Reader, File query data source, Read source");

        await foreach (var batch in Context.ExecuteQueryAsync(Query, queryContext, cancellation))
        {
            for (var i = 0; i < batch.RowCount; i++)
            {
                step.IncrementRowCount();

                var row = new object?[batch.Results.Count];

                for (var j = 0; j < batch.Results.Count; j++)
                {
                    row[j] = batch.Results[j].Values[i];
                }

                yield return row;
            }
        }
    }

    /// <summary>
    /// Queries a file and builds a schema from the returned field types
    /// </summary>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Query output schema</returns>
    public virtual async ValueTask<Schema> QuerySchemaAsync(CancellationToken cancellation = default)
    {
        if (string.IsNullOrWhiteSpace(Query))
        {
            return Context.Tables.First().Value.Schema!;
        }

        var enumerable = Context.ExecuteQueryAsync(Query, new QueryContext { MaxResults = 1000, BatchSize = 1000 }, cancellation); //TODO params

        var enumerator = enumerable.GetAsyncEnumerator(cancellation);
        await enumerator.MoveNextAsync();
        var schemaBatch = enumerator.Current;
        await enumerator.DisposeAsync();
        return schemaBatch.Schema;
    }
}