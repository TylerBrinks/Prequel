using System.Runtime.CompilerServices;
using Prequel.Core.Data;
using Prequel.Execution;
using Prequel.Data;
using Prequel.Metrics;
using Prequel.Engine.IO;
using Prequel.Engine.Caching;

namespace Prequel.Engine.Source.File;

/// <summary>
/// Physical file data storage
/// </summary>
public abstract class PhysicalFileDataTable(string tableName, CacheOptions cacheOptions) : DataTable
{
    /// <summary>
    /// Table name
    /// </summary>
    public sealed override string Name { get; } = tableName;

    /// <summary>
    /// Caching context s
    /// </summary>
    protected CacheOptions CacheOptions { get; set; } = cacheOptions;

    /// <summary>
    /// Reads data from the stream in consumable batches
    /// </summary>
    /// <param name="indices">Indices of the data to read; all other data is omitted.</param>
    /// <param name="queryContext">Options for controlling the query output</param>
    /// <param name="cancellation">IOptional cancellation token</param>
    /// <returns>Async enumerable list of string values</returns>
    public abstract IAsyncEnumerable<List<string?[]>> ReadAsync(
        List<int> indices,
        QueryContext queryContext,
        CancellationToken cancellation = default!);
    /// <summary>
    /// Infers the source's schema by reading a subset of the source's data
    /// </summary>
    /// <param name="queryContext">Options for controlling the query output</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Awaitable task</returns>
    public abstract Task InferSchemaAsync(QueryContext queryContext, CancellationToken cancellation = default!);
    /// <summary>
    /// Creates an execution that will read a physical file and yield record batches
    /// </summary>
    /// <param name="projection">Plan projection with fields to read</param>
    /// <returns>Avro execution plan</returns>
    public override IExecutionPlan Scan(List<int>? projection)
    {
        IExecutionPlan execution = new PhysicalFileExecution(Schema!, BuildProjection(projection), this);

        if (CacheOptions.ShouldCacheOutput)
        {
            execution = new OutputCacheExecution(Schema!, CacheOptions, execution);
        }

        return execution;
    }

    protected async IAsyncEnumerable<List<string?[]>> EnumerateDataReader(
        QueryContext queryContext,
        IDataSourceReader reader,
        [EnumeratorCancellation] CancellationToken cancellation)
    {
        var rows = new List<string?[]>();
        var count = 0;

        await foreach (var line in reader.ReadSourceAsync(queryContext, cancellation))
        {
            rows.Add(line.Select(l => l != null ? l.ToString() : string.Empty).ToArray());

            if (++count != queryContext.BatchSize)
            {
                continue;
            }

            count = 0;
            // Copy the lines read from the source reader
            var slice = rows.ToList();
            rows.Clear();

            yield return slice;
        }

        if (count > 0)
        {
            yield return rows;
        }
    }

    protected static async Task<List<InferredDataType>> GetInferredDataTypes(IDataSourceReader reader,
        QueryContext queryContext,
        int inferMax, CancellationToken cancellation)
    {
        List<InferredDataType> columnTypes = null!;

        var rowCount = 0;

        using var step = queryContext.Profiler.Step("Data Table, Physical File data table, Infer data types");

        await foreach (var line in reader.ReadSourceAsync(new QueryContext(), cancellation)) //TODO why not reuse the existing query context?
        {
            step.IncrementRowCount();
            columnTypes ??= [.. Enumerable.Range(0, line.Length).Select(_ => new InferredDataType())];

            for (var i = 0; i < line.Length; i++)
            {
                if (line[i] is not string value) { continue; }
                columnTypes[i].Update(value);
            }

            rowCount++;

            if (rowCount == inferMax)
            {
                break;
            }
        }

        return columnTypes;
    }
}