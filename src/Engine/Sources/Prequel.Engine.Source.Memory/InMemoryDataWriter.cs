using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Metrics;
using Prequel.Engine.IO;

namespace Prequel.Engine.Source.Memory;

/// <summary>
/// Data writer for in-memory data storage
/// </summary>
public class InMemoryDataWriter(string tableName, QueryContext queryContext) : IDataWriter
{
    private readonly InMemoryDataTable _table = new(tableName);

    /// <summary>
    /// Date source for memory-based queries
    /// </summary>
    public DataTable DataTable => _table;

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
        //TODO anything to clean up?
    }

    /// <summary>
    /// Writes a record batch to the data source
    /// </summary>
    /// <param name="records">Record batches to write</param>
    /// <param name="cancellation">Optional cancellation token</param>
    public async ValueTask WriteAsync(IAsyncEnumerable<RecordBatch> records, CancellationToken cancellation = default!)
    {
        using var step = queryContext.Profiler.Step("Data Writer, In-Memory, Write batches");

        await foreach (var batch in records.WithCancellation(cancellation))
        {
            _table.AddBatch(batch);
        }
    }

    public ValueTask WriteAsync(RecordBatch batch, CancellationToken cancellation = default)
    {
        throw new NotImplementedException();
    }
}