using System.Runtime.CompilerServices;
using Prequel.Data;
using Prequel.Execution;
using Prequel.Logical;
using Prequel.Metrics;

namespace Prequel.Engine.Source.Memory;

/// <summary>
/// Store and execution plan for reading in-memory data
/// </summary>
public class InMemoryDataTable(string tableName) : DataTable, IExecutionPlan
{
    private List<int> _projection = null!;
    private RecordBatch? _batch;

    public override string Name { get; } = tableName;

    /// <summary>
    /// Schema of the data stored in memory
    /// </summary>
    public override Schema Schema => _batch!.Schema;

    /// <summary>
    /// Reads the data from memory already stored in record batch format.
    /// Data is repartitioned to match the requested batching
    /// </summary>
    /// <param name="queryContext">Query queryContext</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Async enumerable record batches</returns>
    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        if (_batch == null)
        {
            yield break;
        }

        var clone = _batch.CloneBatch();
        clone.Project(_projection);

        using var step = queryContext.Profiler.Step("Execution Plan, In-memory data table, Repartition clone");

        await foreach (var batch in clone.Repartition(queryContext.BatchSize).ToAsyncEnumerable().WithCancellation(cancellation))
        {
            step.IncrementBatch(batch.RowCount);
            yield return batch;
        }
    }
    /// <summary>
    /// Data in memory may not be used entirely by downstream plans.  This
    /// scan operation sets the projection required for retrieving
    /// fragments of data required by subsequent execution plans.
    /// from memory.
    /// </summary>
    /// <param name="projection">Query projection</param>
    /// <returns>CSV Execution plan</returns>
    public override IExecutionPlan Scan(List<int> projection)
    {
        _projection = BuildProjection(projection);
        return this;
    }
    /// <summary>
    /// Adds a new batch containing data to be held temporarily in memory
    /// </summary>
    /// <param name="batch">Record batch to persist in memory</param>
    public void AddBatch(RecordBatch batch)
    {
        if (_batch == null)
        {
            _batch = batch;
        }
        else
        {
            _batch.Concat(batch);
        }
    }

    public string ToStringIndented(Indentation? indentation = null)
    {
        return "In-Memory Execution";
    }
}