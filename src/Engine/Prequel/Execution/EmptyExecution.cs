using System.Runtime.CompilerServices;
using Prequel.Data;

namespace Prequel.Execution;

internal record EmptyExecution(Schema Schema) : IExecutionPlan
{
    /// <summary>
    /// Placeholder execution used for literal expressions etc.
    /// </summary>
    /// <param name="queryContext">Execution query queryContext</param>
    /// <param name="cancellation">Optional cancellation token.</param>
    /// <returns>Async enumerable array of record batches</returns>
    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        await Task.CompletedTask;

        var batch = new RecordBatch(Schema);

        batch.AddResultArray(new BooleanArray());
        batch.AddResult(0, true);

        yield return batch;
    }
}