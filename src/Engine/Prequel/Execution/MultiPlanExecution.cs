using System.Runtime.CompilerServices;
using Prequel.Data;
using Prequel.Metrics;

namespace Prequel.Execution;

public class MultiPlanExecution(IEnumerable<IExecutionPlan> childPlans, Schema schema) : IExecutionPlan
{
    public Schema Schema { get; } = schema;

    /// <summary>
    /// Reads data from all child plans and passes through record
    /// batch instances yielded by each child plan.
    /// </summary>
    /// <param name="queryContext">Query queryContext</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Async enumerable record batches</returns>
    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        foreach (var plan in childPlans)
        {
            using var step = queryContext.Profiler.Step("Execution Pla, Multi-source Plan, Execute plan");

            await foreach (var batch in plan.ExecuteAsync(queryContext, cancellation))
            {
                step.IncrementBatch(batch.RowCount);

                yield return batch;
            }
        }
    }
}