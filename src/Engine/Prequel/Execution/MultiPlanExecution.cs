using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using Prequel.Data;
using Prequel.Logical;
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

    public string ToStringIndented(Indentation? indentation = null)
    {
        var indent = indentation ?? new Indentation();
        var builder = new StringBuilder("Multi-Plan Execution: ");

        for (var i = 0; i < childPlans.Count(); i++)
        {
            builder.Append(i == 0
                ? indent.Next(childPlans.Skip(i).First())
                : indent.Repeat(childPlans.Skip(i).First()));
        }

        return builder.ToString();
    }
}