using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;
using Prequel.Data;
using Prequel.Logical;
using Prequel.Metrics;

namespace Prequel.Execution;
/// <summary>
/// Execution plan that executes multiple queries into a single set of batches
/// </summary>
/// <param name="Plans">List of plans to execute</param>
/// <param name="Schema">Schema for all plans under execution</param>
internal record UnionExecution(List<IExecutionPlan> Plans, Schema Schema) : IExecutionPlan
{
    /// <summary>
    /// Executes all plans in parallel and continues with
    /// batch output as a standard execution plan.
    /// </summary>
    /// <param name="queryContext">Execution query queryContext</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Async enumerable array of record batches</returns>
    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        var batches = new ConcurrentQueue<RecordBatch>();

        // Execute batches in parallel.  Cannot yield here, but the 
        // operation can still be done in parallel preserving the
        // order of each batch as its returned
        await Parallel.ForEachAsync(Plans, cancellation, async (plan, token) =>
        {
            using var step = queryContext.Profiler.Step("Execution Plan, Union, execute");

            await foreach (var batch in plan.ExecuteAsync(queryContext, token))
            {
                step.IncrementBatch(batch.RowCount);

                batches.Enqueue(batch);
            }
        });

        foreach (var batch in batches)
        {
            yield return batch;
        }
    }

    public string ToStringIndented(Indentation? indentation = null)
    {
        var indent = indentation ?? new Indentation();
        var builder = new StringBuilder("Union Execution: ");

        for (var i = 0; i < Plans.Count; i++)
        {
            builder.Append(i == 0
                ? indent.Next(Plans.Skip(i).First())
                : indent.Repeat(Plans.Skip(i).First()));
        }

        return builder.ToString();
    }
}