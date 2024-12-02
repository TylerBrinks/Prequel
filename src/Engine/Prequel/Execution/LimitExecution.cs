using Prequel.Data;
using Prequel.Logical;
using Prequel.Metrics;
using System.Runtime.CompilerServices;

namespace Prequel.Execution;

/// <summary>
/// Execution limiting the number of output rows across the entire group of record batches
/// </summary>
/// <param name="Plan">Parent execution plan</param>
/// <param name="Skip">Number of rows to skip</param>
/// <param name="Fetch">Maximum number of rows to return</param>
internal record LimitExecution(IExecutionPlan Plan, int Skip, int Fetch) : IExecutionPlan
{
    public Schema Schema => Plan.Schema;

    /// <summary>
    /// Execution that limits the total number of records returned.
    /// </summary>
    /// <param name="queryContext">Execution query queryContext</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Async enumerable array of record batches</returns>
    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        var skip = Skip;
        var fetch = Fetch;

        var ignoreLimit = Skip == 0 && Fetch == int.MaxValue;

        using var step = queryContext.Profiler.Step("Execution Plan, Limit execution, Execute plan");

        await foreach (var batch in Plan.ExecuteAsync(queryContext, cancellation))
        {
            step.IncrementBatch(batch.RowCount);
            if (fetch == 0)
            {
                // Fetch satisfied; short circuit further iteration
                yield break;
            }

            if (ignoreLimit)
            {
                yield return batch;
            }

            var rowCount = batch.RowCount;

            if (rowCount <= skip)
            {
                skip -= rowCount;
                continue;
            }

            batch.Slice(skip, Math.Min(rowCount - skip, fetch));

            skip = 0;

            if (rowCount < fetch)
            {
                fetch -= batch.RowCount;
            }
            else
            {
                fetch = 0;
            }

            yield return batch;

            if (fetch == 0)
            {
                // Fetch satisfied; short circuit further iteration
                yield break;
            }
        }
    }

    public override string ToString()
    {
        return $"Limit Execution: Skip {Skip}, Limit {Fetch}";
    }

    public string ToStringIndented(Indentation? indentation = null)
    {
        var indent = indentation ?? new Indentation();
        return $"{this} {indent.Next(Plan)}";
    }
}