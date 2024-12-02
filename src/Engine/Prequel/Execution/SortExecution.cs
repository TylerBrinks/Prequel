using Prequel.Data;
using Prequel.Logical;
using Prequel.Metrics;
using Prequel.Physical.Expressions;
using System.Runtime.CompilerServices;

namespace Prequel.Execution;

/// <summary>
/// Execution plan that sorts data produced by the parent execution
/// </summary>
/// <param name="SortExpressions">List of expressions used to sort incoming data</param>
/// <param name="Plan">Parent execution plan</param>
internal record SortExecution(List<PhysicalSortExpression> SortExpressions, IExecutionPlan Plan) : IExecutionPlan
{
    /// <summary>
    /// Pass through to the plan's schema
    /// </summary>
    public Schema Schema => Plan.Schema;

    /// <summary>
    /// Sorts data within each batch for all batches produced by the parent execution plan.
    /// All data is collected and repartitioned prior to forwarding batches to child plans.
    /// </summary>
    /// <param name="queryContext">Execution query queryContext</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Async enumerable array of record batches</returns>
    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        RecordBatch sortedBatch;
        if (SortExpressions.Count == 1)
        {
            sortedBatch = await SortSingleColumnAsync(queryContext, cancellation);
        }
        else
        {
            sortedBatch = await SortColumnsAsync(queryContext, cancellation);
        }

        using var step = queryContext.Profiler.Step("Execution Plan, Sort, execute");

        foreach (var batch in sortedBatch.Repartition(queryContext.BatchSize))
        {
            step.IncrementBatch(batch.RowCount);
            yield return batch;
        }
    }

    /// <summary>
    /// Sorts a single column using data in all batches
    /// </summary>
    /// <param name="context">Execution query queryContext</param>
    /// <param name="cancellation">Cancellation token</param>
    /// <returns>Single record batch with sorted data</returns>
    private async Task<RecordBatch> SortSingleColumnAsync(QueryContext context, CancellationToken cancellation)
    {
        var collectedBatch = await CoalesceBatchesAsync(context, cancellation);
        var sortExpression = SortExpressions[0];
        var sortColumn = (Column)sortExpression.Expression;

        var array = collectedBatch.Results[sortColumn.Index];

        var indices = array.GetSortIndices(sortExpression.Ascending);
        collectedBatch.Reorder(indices);

        return collectedBatch;
    }

    /// <summary>
    /// Sorts against multiple columns using data in all batches
    /// </summary>
    /// <param name="context">Execution query queryContext</param>
    /// <param name="cancellation">Cancellation token</param>
    /// <returns>Single record batch with sorted data</returns>
    private async Task<RecordBatch> SortColumnsAsync(QueryContext context, CancellationToken cancellation)
    {
        var sortColumns = SortExpressions.Select(sc => (Column)sc.Expression).ToList();
        // The first column will always be sorted in full.  Proactively 
        // sorting here makes it simpler to build an index list of
        // columns that can be skipped in subsequent sorting passes
        var batch = await SortSingleColumnAsync(context, cancellation);

        // Skip the first column; it has been sorted already
        var indicesToIgnore = new List<int> { sortColumns[0].Index };

        for (var sortIndex = 1; sortIndex < sortColumns.Count; sortIndex++)
        {
            // Sorting multiple columns must preserve the order of previously 
            // sorted columns.  To do so, the previous column is used to construct
            // a distinct list of values, which is effectively an array-based way
            // of doing a GroupBy operation.  These values represent the boundary
            // for each sub-group that needs to be sorted.
            // For example, if a previous column sort yields values
            // 'a', 'a', 'a', 'b, 'b', 'c', 'c', 'c'
            // The subsequent sort will sort all items with values related
            // to 'a', then all values related to 'b', and finally 'c.'  This
            // preserves the previous sort order akin to OrderBy().ThenBy()... etc.
            var previousSortColumn = batch.Results[sortColumns[sortIndex - 1].Index];
            var array = batch.Results[sortColumns[sortIndex].Index];

            // Get the index boundaries for the subsequent sort groups
            var distinctValueIndices = previousSortColumn.Values.Cast<object>()
                .ToList()
                .Distinct()
                .Select(previousSortColumn.Values.IndexOf)
                .ToArray();

            var groupCount = distinctValueIndices.Length;

            // Store the sort indices for each sub-group.  They will
            // be applied in one operation instead of multiple passes.
            var reorderedIndices = new List<int>();

            for (var groupIndex = 0; groupIndex < groupCount; groupIndex++)
            {
                // 0 in the case it's the first pass or the whole column
                var start = groupIndex == 0 ? 0 : distinctValueIndices[groupIndex];
                // End boundary or end of list; whichever comes first
                var end = groupIndex == groupCount - 1 ? previousSortColumn.Values.Count : distinctValueIndices[groupIndex + 1];
                var groupSize = end - start;

                var ascending = SortExpressions[sortIndex].Ascending;
                var indices = array.GetSortIndices(ascending, start, groupSize)
                    // Add the boundary offset to each set of indices so 
                    // the full array can be reordered in one operation
                    .Select(i => i + start)
                    .ToList();

                reorderedIndices.AddRange(indices);
            }
            // Reorder all arrays that have not been sorted
            batch.Reorder(reorderedIndices, indicesToIgnore);
            // This column has been sorted; track to prevent re-sorting 
            indicesToIgnore.Add(sortColumns[sortIndex].Index);
        }

        return batch;
    }
    /// <summary>
    /// Polls the input plan for all batches and merges data
    /// from all batches into a single batch 
    /// </summary>
    /// <param name="context">Execution query queryContext</param>
    /// <param name="cancellation">Cancellation token</param>
    /// <returns>Awaitable record batch</returns>
    private async Task<RecordBatch> CoalesceBatchesAsync(QueryContext context, CancellationToken cancellation)
    {
        var collectedBatch = new RecordBatch(Plan.Schema);

        await foreach (var batch in Plan.ExecuteAsync(context, cancellation))
        {
            for (var i = 0; i < batch.Results.Count; i++)
            {
                foreach (var value in batch.Results[i].Values)
                {
                    collectedBatch.AddResult(i, value);
                }
            }
        }

        return collectedBatch;
    }

    public string ToStringIndented(Indentation? indentation = null)
    {
        var indent = indentation ?? new Indentation();
        var orders = string.Join(",", SortExpressions.Select(o => o.ToString()?
            .Replace("Order By", string.Empty, StringComparison.InvariantCultureIgnoreCase)));
        return $"Sort Execution: {orders}{indent.Next(Plan)}";
    }
}