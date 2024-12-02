using Prequel.Data;
using Prequel.Logical;
using Prequel.Metrics;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Prequel.Execution;

/// <summary>
/// Execution that uses an implied join columns
/// </summary>
/// <param name="Left">Execution plan on the left side of the join condition</param>
/// <param name="Right">Execution plan on the right side of the join condition</param>
internal record CrossJoinExecution(IExecutionPlan Left, IExecutionPlan Right) : JoinExecution, IExecutionPlan
{
    private Schema? _schema;

    public Schema Schema => _schema ??= new Schema(Left.Schema.Fields.Concat(Right.Schema.Fields).ToList());

    /// <summary>
    /// Executes a cross on the data output from the parent execution plan.  All data from the left
    /// side of the join is gathered and then compared to data from the right side of the join.  Only
    /// matching data from right side of the join is kept with the left side data.  New batches are
    /// produced for the joined data.
    /// </summary>
    /// <param name="queryContext">Execution query queryContext</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Async enumerable array of record batches</returns>
    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        var leftData = new RecordBatch(Left.Schema);

        using (var letJoinStep = queryContext.Profiler.Step("Execution Plan, Cross Join, Execute left"))
        {
            await foreach (var leftBatch in Left.ExecuteAsync(queryContext, cancellation))
            {
                letJoinStep.IncrementBatch(leftBatch.RowCount);
                leftData.Concat(leftBatch);
            }
        }

        using var rightJoinStep = queryContext.Profiler.Step("Execution Plan, Cross Join, Execute right");
        await foreach (var rightBatch in Right.ExecuteAsync(queryContext, cancellation))
        {
            rightJoinStep.IncrementBatch(rightBatch.RowCount);

            for (var i = 0; i < leftData.RowCount; i++)
            {
                var partialBatch = BuildBatch(leftData, rightBatch, i);
                yield return partialBatch;
            }
        }
    }
    /// <summary>
    /// Builds a record batch from cross joined left and right hand side data
    /// </summary>
    /// <param name="leftData">Left hand side with results to combine</param>
    /// <param name="rightData">Right hand side with results to combine</param>
    /// <param name="size">Size of the new record batch array after data is joined</param>
    /// <returns></returns>
    private RecordBatch BuildBatch(RecordBatch leftData, RecordBatch rightData, int size)
    {
        // Left side repeated lists
        var arrays = leftData.Results
            .Select(array => array.ToArrayOfSize(rightData.RowCount, size))
            .ToList();

        // Append right side lists
        arrays.AddRange(rightData.Results.Select(rightSideArray => rightSideArray.Values));

        return RecordBatch.TryNewWithLists(Schema, arrays);
    }

    public string ToStringIndented(Indentation? indentation = null)
    {
        var indent = indentation ?? new Indentation();
        return $"Cross Join: {indent.Next(Left)}{indent.Repeat(Right)}";
    }
}