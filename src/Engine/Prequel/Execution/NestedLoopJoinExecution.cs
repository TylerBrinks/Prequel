using Prequel.Data;
using Prequel.Logical;
using Prequel.Metrics;
using Prequel.Physical.Joins;
using System.Runtime.CompilerServices;

namespace Prequel.Execution;

/// <summary>
/// Execution used for a query contains a nested select that can be treated as a join.
///
/// <code>
///     SELECT * 
///     FROM table1 alias where col_1 = (
///         SELECT AVG(col_1)
///     FROM table1
///     WHERE col_1 = alias.col_1
/// </code>
/// </summary>
/// <param name="Left">Left side join plan</param>
/// <param name="Right">Right side join plan</param>
/// <param name="Filter">Optional join filter</param>
/// <param name="JoinType">Join type</param>
/// <param name="ColumnIndices">Join column indices</param>
/// <param name="Schema">Schema containing all join fields</param>
internal record NestedLoopJoinExecution(
    IExecutionPlan Left,
    IExecutionPlan Right,
    JoinFilter? Filter,
    JoinType JoinType,
    List<JoinColumnIndex> ColumnIndices,
    Schema Schema) : JoinExecution, IExecutionPlan
{
    /// <summary>
    /// True for Right, Right-Semi, Right-Anti, and Full joins; otherwise false.
    /// </summary>
    internal bool LeftIsBuildSide => JoinType is JoinType.Right
        or JoinType.RightSemi
        or JoinType.RightAnti
        or JoinType.Full;

    /// <summary>
    /// Executes a looping join
    /// </summary>
    /// <param name="queryContext">Execution query queryContext</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Async enumerable array of record batches</returns>
    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        if (LeftIsBuildSide)
        {
            // Left side builds require a single left batch
            var leftMerged = new RecordBatch(Left.Schema);

            using (var leftStep = queryContext.Profiler.Step("Execution Plan, Nested Loop, Execute left"))
            {
                await foreach (var leftBatch in Left.ExecuteAsync(queryContext, cancellation))
                {
                    leftStep.IncrementBatch(leftBatch.RowCount);

                    leftMerged.Concat(leftBatch);
                }
            }

            // Bitmap for a full join
            var visitedLeftSide = JoinType == JoinType.Full
                ? new bool[leftMerged.RowCount]
                : [];

            using (var rightStep = queryContext.Profiler.Step("Execution Plan, Nested Loop, Execute right"))
            {
                await foreach (var rightBatch in Right.ExecuteAsync(queryContext, cancellation))
                {
                    rightStep.IncrementBatch(rightBatch.RowCount);

                    var intermediate = JointLeftAndRightBatch(leftMerged, rightBatch, ColumnIndices, visitedLeftSide);
                    yield return intermediate;
                }
            }

            if (JoinType != JoinType.Full)
            {
                yield break;
            }

            var (finalLeft, finalRight) = GetFinalIndices(visitedLeftSide, JoinType.Full);

            var emptyBatch = new RecordBatch(Left.Schema);
            var finalBatch = BuildBatchFromIndices(Schema, leftMerged, emptyBatch, finalLeft, finalRight, ColumnIndices, JoinSide.Left);
            yield return finalBatch;
        }
        else
        {
            using var rightStep = queryContext.Profiler.Step("Execution Plan, Nested Loop, Right build, execute right");

            await foreach (var rightBatch in Right.ExecuteAsync(queryContext, cancellation))
            {
                rightStep.IncrementBatch(rightBatch.RowCount);

                using var leftStep = queryContext.Profiler.Step("Execution Plan, Nested Loop, Right build, execute left");

                await foreach (var leftBatch in Left.ExecuteAsync(queryContext, cancellation))
                {
                    leftStep.IncrementBatch(leftBatch.RowCount);

                    yield return JointLeftAndRightBatch(leftBatch, rightBatch, ColumnIndices, []);
                }
            }
        }
    }
    /// <summary>
    /// Joins left and right record batches into a single record batch with joined data
    /// </summary>
    /// <param name="leftBatch">Left side record batch data</param>
    /// <param name="rightBatch">Right side record batch data</param>
    /// <param name="columnIndices">Join column indices</param>
    /// <param name="visitedLeftSide">Array of visited left side indices</param>
    /// <returns></returns>
    private RecordBatch JointLeftAndRightBatch(
        RecordBatch leftBatch,
        RecordBatch rightBatch,
        List<JoinColumnIndex> columnIndices,
        bool[] visitedLeftSide)
    {
        var indicesResult = Enumerable.Range(0, leftBatch.RowCount).ToList()
            .Select(leftRowIndex => BuildJoinIndices(leftRowIndex, rightBatch, leftBatch)).ToList();

        var leftIndices = new List<long>();
        var rightIndices = new List<long>();

        foreach (var (leftIndicesResult, rightIndicesResult) in indicesResult)
        {
            leftIndices.AddRange(leftIndicesResult);
            rightIndices.AddRange(rightIndicesResult);
        }

        if (JoinType == JoinType.Full)
        {
            foreach (var leftIndex in leftIndices)
            {
                visitedLeftSide[leftIndex] = true;
            }
        }

        var (leftSide, rightSide) = AdjustIndicesByJoinType(leftIndices.ToArray(), rightIndices.ToArray(), leftBatch.RowCount, rightBatch.RowCount, JoinType);

        return BuildBatchFromIndices(Schema, leftBatch, rightBatch, leftSide, rightSide, columnIndices, JoinSide.Left);
    }
    /// <summary>
    /// Builds left and right join indices from a pair of left and right side record batches.
    /// </summary>
    /// <param name="leftRowIndex">>eft row indices</param>
    /// <param name="rightBatch">Left row join indices</param>
    /// <param name="leftBatch">Right row join indices</param>
    /// <returns></returns>
    private (long[] LeftIndices, long[] RightIndices) BuildJoinIndices(int leftRowIndex, RecordBatch rightBatch, RecordBatch leftBatch)
    {
        var rightRowCount = rightBatch.RowCount;
        var leftIndices = Enumerable.Repeat(leftRowIndex, rightRowCount).Select(i => (long)i).ToArray();
        var rightIndices = Enumerable.Range(0, rightRowCount).Select(i => (long)i).ToArray();

        return Filter != null
            ? ApplyJoinFilterToIndices(leftBatch, rightBatch, leftIndices, rightIndices, Filter, JoinSide.Left)
            : (leftIndices, rightIndices);
    }
    /// <summary>
    /// Adjust the indices based on the type of join operation
    /// </summary>
    /// <param name="leftIndices">Left side join indices</param>
    /// <param name="rightIndices">Right side join indices</param>
    /// <param name="leftBatchRowCount">Number of records on the left side batch</param>
    /// <param name="rightBatchRowCount">umber of records on the left side batch</param>
    /// <param name="joinType">Join type operation</param>
    /// <returns>Adjusted left and right side join indices</returns>
    protected static (long?[] LeftIndices, long?[] RightIndices) AdjustIndicesByJoinType(
        long[] leftIndices,
        long[] rightIndices,
        int leftBatchRowCount,
        int rightBatchRowCount,
        JoinType joinType)
    {
        return joinType switch
        {
            JoinType.Inner => (leftIndices.AsNullable(), rightIndices.AsNullable()),
            JoinType.Left => AppendLeftIndices(leftIndices, rightIndices, leftIndices.GetAntiIndices(leftBatchRowCount)),
            JoinType.LeftSemi => (leftIndices.GetSemiIndices(leftBatchRowCount).AsNullable(), rightIndices.AsNullable()),
            JoinType.LeftAnti => (leftIndices.GetAntiIndices(leftBatchRowCount).AsNullable(), rightIndices.AsNullable()),
            JoinType.Right or JoinType.Full => AppendRightIndices(leftIndices, rightIndices, rightIndices.GetAntiIndices(rightBatchRowCount)),
            JoinType.RightSemi => (leftIndices.AsNullable(), rightIndices.GetSemiIndices(rightBatchRowCount).AsNullable()),
            JoinType.RightAnti => (leftIndices.AsNullable(), rightIndices.GetAntiIndices(rightBatchRowCount).AsNullable()),

            _ => throw new NotImplementedException("AdjustIndicesByJoinType Implement join type")
        };
    }

    public string ToStringIndented(Indentation? indentation = null)
    {
        var indent = indentation ?? new Indentation();
        return $"Nested Loop Join: {indent.Next(Left)}{indent.Repeat(Right)}";
    }
}