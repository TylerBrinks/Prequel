using Prequel.Data;
using Prequel.Metrics;
using Prequel.Physical.Expressions;
using Prequel.Physical.Joins;
using Prequel.Values;
using System.Runtime.CompilerServices;

namespace Prequel.Execution;

/// <summary>
/// Hash join algorithm for fast, efficient aggregation operations.  Unlike non-grouping
/// aggregations, hashed aggregations maintain a list of key and value pairs where the
/// key represents the groups and values can be created or appending as each aggregate
/// is calculated  
/// </summary>
/// <param name="Left">Aggregation left hand execution plan</param>
/// <param name="Right">Aggregation right hand execution plan</param>
/// <param name="On">List of join on clauses</param>
/// <param name="Filter">Optional join filter</param>
/// <param name="JoinType">Join type flag</param>
/// <param name="ColumnIndices">Index values of the columns being joined</param>
/// <param name="NullEqualsNull"></param>
/// <param name="Schema">Schema containing all join field details</param>
internal record HashJoinExecution(
    IExecutionPlan Left,
    IExecutionPlan Right,
    List<JoinOn> On,
    JoinFilter? Filter,
    JoinType JoinType,
    //PartitionMode PartitionMode,
    List<JoinColumnIndex> ColumnIndices,
    bool NullEqualsNull, //TODO: unused
    Schema Schema) : JoinExecution, IExecutionPlan
{
    /// <summary>
    /// Executes a hash join against all data in the left and right execution plan batches
    /// </summary>
    /// <param name="queryContext">Execution query queryContext</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Async enumerable array of record batches</returns>
    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        var onLeft = On.Select(j => j.Left).ToList();
        var onRight = On.Select(j => j.Right).ToList();

        //if (PartitionMode == PartitionMode.CollectLeft)
        //{
        var leftData = await CollectLeftAsync(queryContext, onLeft);

        // These join type need the bitmap to identify which row has be matched or unmatched.
        // For the `left semi` join, need to use the bitmap to produce the matched row on the left side
        // For the `left` join, need to use the bitmap to produce the unmatched row on the left side with null
        // For the `left anti` join, need to use the bitmap to produce the unmatched row on the left side
        // For the `full` join, need to use the bitmap to produce the unmatched row on the left side with null
        var visitedLeftSide = NeedProduceResultInFinal(JoinType)
            ? new bool[leftData.Batch.RowCount]
            : [];

        using (var rightStep = queryContext.Profiler.Step("Execution Plan, Hash Join, Execute right"))
        {
            await foreach (var rightBatch in Right.ExecuteAsync(queryContext, cancellation))
            {
                rightStep.IncrementBatch(rightBatch.RowCount);

                if (rightBatch.RowCount > 0)
                {
                    var (leftIndies, rightIndices) =
                        BuildJoinIndices(rightBatch, leftData, onLeft, onRight, Filter, 0, JoinSide.Left);

                    // Set the left bitmap
                    // Only left, full, left semi, left anti need the left bitmap
                    if (NeedProduceResultInFinal(JoinType))
                    {
                        foreach (var index in leftIndies)
                        {
                            visitedLeftSide[(int)index] = true;
                        }
                    }

                    var (leftSide, rightSide) = AdjustIndicesByJoinType(leftIndies, rightIndices,
                        rightBatch.RowCount, JoinType);

                    var batch = BuildBatchFromIndices(Schema, leftData.Batch, rightBatch,
                        leftSide, rightSide, ColumnIndices, JoinSide.Left);

                    if (batch.RowCount > 0)
                    {
                        yield return batch;
                    }
                }
                //else
                //{
                //    if (NeedProduceResultInFinal(JoinType)) //TODO: && isExhausted
                //    {
                //        var (leftSide, rightSide) = GetFinalIndices(visitedLeftSide, JoinType);
                //        var rightEmptyBatch = new RecordBatch(Right.Schema);

                //        yield return BuildBatchFromIndices(Schema, leftData.Batch, rightEmptyBatch,
                //            leftSide, rightSide, ColumnIndices, JoinSide.Left);
                //    }
                //}
            }
        }

        if (NeedProduceResultInFinal(JoinType)) //TODO: && isExhausted
        {
            var (leftSide, rightSide) = GetFinalIndices(visitedLeftSide, JoinType);
            var rightEmptyBatch = new RecordBatch(Right.Schema);

            yield return BuildBatchFromIndices(Schema, leftData.Batch, rightEmptyBatch,
                leftSide, rightSide, ColumnIndices, JoinSide.Left);
        }
    }
    /// <summary>
    /// Checks if a Final result must be immediately produced based on the type of join
    /// </summary>
    /// <param name="joinType">True if a final result can be produced; otherwise false</param>
    /// <returns>True if a final result must be produced.</returns>
    private static bool NeedProduceResultInFinal(JoinType joinType)
    {
        return joinType is JoinType.Left or JoinType.LeftAnti or JoinType.LeftSemi or JoinType.Full;
    }
    /// <summary>
    /// Gets the right and left side join index values based on join type
    /// </summary>
    /// <param name="leftIndices">Left side index values</param>
    /// <param name="rightIndices">Right side index values</param>
    /// <param name="countRightBatch">Semi or Anti Join batch count</param>
    /// <param name="joinType">Join type</param>
    /// <returns>Left and Right side join index arrays</returns>
    private static (long?[] LeftIndices, long?[] RightIndices) AdjustIndicesByJoinType(
        long[] leftIndices,
        long[] rightIndices,
        int countRightBatch,
        JoinType joinType)
    {
#pragma warning disable CS8524
        return joinType switch
#pragma warning restore CS8524
        {
            JoinType.Inner or JoinType.Left => (leftIndices.AsNullable(), rightIndices.AsNullable()),

            // Unmatched right row will be produced in this batch
            // Combine the matched and unmatched right result together
            JoinType.Right or JoinType.Full => AppendRightIndices(leftIndices, rightIndices, rightIndices.GetAntiIndices(countRightBatch)),

            // Remove duplicated records on the right side
            JoinType.RightSemi => (leftIndices.AsNullable(), rightIndices.GetSemiIndices(countRightBatch).AsNullable()),

            // Remove duplicated records on the right side
            JoinType.RightAnti => (leftIndices.AsNullable(), rightIndices.GetAntiIndices(countRightBatch).AsNullable()),

            // matched or unmatched left row will be produced in the end of loop
            // When visit the right batch, we can output the matched left row and don't need to wait the end of loop
            JoinType.LeftSemi or JoinType.LeftAnti => (Array.Empty<long?>(), Array.Empty<long?>())
        };
    }
    /// <summary>
    /// Collects all left side data into a batch and builds a join map
    /// that will act as hash keys for the right side join data.
    /// </summary>
    /// <param name="context">Execution query queryContext</param>
    /// <param name="onLeft">Column collection for creating or updating the hash map</param>
    /// <returns>Left side join data</returns>
    private async Task<JoinLeftData> CollectLeftAsync(QueryContext context, IReadOnlyCollection<Column> onLeft)
    {
        // Left side builds require a single batch
        var joinBatch = new RecordBatch(Left.Schema);

        var offset = 0;
        var joinMap = new JoinMap();

        using var step = context.Profiler.StepInner("Hash join collect left data");

        await foreach (var batch in Left.ExecuteAsync(context))
        {
            var rowCount = batch.RowCount;
            step.IncrementBatch(rowCount);

            var hashBuffer = new int[rowCount];
            UpdateHash(onLeft, batch, joinMap, offset, hashBuffer);

            offset += batch.RowCount;

            joinBatch.Concat(batch);
        }

        return new JoinLeftData(joinMap, joinBatch);
    }
    /// <summary>
    /// Updates the hash map for a given record batch
    /// </summary>
    /// <param name="on">Columns involved in the join's ON clause</param>
    /// <param name="batch">Batch with data to assign to a hash key</param>
    /// <param name="hashMap">The join's hash map</param>
    /// <param name="offset">Location in the ash map where new values need to be added</param>
    /// <param name="hashValues">Hash value array</param>
    private static void UpdateHash(IEnumerable<Column> on, RecordBatch batch, JoinMap hashMap, int offset, int[] hashValues)
    {
        var keyValues = on.Select(c => c.Evaluate(batch)).ToList();

        CreateHashes(keyValues, hashValues);

        for (var row = 0; row < hashValues.Length; row++)
        {
            var hashValue = hashValues[row];
            var found = hashMap.TryGetValue(hashValue, out var list);

            if (found)
            {
                list!.Add(row + offset);
            }
            else
            {
                hashMap.Add(hashValue, [row + offset]);
            }
        }
    }
    /// <summary>
    /// Creates a hash from the output of a column's values
    /// </summary>
    /// <param name="columnValues">Column value array</param>
    /// <param name="hashBuffer">Buffer to fill with hash values</param>
    private static void CreateHashes(IReadOnlyCollection<ColumnValue> columnValues, IList<int> hashBuffer)
    {
        for (var i = 0; i < hashBuffer.Count; i++)
        {
            var values = columnValues.Select(v => v.GetValue(i)).Where(v => v != null).ToList();
            var hash = new HashCode();

            foreach (var value in values)
            {
                hash.Add(value);
            }

            hashBuffer[i] = hash.ToHashCode();
        }
    }
    /// <summary>
    /// Builds a list of column index positions involved in a join
    /// </summary>
    /// <param name="probeBatch">Batch to prove for join index values</param>
    /// <param name="joinData">Left side join data record batch</param>
    /// <param name="onBuild">Join ON clause build columns</param>
    /// <param name="onProbe">Join ON clause probe columns</param>
    /// <param name="filter">Optional join filter</param>
    /// <param name="offset">Offset (currently always zero)</param>
    /// <param name="joinSide">Specified Left or Right join mode</param>
    /// <returns>Left and Right index values</returns>
    private static (long[] LeftIndies, long[] RightIndices) BuildJoinIndices(
        RecordBatch probeBatch,
        JoinLeftData joinData,
        IEnumerable<Column> onBuild,
        IEnumerable<Column> onProbe,
        JoinFilter? filter,
        int offset,
        JoinSide joinSide
    )
    {
        var buildInputBuffer = joinData.Batch;

        var (buildIndices, probeIndices) = BuildEqualConditionJoinIndices(joinData.JoinMap,
            buildInputBuffer, probeBatch, onBuild, onProbe, offset);

        if (filter != null)
        {
            return ApplyJoinFilterToIndices(buildInputBuffer, probeBatch, buildIndices, probeIndices, filter, joinSide);
        }

        return (buildIndices, probeIndices);
    }
    /// <summary>
    /// Returns build/probe indices satisfying the equality condition.
    /// On LEFT.b1 = RIGHT.b2
    /// LEFT Table:
    ///  a1  b1  c1
    ///  1   1   10
    ///  3   3   30
    ///  5   5   50
    ///  7   7   70
    ///  9   8   90
    ///  11  8   110
    ///  13  10  130
    /// RIGHT Table:
    ///  a2   b2  c2
    ///  2    2   20
    ///  4    4   40
    ///  6    6   60
    ///  8    8   80
    /// 10   10  100
    /// 12   10  120
    /// The result is
    /// +----+----+-----+----+----+-----+
    /// | a1 | b1 | c1  | a2 | b2 | c2  |
    /// +----+----+-----+----+----+-----+
    /// | 11 | 8  | 110 | 8  | 8  | 80  |
    /// | 13 | 10 | 130 | 10 | 10 | 100 |
    /// | 13 | 10 | 130 | 12 | 10 | 120 |
    /// | 9  | 8  | 90  | 8  | 8  | 80  |
    /// +----+----+-----+----+----+-----+
    /// And the result of build and probe indices are:
    /// Build indices:  5, 6, 6, 4
    /// Probe indices: 3, 4, 5, 3
    /// </summary>
    /// <param name="buildHashmap">Join hash map</param>
    /// <param name="buildInputBuffer">Record bach with input data</param>
    /// <param name="probeBatch">Record batch to probe for matches</param>
    /// <param name="onBuild">Join ON clause build columns</param>
    /// <param name="onProbe">Join ON clause probe columns</param>
    /// <param name="offset">Hash map index offset</param>
    /// <returns>Build and Probe index arrays</returns>
    private static (long[] BuildIndices, long[] ProbeIndices) BuildEqualConditionJoinIndices(
        JoinMap buildHashmap,
        RecordBatch buildInputBuffer,
        RecordBatch probeBatch,
        IEnumerable<Column> onBuild,
        IEnumerable<Column> onProbe,
        int offset)
    {
        var keysValues = onProbe.Select(c => c.Evaluate(probeBatch)).ToList();
        var buildJoinValues = onBuild.Select(c => c.Evaluate(buildInputBuffer)).ToList();

        var hashBuffer = new int[probeBatch.RowCount];
        CreateHashes(keysValues, hashBuffer);

        var buildIndices = new List<long>();
        var probeIndices = new List<long>();

        for (var row = 0; row < hashBuffer.Length; row++)
        {
            var hashValue = hashBuffer[row];
            // Get the hash and find it in the build index

            // For every item on the build and probe we check if it matches
            // This possibly contains rows with hash collisions,
            // So we have to check here whether rows are equal or not

            if (!buildHashmap.TryGetValue(hashValue, out var indices))
            {
                continue;
            }

            foreach (var offsetBuildIndex in indices.Select(i => i - offset)
                         .Where(offsetBuildIndex => EqualRows(offsetBuildIndex, row, buildJoinValues, keysValues)))
            {
                buildIndices.Add(offsetBuildIndex);
                probeIndices.Add(row);
            }
        }

        return (buildIndices.ToArray(), probeIndices.ToArray());
    }

    /// <summary>
    /// Left and right row have equal values
    /// If more data types are supported here, please also add the data types in can_hash function
    /// to generate hash join logical plan.
    /// </summary>
    /// <param name="left">Left row index</param>
    /// <param name="right">Right row index</param>
    /// <param name="leftArrays">Left array column values</param>
    /// <param name="rightArrays">Right array column values</param>
    /// <returns>True if the rows are equal; otherwise false.</returns>
    private static bool EqualRows(int left, int right, IEnumerable<ColumnValue> leftArrays, IEnumerable<ColumnValue> rightArrays)
    {
        return leftArrays.Zip(rightArrays).All(c =>
            {
                //todo: !! nulls equal
                //if left == null && right == null && null_equals_null

                var leftValue = c.First.GetValue(left);
                var rightValue = c.Second.GetValue(right);

                var equal = leftValue.CompareValueEquality(rightValue);
                return equal;
            }
        );
    }
}
