using System.Collections;
using Prequel.Engine.Core;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Physical.Joins;
using Prequel.Engine.Core.Values;

namespace Prequel.Engine.Core.Execution;

/// <summary>
/// Join map serving as a hash key and list of index values for join operations
/// </summary>
internal class JoinMap : Dictionary<int, List<int>>;

/// <summary>
/// Left-side join data used for hash join execution
/// </summary>
/// <param name="JoinMap">Left side join hash map</param>
/// <param name="Batch">Batch containing left side join data</param>
internal record JoinLeftData(JoinMap JoinMap, RecordBatch Batch);

/// <summary>
/// Join execution base class
/// </summary>
internal abstract record JoinExecution
{
    /// <summary>
    /// Applies a filter to join data preserving only data tha matches the join criteria.
    /// </summary>
    /// <param name="buildInputBuffer">Build side input record batch</param>
    /// <param name="probeBatch">Probe side data record batch</param>
    /// <param name="buildIndices">Build side index array</param>
    /// <param name="probeIndices">Probe side index array</param>
    /// <param name="filter">Join filter expression to evaluate against the created incremental batch</param>
    /// <param name="buildSide">Specifies a Left or Right join</param>
    /// <returns>Array of left and right filtered index values</returns>
    protected static (long[] Left, long[] Right) ApplyJoinFilterToIndices(
        RecordBatch buildInputBuffer,
        RecordBatch probeBatch,
        long[] buildIndices,
        long[] probeIndices,
        JoinFilter filter,
        JoinSide buildSide)
    {
        if (!buildIndices.Any() && !probeIndices.Any())
        {
            return (buildIndices, probeIndices);
        }

        var intermediateBatch = BuildBatchFromIndices(
            filter.Schema,
            buildInputBuffer,
            probeBatch,
            buildIndices.AsNullable(),
            probeIndices.AsNullable(),
            filter.ColumnIndices,
            buildSide);

        var mask = ((BooleanColumnValue)filter.FilterExpression.Evaluate(intermediateBatch)).Values;

        var leftFiltered = buildIndices.Where((_, index) => mask[index]!.Value).ToArray();
        var rightFiltered = probeIndices.Where((_, index) => mask[index]!.Value).ToArray();

        return (leftFiltered, rightFiltered);
    }
    /// <summary>
    /// Builds a batch from a given schema and a combination of build and probe record batch data.
    /// The operation is identical but inverted for left or right joins
    /// </summary>
    /// <param name="schema">Schema containing all join fields</param>
    /// <param name="buildInputBuffer">Input build side record batch</param>
    /// <param name="probeBatch">Probe side record batch</param>
    /// <param name="buildIndices">Build side indices</param>
    /// <param name="probeIndices">Probe side indices</param>
    /// <param name="columnIndices">List of join column index values</param>
    /// <param name="buildSide">Specifies a Left or Right join</param>
    /// <returns>Record batch build from index values</returns>
    protected static RecordBatch BuildBatchFromIndices(
        Schema schema,
        RecordBatch buildInputBuffer,
        RecordBatch probeBatch,
        IReadOnlyCollection<long?> buildIndices,
        IReadOnlyCollection<long?> probeIndices,
        List<JoinColumnIndex> columnIndices,
        JoinSide buildSide)
    {
        if (!schema.Fields.Any())
        {
            return new RecordBatch(schema);
        }

        var columns = new List<IList>(schema.Fields.Count);

        foreach (var columnIndex in columnIndices)
        {
            IList array;

            if (columnIndex.JoinSide == buildSide)
            {
                var recordArray = buildInputBuffer.Results[columnIndex.Index];

                if (recordArray.Values.Count == 0 || buildIndices.NullCount() == buildIndices.Count)
                {
                    array = recordArray.NewEmpty(buildIndices.Count).Values;
                }
                else
                {
                    array = buildIndices.Select(i => i == null ? null : recordArray.Values[(int)i]).ToList();
                }
            }
            else
            {
                var recordArray = probeBatch.Results[columnIndex.Index];
                if (recordArray.Values.Count == 0 || probeIndices.NullCount() == probeIndices.Count)
                {
                    array = recordArray.NewEmpty(probeIndices.Count).Values;
                }
                else
                {
                    array = probeIndices.Select(i => i == null ? null : recordArray.Values[(int)i]).ToList();
                }
            }

            columns.Add(array);
        }

        return RecordBatch.TryNewWithLists(schema, columns);
    }
    /// <summary>
    /// Gets the final join index values based on the input mask
    /// </summary>
    /// <param name="mask">Bit mask to build a final list of index values.  For LeftSemi joins, the
    /// mask is checked against the negated mask value.</param>
    /// <param name="joinType">Specifies the join type operation</param>
    /// <returns>Array of left and right filtered index values</returns>
    internal static (long?[] FinalLeft, long?[] FinalRight) GetFinalIndices(bool[] mask, JoinType joinType)
    {
        long[] leftIndices;

        if (joinType == JoinType.LeftSemi)
        {
            leftIndices = Enumerable.Range(0, mask.Length)
                .Select(index => ((long)index, mask[index]))
                .Where(i => i.Item2)
                .Select(i => i.Item1)
                .ToArray();
        }
        else
        {
            // Left, LeftAnti, and Full
            leftIndices = Enumerable.Range(0, mask.Length)
                .Select(index => ((long)index, mask[index]))
                .Where(i => !i.Item2)
                .Select(i => i.Item1)
                .ToArray();
        }

        return (leftIndices.AsNullable(), new long?[leftIndices.Length]);
    }
    /// <summary>
    /// Appends unmatched left index values to the list of matched
    /// left index values and fills the right index list with null 
    /// values to keep the length of both index lists consistent.
    /// </summary>
    /// <param name="leftIndices">Left side index array</param>
    /// <param name="rightIndices">Right side index array</param>
    /// <param name="leftUnmatchedIndices">Unlatched left side index list</param>
    /// <returns>Left and Right side indices</returns>
    internal static (long?[] LeftIndices, long?[] RightIndices) AppendLeftIndices(
        long[] leftIndices, long[] rightIndices, IReadOnlyCollection<long> leftUnmatchedIndices)
    {
        var unmatchedSize = leftUnmatchedIndices.Count;
        if (unmatchedSize == 0)
        {
            return (leftIndices.AsNullable(), rightIndices.AsNullable());
        }

        var newLeftIndices = leftIndices.Concat(leftUnmatchedIndices).AsNullable();
        var newRightIndices = rightIndices.AsNullable().Concat(new long?[unmatchedSize]).ToArray();

        return (newLeftIndices, newRightIndices);
    }
    /// <summary>
    /// Appends unmatched right index values to the list of matched
    /// right index values and fills the left index list with null 
    /// values to keep the length of both index lists consistent.
    /// </summary>
    /// <param name="leftIndices">Left side index array</param>
    /// <param name="rightIndices">Right side index array</param>
    /// <param name="rightUnmatchedIndices">Unlatched right side index list</param>
    /// <returns>Left and Right side indices</returns>
    internal static (long?[] LeftIndices, long?[] RightIndices) AppendRightIndices(
        long[] leftIndices, long[] rightIndices, IReadOnlyCollection<long> rightUnmatchedIndices)
    {
        var unmatchedSize = rightUnmatchedIndices.Count;
        if (unmatchedSize == 0)
        {
            return (leftIndices.AsNullable(), rightIndices.AsNullable());
        }

        var newLeftIndices = leftIndices.AsNullable().Concat(new long?[unmatchedSize]).ToArray();
        var newRightIndices = rightIndices.Concat(rightUnmatchedIndices).AsNullable();

        return (newLeftIndices, newRightIndices);
    }
}