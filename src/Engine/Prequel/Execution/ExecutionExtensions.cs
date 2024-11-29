namespace Prequel.Execution;

internal static class ExecutionExtensions
{
    /// <summary>
    /// Gets inverse of the unmatched de-duplicated indices
    /// </summary>
    /// <param name="rowCount">Number of rows in the current batch</param>
    /// <param name="inputIndices">Left side index values</param>
    /// <returns>Anti-index values (inverse of matched/true values)</returns>
    internal static long[] GetAntiIndices(this IEnumerable<long> inputIndices, int rowCount)
    {
        return GetUnmatchedIndices(rowCount, inputIndices, match => !match);
    }
    /// <summary>
    /// Gets unmatched de-duplicated indices
    /// </summary>
    /// <param name="rowCount">Number of rows in the current batch</param>
    /// <param name="inputIndices">Left side index values</param>
    /// <returns>Semi-index values</returns>
    internal static long[] GetSemiIndices(this IEnumerable<long> inputIndices, int rowCount)
    {
        return GetUnmatchedIndices(rowCount, inputIndices, match => match);
    }
    /// <summary>
    /// Gets unmatched de-duplicated indices
    /// </summary>
    /// <param name="rowCount">Number of rows in the current batch</param>
    /// <param name="inputIndices">Left side index values</param>
    /// <param name="comparison">Comparison function for semi/anti index value matching</param>
    /// <returns>Semi-join index values</returns>
    internal static long[] GetUnmatchedIndices(int rowCount, IEnumerable<long> inputIndices, Func<bool, bool> comparison)
    {
        var bitmap = new bool[rowCount];

        foreach (var inputIndex in inputIndices)
        {
            bitmap[inputIndex] = true;
        }

        var antiIndices = Enumerable.Range(0, rowCount)
            .Select(index => ((long)index, comparison(bitmap[index])))
            .Where(i => i.Item2)
            .Select(i => i.Item1);

        return antiIndices.ToArray();
    }
}
