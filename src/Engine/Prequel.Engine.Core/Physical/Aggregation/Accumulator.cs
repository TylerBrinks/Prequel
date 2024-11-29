using Prequel.Engine.Core.Logical.Values;
using Prequel.Engine.Core.Values;

namespace Prequel.Engine.Core.Physical.Aggregation;

/// <summary>
/// Base class for accumulating values into an a
/// calculated scalar output value 
/// </summary>
internal abstract record Accumulator
{
    /// <summary>
    /// Accumulated value
    /// </summary>
    public abstract object? Value { get; }
    /// <summary>
    /// Accumulator state to use between aggregation operations or calculation steps
    /// </summary>
    public abstract List<ScalarValue> State { get; }
    /// <summary>
    /// Evaluates the aggregate function and produces a scalar result
    /// </summary>
    public abstract ScalarValue Evaluate { get; }
    /// <summary>
    /// Adds a data point to the accumulator
    /// </summary>
    /// <param name="value">Value to accumulate into the calculated result</param>
    public abstract void Accumulate(object value);
    /// <summary>
    /// Adds multiple data points to the accumulator
    /// </summary>
    /// <param name="values">Values to append</param>
    public abstract void UpdateBatch(List<ArrayColumnValue> values);
    /// <summary>
    /// Merges multiple data points from multiple accumulations
    /// into the accumulator to produce a final result
    /// </summary>
    /// <param name="values">Values to merge</param>
    public abstract void MergeBatch(List<ArrayColumnValue> values);
}