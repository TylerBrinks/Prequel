using Prequel.Engine.Core.Logical.Values;
using Prequel.Engine.Core.Logical.Values;
using Prequel.Engine.Core.Values;

namespace Prequel.Engine.Core.Physical.Aggregation;

/// <summary>
/// Count value aggregation accumulator
/// </summary>
internal record CountAccumulator : Accumulator
{
    private uint _count;

    /// <summary>
    /// Accumulated count value
    /// </summary>
    public override object Value => _count;
    /// <summary>
    /// Accumulator state in the form of the calculated average
    /// </summary>
    public override List<ScalarValue> State => [Evaluate];
    /// <summary>
    /// Calculated count value
    /// </summary>
    public override ScalarValue Evaluate => new IntegerScalar(_count);
    /// <summary>
    /// Adds a data point to the accumulator
    /// </summary>
    /// <param name="value">Value to accumulate into the calculated result</param>
    public override void Accumulate(object value)
    {
        _count++;
    }
    /// <summary>
    /// Adds multiple data points to the accumulator
    /// </summary>
    /// <param name="values">Values to append</param>
    public override void UpdateBatch(List<ArrayColumnValue> values)
    {
        foreach (var value in values[0].Values)
        {
            Accumulate(value);
        }
    }
    /// <summary>
    /// Merges multiple data points from multiple accumulations
    /// into the accumulator to produce a final result
    /// </summary>
    /// <param name="values">Values to merge</param>
    public override void MergeBatch(List<ArrayColumnValue> values)
    {
        _count = Convert.ToUInt32(values[0].Values[0]);
    }
}