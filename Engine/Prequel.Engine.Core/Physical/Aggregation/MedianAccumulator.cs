using Prequel.Engine.Core.Logical.Values;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Logical.Values;
using Prequel.Engine.Core.Values;

namespace Prequel.Engine.Core.Physical.Aggregation;

/// <summary>
/// Median value aggregation accumulator
/// </summary>
/// <param name="DataType">Calculated value output data type</param>
internal record MedianAccumulator(ColumnDataType DataType) : Accumulator
{
    private readonly List<double> _values = [];
    /// <summary>
    /// Accumulated median value
    /// </summary>
    public override object Value => CalculateMedian().RawValue!;
    /// <summary>
    /// Accumulator state in the form of the calculated median
    /// </summary>
    public override List<ScalarValue> State => [Evaluate];
    /// <summary>
    /// Calculated median value
    /// </summary>
    public override ScalarValue Evaluate => CalculateMedian();
    /// <summary>
    /// Adds a data point to the accumulator
    /// </summary>
    /// <param name="value">Value to accumulate into the calculated result</param>
    public override void Accumulate(object? value)
    {
        if (value == null)
        {
            return;
        }

        _values.Add(Convert.ToDouble(value));
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
        UpdateBatch(values);
    }
    /// <summary>
    /// Calculates accumulated median
    /// </summary>
    /// <returns>Calculated median</returns>
    private ScalarValue CalculateMedian()
    {
        var valueArray = _values.ToArray();
        Array.Sort(valueArray);
        var length = valueArray.Length;
        var middle = length / 2;

        var median = length % 2 != 0
            ? valueArray[middle]
            : (valueArray[middle] + valueArray[middle - 1]) / 2;

        return DataType switch
        {
            ColumnDataType.Integer => new IntegerScalar(Convert.ToInt64(median)),
            _ => new DoubleScalar(median),
        };
    }
}