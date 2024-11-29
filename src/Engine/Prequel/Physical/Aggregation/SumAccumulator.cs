using Prequel.Logical.Values;
using Prequel.Values;
using Prequel.Data;

namespace Prequel.Physical.Aggregation;

/// <summary>
/// Sum value aggregation accumulator
/// </summary>
/// <param name="DataType">Calculated value output data type</param>
internal record SumAccumulator(ColumnDataType DataType) : Accumulator
{
    private object? _value;

    /// <summary>
    /// Accumulated average value
    /// </summary>
    public override object? Value => _value;
    /// <summary>
    /// Accumulator state in the form of the calculated average
    /// </summary>
    public override List<ScalarValue> State => [Evaluate];
    /// <summary>
    /// Calculated average value
    /// </summary>
    public override ScalarValue Evaluate =>
        DataType == ColumnDataType.Integer
            ? new IntegerScalar(Convert.ToInt64(_value))
            : new DoubleScalar(Convert.ToDouble(_value));
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

        if (_value == null)
        {
            _value = value;
        }
        else
        {
            _value = value switch
            {
                int i => (int)_value + i,
                long l => (long)_value + l,
                double d => (double)_value + d,
                _ => _value
            };
        }
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
}