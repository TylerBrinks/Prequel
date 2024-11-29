using Prequel.Engine.Core.Logical.Values;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Logical.Values;
using Prequel.Engine.Core.Values;

namespace Prequel.Engine.Core.Physical.Aggregation;

/// <summary>
/// Maximum value aggregation accumulator
/// </summary>
/// <param name="DataType">Accumulated data type</param>
internal record MaxAccumulator(ColumnDataType DataType) : Accumulator
{
    private double? _value;
    /// <summary>
    /// Accumulated max value
    /// </summary>
    public override object? Value => _value;
    /// <summary>
    /// State value in the form of the calculated max value
    /// </summary>
    public override List<ScalarValue> State => [Evaluate];
    /// <summary>
    /// Calculated max in integer (long) or double format
    /// </summary>
    public override ScalarValue Evaluate =>
        DataType == ColumnDataType.Integer
            ? new IntegerScalar((long)(_value ?? 0))
            : new DoubleScalar(_value ?? 0);
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
            _value = Convert.ToDouble(value);
        }
        else
        {
            _value = value switch
            {
                int i when i > _value => Convert.ToDouble(i),
                long l when l > _value => Convert.ToDouble(l),
                double d when d > _value => d,
                _ => _value
            };
        }
    }
    /// <summary>
    /// Checks for a new maximum value.
    /// </summary>
    /// <param name="values">Values to check against the current maximum</param>
    public override void UpdateBatch(List<ArrayColumnValue> values)
    {
        foreach (var value in values[0].Values)
        {
            Accumulate(value);
        }
    }
    /// <summary>
    /// Checks for a new maximum value.
    /// </summary>
    /// <param name="values">Values to check against the current maximum</param>
    public override void MergeBatch(List<ArrayColumnValue> values)
    {
        UpdateBatch(values);
    }
}