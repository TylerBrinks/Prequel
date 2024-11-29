using Prequel.Logical.Values;
using Prequel.Values;
using Prequel.Data;

namespace Prequel.Physical.Aggregation;

/// <summary>
/// Average value aggregation accumulator
/// </summary>
/// <param name="DataType">Calculated value output data type</param>
internal record AverageAccumulator(ColumnDataType DataType) : Accumulator
{
    private long _count;
    private double _sum;

    /// <summary>
    /// Accumulated average value
    /// </summary>
    public override object Value => CalculateAverage();
    /// <summary>
    /// Accumulator state in the form of the calculated average
    /// </summary>
    public override List<ScalarValue> State => [Evaluate];
    /// <summary>
    /// Calculated average value
    /// </summary>
    public override ScalarValue Evaluate => CalculateAverageValue();

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

        _count++;
        _sum += Convert.ToDouble(value);
    }
    /// <summary>
    /// Adds multiple data points to the accumulator
    /// </summary>
    /// <param name="values">Values to append</param>
    public override void UpdateBatch(List<ArrayColumnValue> values)
    {
        var array = values[0];

        _count += array.Values.Count;

        foreach (var item in array.Values)
        {
            _sum += Convert.ToDouble(item);
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
    /// Calculates the average value based on the expected data
    /// type; integer (long) or double
    /// </summary>
    /// <returns>Calculated scalar value</returns>
    private ScalarValue CalculateAverageValue()
    {
        var average = CalculateAverage();

        return DataType switch
        {
            ColumnDataType.Integer => new IntegerScalar(Convert.ToInt64(average)),
            _ => new DoubleScalar(average)
        };
    }
    /// <summary>
    /// Calculates accumulated average
    /// </summary>
    /// <returns>Calculated average</returns>
    private double CalculateAverage()
    {
        return _count == 0 ? 0 : _sum / _count;
    }
}