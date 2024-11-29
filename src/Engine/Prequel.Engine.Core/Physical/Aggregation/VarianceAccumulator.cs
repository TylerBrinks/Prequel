using Prequel.Engine.Core.Logical.Values;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Logical.Values;
using Prequel.Engine.Core.Physical;
using Prequel.Engine.Core.Values;

namespace Prequel.Engine.Core.Physical.Aggregation;

/// <summary>
/// Variance aggregation accumulator
/// </summary>
/// <param name="DataType">Accumulated data type</param>
/// <param name="StatisticType">Population or Sample calculation</param>
internal record VarianceAccumulator(ColumnDataType DataType, StatisticType StatisticType) : Accumulator
{
    private long _count;
    private double _mean;
    private double _m2;

    /// <summary>
    /// Accumulated variance value
    /// </summary>
    public override object? Value => CalculateVariance();
    /// <summary>
    /// State carries count, mean, and mean2 (m2) as
    /// values used in variance calculations
    /// </summary>
    public override List<ScalarValue> State =>
    [
        new IntegerScalar(_count),
        new DoubleScalar(_mean),
        new DoubleScalar(_m2)
    ];
    /// <summary>
    /// Calculated standard deviation in integer (long) or double format
    /// </summary>
    public override ScalarValue Evaluate =>
        DataType == ColumnDataType.Integer
            ? new IntegerScalar(Convert.ToInt64(CalculateVariance()))
            : new DoubleScalar(CalculateVariance());

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

        var val = Convert.ToDouble(value);

        var newCount = _count + 1;
        var delta1 = val - _mean;
        var newMean = delta1 / newCount + _mean;
        var delta2 = val - newMean;
        var newM2 = _m2 + delta1 * delta2;
        _count += 1;
        _mean = newMean;
        _m2 = newM2;
    }
    /// <summary>
    /// Processes the list of values recalculating each count,
    /// mean, and mean 2 value
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
    /// Merges data from multiple calculations into a single calculated
    /// value.  The existing state values are calculated against the
    /// new list of values and count, mean 1, and mean 2 values
    /// are all recalculated to derive the final covariance value.
    /// </summary>
    /// <param name="values">Values to merge</param>
    public override void MergeBatch(List<ArrayColumnValue> values)
    {
        var counts = values[0];
        var means = values[1];
        var m2s = values[2];

        for (var i = 0; i < counts.Values.Count; i++)
        {
            var c = Convert.ToInt64(counts.GetValue(i)!);

            if (c == 0)
            {
                continue;
            }

            var newCount = _count + c;
            var indexMean = (double)means.GetValue(i)!;

            var newMean = _mean * _count / newCount + indexMean * c / newCount;
            var delta = _mean - indexMean;
            var newM2 = _m2 + (double)m2s.GetValue(i)! + delta * delta * _count * c / newCount;

            _count = newCount;
            _mean = newMean;
            _m2 = newM2;
        }
    }
    /// <summary>
    /// Calculates variance using the configured population
    /// or sample mode.
    /// </summary>
    /// <returns>Calculated variance</returns>
    public double CalculateVariance()
    {
        var count = GetCount();

        if (count is 0 or 1)
        {
            return 0;
        }

        return _m2 / count;
    }
    /// <summary>
    /// Gets the count based on statistic type
    /// </summary>
    /// <returns>Count value</returns>
    private long GetCount()
    {
        if (StatisticType == StatisticType.Population)
        {
            return _count;
        }

        if (_count > 0)
        {
            return _count - 1;
        }

        return _count;
    }
}