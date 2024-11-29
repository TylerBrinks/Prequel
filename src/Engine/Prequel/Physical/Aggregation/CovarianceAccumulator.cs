using Prequel.Logical.Values;
using Prequel.Values;
using Prequel.Data;

namespace Prequel.Physical.Aggregation;

/// <summary>
/// Covariance value aggregation accumulator
/// </summary>
/// <param name="DataType">Accumulated data type</param>
/// <param name="StatisticType">Population or Sample calculation</param>
internal record CovarianceAccumulator(ColumnDataType DataType, StatisticType StatisticType) : Accumulator
{
    private long _count;
    private double _const;
    private double _mean1;
    private double _mean2;

    /// <summary>
    /// Accumulated covariance value
    /// </summary>
    public override object Value => CalculateCovariance();
    /// <summary>
    /// State carries count, mean 1, mean 2, and const as
    /// values used in covariance calculations
    /// </summary>
    public override List<ScalarValue> State =>
    [
        new IntegerScalar(_count),
        new DoubleScalar(_mean1),
        new DoubleScalar(_mean2),
        new DoubleScalar(_const)
    ];
    /// <summary>
    /// Calculated covariance in integer (long) or double format
    /// </summary>
    public override ScalarValue Evaluate =>
        DataType == ColumnDataType.Integer
            ? new IntegerScalar(Convert.ToInt64(CalculateCovariance()))
            : new DoubleScalar(CalculateCovariance());

    /// <summary>
    /// Adds a data point to the accumulator
    /// </summary>
    /// <param name="value">Value to accumulate into the calculated result</param>
    public override void Accumulate(object value)
    {
        throw new NotImplementedException("Covariance does not accumulate singular values.");
    }
    /// <summary>
    /// Processes the list of values recalculating each count,
    /// mean 1, mean 2, and const value
    /// </summary>
    /// <param name="values">Values to append</param>
    public override void UpdateBatch(List<ArrayColumnValue> values)
    {
        var values1 = values[0];
        var values2 = values[1];

        for (var i = 0; i < values1.Values.Count; i++)
        {
            var val1 = values1.Values[i];
            var val2 = values2.Values[i];

            if (val1 == null || val2 == null)
            {
                continue;
            }

            var value1 = Convert.ToDouble(val1);
            var value2 = Convert.ToDouble(val2);

            var newCount = _count + 1;
            var delta1 = value1 - _mean1;
            var newMean1 = delta1 / newCount + _mean1;
            var delta2 = value2 - _mean2;
            var newMean2 = delta2 / newCount + _mean2;
            var newConst = delta1 * (value2 - newMean2) + _const;

            _count += 1;
            _mean1 = newMean1;
            _mean2 = newMean2;
            _const = newConst;
        }
    }
    /// <summary>
    /// Merges data from multiple calculations into a single calculated
    /// value.  The existing state values are calculated against the
    /// new list of values and count, mean 1, mean 2, and const values
    /// are all recalculated to derive the final covariance value.
    /// </summary>
    /// <param name="values">Values to merge</param>
    public override void MergeBatch(List<ArrayColumnValue> values)
    {
        var counts = values[0];
        var means1 = values[1];
        var means2 = values[2];
        var cs = values[3];

        for (var i = 0; i < counts.Values.Count; i++)
        {
            var c = Convert.ToInt64(counts.GetValue(i)!);

            var newCount = _count + c;
            var indexMean1 = (double)means1.GetValue(i)!;
            var indexMean2 = (double)means2.GetValue(i)!;
            var indexCs = (double)cs.GetValue(i)!;

            var newMean1 = _mean1 * _count / newCount + indexMean1 * c / newCount;
            var newMean2 = _mean2 * _count / newCount + indexMean2 * c / newCount;
            var delta1 = _mean1 - indexMean1;
            var delta2 = _mean2 - indexMean2;
            var newConst = _const + indexCs + delta1 * delta2 * _count * c / newCount;

            _count = newCount;
            _mean1 = newMean1;
            _mean2 = newMean2;
            _const = newConst;
        }
    }
    /// <summary>
    /// Calculates covariance using the configured population
    /// or sample mode.
    /// </summary>
    /// <returns>Calculated covariance</returns>
    public double CalculateCovariance()
    {
        var count = GetCount();

        if (count == 0)
        {
            return 0;
        }

        return _const / count;
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