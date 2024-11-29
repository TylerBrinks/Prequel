using Prequel.Engine.Core.Logical.Values;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Logical.Values;
using Prequel.Engine.Core.Physical;

namespace Prequel.Engine.Core.Physical.Aggregation;

/// <summary>
/// Standard deviation aggregation accumulator
/// </summary>
/// <param name="DataType">Accumulated data type</param>
/// <param name="StatisticType">Population or Sample calculation</param>
internal record StandardDeviationAccumulator(ColumnDataType DataType, StatisticType StatisticType)
    : VarianceAccumulator(DataType, StatisticType)
{
    /// <summary>
    /// Accumulated standard deviation value
    /// </summary>
    public override object? Value => CalculateStandardDeviation();
    /// <summary>
    /// Calculated standard deviation in integer (long) or double format
    /// </summary>
    public override ScalarValue Evaluate
    {
        get
        {
            var deviation = CalculateStandardDeviation();

            return DataType == ColumnDataType.Integer
                ? new IntegerScalar(Convert.ToInt64(deviation))
                : new DoubleScalar(deviation);
        }
    }
    /// <summary>
    /// Calculates standard deviation using the square
    /// root of the calculated variance
    /// </summary>
    /// <returns>Calculated standard deviation</returns>
    private double CalculateStandardDeviation()
    {
        return Math.Sqrt(CalculateVariance());
    }
}