// ReSharper disable StringLiteralTypo
namespace Prequel.Logical.Expressions;

/// <summary>
/// Defines an aggregate function
/// </summary>
/// <param name="FunctionType">Type of aggregation</param>
/// <param name="Args">Arguments used in the aggregate function call</param>
/// <param name="Distinct">True if the function contains a DISTINCT keyword</param>
/// <param name="Filter">Optional filter expression</param>
internal record AggregateFunction(
       AggregateFunctionType FunctionType,
       List<ILogicalExpression> Args,
       bool Distinct,
       ILogicalExpression? Filter = null) : ILogicalExpression
{
    /// <summary>
    /// Translates a string name into an aggregate function type
    /// </summary>
    /// <param name="name">Function name</param>
    /// <returns>AggregateFunctionType</returns>
    internal static AggregateFunctionType? GetFunctionType(string name)
    {
        return name.ToLowerInvariant() switch
        {
            "min" => AggregateFunctionType.Min,
            "max" => AggregateFunctionType.Max,
            "count" => AggregateFunctionType.Count,
            "avg" or "mean" => AggregateFunctionType.Avg,
            "sum" => AggregateFunctionType.Sum,
            "median" => AggregateFunctionType.Median,

            //"approx_distinct" => AggregateFunctionType.ApproxDistinct,
            //"array_agg" => AggregateFunctionType.ArrayAgg

            "var" or "var_samp" => AggregateFunctionType.Variance,
            "var_pop" => AggregateFunctionType.VariancePop,
            "stddev" or "stddev_samp" => AggregateFunctionType.StdDev,
            "stddev_pop" => AggregateFunctionType.StdDevPop,
            "covar" or "covar_samp" => AggregateFunctionType.Covariance,
            "covar_pop" => AggregateFunctionType.CovariancePop,

            //"corr" => AggregateFunctionType.Correlation,
            //"approx_percentile_cont" => AggregateFunction::ApproxPercentileCont,
            //"approx_percentile_cont_with_weight" => {
            //"approx_median" => AggregateFunctionType.ApproxMedian,
            //"grouping" => AggregateFunction::Grouping,

            //TODO other aggregate functions
            _ => null
        };
    }

    public override string ToString()
    {
        var exp = string.Join(", ", Args.Select(a => a.ToString()));
        return $"{FunctionType}({exp})";
    }

    public virtual bool Equals(AggregateFunction? other)
    {
        if (other == null) { return false; }

        var equal = FunctionType == other.FunctionType &&
                    Distinct == other.Distinct &&
                    Args.SequenceEqual(other.Args);

        if (equal && Filter != null)
        {
            equal &= Filter.Equals(other.Filter);
        }

        return equal;
    }
}

