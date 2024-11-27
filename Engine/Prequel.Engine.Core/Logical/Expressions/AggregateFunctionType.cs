namespace Prequel.Engine.Core.Logical.Expressions;

internal enum AggregateFunctionType
{
    Count,
    Sum,
    Min,
    Max,
    Avg,
    Median,
    /// Approximate aggregate function
    ApproxDistinct,
    ArrayAgg,
    /// Variance (Sample)
    Variance,
    /// Variance (Population)
    VariancePop,
    /// Standard Deviation (Sample)
    StdDev,
    /// Standard Deviation (Population)
    StdDevPop,
    /// Covariance (Sample)
    Covariance,
    /// Covariance (Population)
    CovariancePop,
    Correlation,
    /// Approximate continuous percentile function
    ApproxPercentileCont,
    /// Approximate continuous percentile function with weight
    ApproxPercentileContWithWeight,
    ApproxMedian,
    Grouping,
}