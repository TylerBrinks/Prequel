using Prequel.Data;
using Prequel.Physical.Aggregation;
using Prequel.Physical.Expressions;

// ReSharper disable StringLiteralTypo

namespace Prequel.Physical.Functions;

/// <summary>
/// Physical covariance function
/// </summary>
/// <param name="InputExpression1">First input expression</param>
/// <param name="InputExpression2">Second input expression</param>
/// <param name="Name">Function string name</param>
/// <param name="DataType">Function output data type</param>
/// <param name="StatisticType">Population or Sample calculation</param>
internal record CovarianceFunction(
        IPhysicalExpression InputExpression1,
        IPhysicalExpression InputExpression2,
        string Name,
        ColumnDataType DataType,
        StatisticType StatisticType)
    : Aggregate(InputExpression1), IAggregation
{
    private string? _prefix;

    /// <summary>
    /// Aggregate state to use between aggregation operations or calculation steps.
    /// Stores count, mean 1, mean 2, and algoConst as unqualified fields.
    /// </summary>
    internal override List<QualifiedField> StateFields =>
    [
        QualifiedField.Unqualified($"{StatePrefix}({Name})[count]", ColumnDataType.Integer),
        QualifiedField.Unqualified($"{StatePrefix}({Name})[mean1]", ColumnDataType.Double),
        QualifiedField.Unqualified($"{StatePrefix}({Name})[mean2]", ColumnDataType.Double),
        QualifiedField.Unqualified($"{StatePrefix}({Name})[algoConst]", ColumnDataType.Double)
    ];
    /// <summary>
    /// Prefix for population or sample respectively
    /// </summary>
    private string StatePrefix => _prefix ??= StatisticType == StatisticType.Population ? "_POP" : "";
    /// <summary>
    /// Gets the field in qualified format
    /// </summary>
    internal override QualifiedField NamedQualifiedField => new(Name, DataType);
    /// <summary>
    /// Gets the expressions used as inputs for the function
    /// </summary>
    internal override List<IPhysicalExpression> Expressions =>
    [
        InputExpression1,
        InputExpression2
    ];
    /// <summary>
    /// Creates an accumulator for the function's data type
    /// </summary>
    /// <returns>Covariance accumulator</returns>
    public Accumulator CreateAccumulator()
    {
        return new CovarianceAccumulator(DataType, StatisticType);
    }

    public override string ToString() => $"covar({Name}):{DataType}";

}