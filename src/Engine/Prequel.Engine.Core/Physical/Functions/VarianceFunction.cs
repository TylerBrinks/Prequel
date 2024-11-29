using Prequel.Engine.Core.Physical.Aggregation;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Physical;
using Prequel.Engine.Core.Physical.Aggregation;
using Prequel.Engine.Core.Physical.Expressions;

namespace Prequel.Engine.Core.Physical.Functions;


/// <summary>
/// Physical variance function
/// </summary>
/// <param name="Expression">First input expression</param>
/// <param name="Name">Function string name</param>
/// <param name="DataType">Function output data type</param>
/// <param name="StatisticType">Population or Sample calculation</param>
internal record VarianceFunction(
        IPhysicalExpression Expression,
        string Name,
        ColumnDataType DataType,
        StatisticType StatisticType)
    : Aggregate(Expression), IAggregation
{
    private string? _prefix;
    /// <summary>
    /// Aggregate state to use between aggregation operations or calculation steps.
    /// Stores count, mean 1, and mean 2 as unqualified fields.
    /// </summary>
    internal override List<QualifiedField> StateFields =>
    [
        QualifiedField.Unqualified($"{StatePrefix}({Name})[count]", ColumnDataType.Integer),
        QualifiedField.Unqualified($"{StatePrefix}({Name})[mean]", ColumnDataType.Double),
        QualifiedField.Unqualified($"{StatePrefix}({Name})[m2]", ColumnDataType.Double)
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
    internal override List<IPhysicalExpression> Expressions => new() { Expression };
    /// <summary>
    /// Creates an accumulator for the function's data type
    /// </summary>
    /// <returns>Variance accumulator</returns>
    public Accumulator CreateAccumulator()
    {
        return new VarianceAccumulator(DataType, StatisticType);
    }
}