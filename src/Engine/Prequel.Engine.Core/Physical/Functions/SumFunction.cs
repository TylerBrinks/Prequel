using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Physical.Aggregation;
using Prequel.Engine.Core.Physical.Expressions;

namespace Prequel.Engine.Core.Physical.Functions;

/// <summary>
/// Physical sum function
/// </summary>
/// <param name="Expression">Function input expression </param>
/// <param name="Name">Function name</param>
/// <param name="DataType">Function data type</param>
internal record SumFunction(IPhysicalExpression Expression, string Name, ColumnDataType DataType)
    : Aggregate(Expression), IAggregation
{
    /// <summary>
    /// Sum state to use between aggregation operations or calculation steps
    /// </summary>
    internal override List<QualifiedField> StateFields => [QualifiedField.Unqualified($"{Name}", DataType)];
    /// <summary>
    /// Gets the field in qualified format
    /// </summary>
    internal override QualifiedField NamedQualifiedField => new(Name, DataType);
    /// <summary>
    /// Gets the expressions used as inputs for the function
    /// </summary>
    internal override List<IPhysicalExpression> Expressions => [Expression];
    /// <summary>
    /// Creates an accumulator for the function's data type
    /// </summary>
    /// <returns>Covariance accumulator</returns>
    public Accumulator CreateAccumulator()
    {
        return new SumAccumulator(DataType);
    }
}