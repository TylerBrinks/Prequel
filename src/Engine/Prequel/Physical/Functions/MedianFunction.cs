using Prequel.Data;
using Prequel.Physical.Aggregation;
using Prequel.Physical.Expressions;

namespace Prequel.Physical.Functions;

/// <summary>
/// Physical median function
/// </summary>
/// <param name="InputExpression"></param>
/// <param name="Name"></param>
/// <param name="DataType"></param>
internal record MedianFunction(IPhysicalExpression InputExpression, string Name, ColumnDataType DataType)
    : Aggregate(InputExpression), IAggregation
{
    /// <summary>
    /// Median state to use between aggregation operations or calculation steps
    /// </summary>
    internal override List<QualifiedField> StateFields => [QualifiedField.Unqualified($"{Name}", DataType)];
    /// <summary>
    /// Gets the field using the unqualified format
    /// </summary>
    internal override QualifiedField NamedQualifiedField => new(Name, DataType);
    /// <summary>
    /// Gets the expressions used as inputs for the function
    /// </summary>
    internal override List<IPhysicalExpression> Expressions => [Expression];
    /// <summary>
    /// Creates an accumulator for the function's data type
    /// </summary>
    /// <returns>Median accumulator</returns>
    public Accumulator CreateAccumulator()
    {
        return new MedianAccumulator(DataType);
    }
}