using Prequel.Physical.Aggregation;
using Prequel.Physical.Expressions;
using Prequel.Data;

namespace Prequel.Physical.Functions;

/// <summary>
/// Physical min function
/// </summary>
/// <param name="InputExpression"></param>
/// <param name="Name"></param>
/// <param name="DataType"></param>
internal record MinFunction(IPhysicalExpression InputExpression, string Name, ColumnDataType DataType)
    : Aggregate(InputExpression), IAggregation
{
    /// <summary>
    /// Min state to use between aggregation operations or calculation steps
    /// </summary>
    internal override List<QualifiedField> StateFields => [QualifiedField.Unqualified($"{Name}", DataType)];
    /// <summary>
    /// Gets the field using the unqualified format
    /// </summary>
    internal override QualifiedField NamedQualifiedField => QualifiedField.Unqualified(Name, DataType);
    /// <summary>
    /// Gets the expressions used as inputs for the function
    /// </summary>
    internal override List<IPhysicalExpression> Expressions => [Expression];
    /// <summary>
    /// Creates an accumulator for the function's data type
    /// </summary>
    /// <returns>Min accumulator</returns>
    public Accumulator CreateAccumulator()
    {
        return new MinAccumulator(DataType);
    }
    
    public override string ToString() => $"min({Name}):{DataType}";
}