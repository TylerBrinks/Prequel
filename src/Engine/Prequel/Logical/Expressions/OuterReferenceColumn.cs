using Prequel.Data;

namespace Prequel.Logical.Expressions;

/// <summary>
/// Outer reference column pointing to a value from an expression higher up in the expression tree.
/// </summary>
/// <param name="DataType">Expression data type</param>
/// <param name="Column">Column details</param>
internal record OuterReferenceColumn(ColumnDataType DataType, Column Column) : ILogicalExpression
{
    public override string ToString()
    {
        return $"outer_ref({Column})";
    }
}