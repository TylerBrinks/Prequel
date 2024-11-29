using Prequel.Data;

namespace Prequel.Logical.Expressions;

/// <summary>
/// Cast logical expression
/// </summary>
/// <param name="Expression">Expression to cast to a new type</param>
/// <param name="CastType">Target data type</param>
internal record Cast(ILogicalExpression Expression, ColumnDataType CastType) : ILogicalExpression
{
    public override string ToString()
    {
        return $"Cast({CastType})";
    }
}