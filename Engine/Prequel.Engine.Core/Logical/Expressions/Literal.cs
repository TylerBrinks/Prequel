using Prequel.Engine.Core.Logical.Values;

namespace Prequel.Engine.Core.Logical.Expressions;

/// <summary>
/// Literal expression
/// </summary>
/// <param name="Value">Scalar literal value</param>
internal record Literal(ScalarValue Value) : ILogicalExpression
{
    public override string ToString()
    {
        if (Value is StringScalar s)
        {
            return $"'{s.Value}'";
        }

        return Value.ToString();
    }
}