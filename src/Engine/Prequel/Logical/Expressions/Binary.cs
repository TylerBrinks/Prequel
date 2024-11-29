using SqlParser.Ast;

namespace Prequel.Logical.Expressions;

/// <summary>
/// Binary logical expression
/// </summary>
/// <param name="Left">Left hand side comparision expression</param>
/// <param name="Op">Binary comparision operation</param>
/// <param name="Right">Right hand side comparision expression</param>
internal record Binary(ILogicalExpression Left, BinaryOperator Op, ILogicalExpression Right) : ILogicalExpression
{
    public override string ToString()
    {
        return $"{Left} {Op.GetDisplayText()} {Right}";
    }
}
