namespace Prequel.Logical.Expressions;

/// <summary>
/// Wildcard expression
/// </summary>
internal record Wildcard : ILogicalExpression
{
    public override string ToString()
    {
        return "*";
    }
}