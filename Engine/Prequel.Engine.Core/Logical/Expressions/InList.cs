namespace Prequel.Engine.Core.Logical.Expressions;

/// <summary>
/// IN logical expression
/// </summary>
/// <param name="Expression">Logical expression</param>
/// <param name="List">List of IN expressions</param>
/// <param name="Negated">True if the expressions are negated; otherwise false</param>
internal record InList(ILogicalExpression Expression, List<ILogicalExpression> List, bool Negated) : ILogicalExpression
{
    public override string ToString()
    {
        var negated = Negated ? "Not " : "";
        return $"{negated}In List";
    }
}