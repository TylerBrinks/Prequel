namespace Prequel.Logical.Expressions;

/// <summary>
/// BETWEEN expression
/// </summary>
/// <param name="Expression">Between logical expression</param>
/// <param name="Negated">True if NOT BETWEEN; otherwise false</param>
/// <param name="Low">Logical expression with the minimum between value</param>
/// <param name="High">Logical expression with the maximum between value</param>
internal record Between(ILogicalExpression Expression, bool Negated, ILogicalExpression Low, ILogicalExpression High) : ILogicalExpression
{
    public override string ToString()
    {
        var negated = Negated ? "Not " : string.Empty;
        return $"{Expression} {negated}Between {Low} And {High}";
    }
}
