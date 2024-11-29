namespace Prequel.Engine.Core.Logical.Expressions;

/// <summary>
/// ORDER BY expression
/// </summary>
/// <param name="Expression">Logical expression</param>
/// <param name="Ascending">true if ascending order; otherwise false</param>
internal record OrderBy(ILogicalExpression Expression, bool Ascending) : ILogicalExpression //todo nulls first?
{
    public override string ToString()
    {
        var direction = Ascending ? "Asc" : "Desc";
        return $"Order By {Expression.CreateLogicalName()} {direction}";
    }
}