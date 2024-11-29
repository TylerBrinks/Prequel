namespace Prequel.Logical.Expressions;

/// <summary>
/// CASE/WHEN/THEN/ELSE logical expression
/// </summary>
/// <param name="Expression">CASE Logical expression</param>
/// <param name="WhenThenExpression">When/Then expression</param>
/// <param name="ElseExpression">Optional ELSE logical expression</param>
internal record Case(
    ILogicalExpression? Expression,
    List<(ILogicalExpression When, ILogicalExpression Then)> WhenThenExpression,
    ILogicalExpression? ElseExpression) : ILogicalExpression
{
    public override string ToString()
    {
        return "Case/When";
    }
}