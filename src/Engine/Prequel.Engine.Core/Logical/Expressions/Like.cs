namespace Prequel.Engine.Core.Logical.Expressions;

/// <summary>
/// LIKE logical expression
/// </summary>
/// <param name="Negated">True if the expression is negated; otherwise false.</param>
/// <param name="Expression">LIKE expression</param>
/// <param name="Pattern">Comparison pattern</param>
/// <param name="EscapeCharacter">Pattern escape character</param>
/// <param name="CaseSensitive">True if the comparison is case-sensitive; otherwise false.</param>
internal record Like(
    bool Negated,
    ILogicalExpression Expression,
    ILogicalExpression Pattern,
    char? EscapeCharacter,
    bool CaseSensitive) : ILogicalExpression
{
    public override string ToString()
    {
        var negated = Negated ? "Not " : "";
        var caseSensitive = CaseSensitive ? "I" : "";

        return $"{negated}{caseSensitive}Like";
    }
}