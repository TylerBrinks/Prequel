namespace Prequel.Engine.Core.Logical.Expressions;

/// <summary>
/// Holds the alias name for a logical expression
///
/// e.g. "Col_1 as Name"
/// </summary>
/// <param name="Expression"></param>
/// <param name="Name"></param>
internal record Alias(ILogicalExpression Expression, string Name) : ILogicalExpression
{
    public override string ToString()
    {
        return $"{Expression} AS {Name}";
    }
}