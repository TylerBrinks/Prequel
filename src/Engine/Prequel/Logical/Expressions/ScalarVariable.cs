namespace Prequel.Logical.Expressions;

/// <summary>
/// Scalar variable
/// </summary>
/// <param name="Names">Variable names</param>
internal record ScalarVariable(IEnumerable<string> Names) : ILogicalExpression
{
    public override string ToString()
    {
        return string.Join(".", Names);
    }
}