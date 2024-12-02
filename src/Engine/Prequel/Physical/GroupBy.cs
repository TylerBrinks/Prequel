using Prequel.Physical.Expressions;

namespace Prequel.Physical;

/// <summary>
/// Class used for creating physical group by expression
/// </summary>
/// <param name="Expression">Grouping expressions and expression names</param>
internal record GroupBy(List<(IPhysicalExpression Expression, string Name)> Expression)
{
    public static GroupBy NewSingle(List<(IPhysicalExpression Expression, string Name)> expressions)
    {
        return new GroupBy(expressions);
    }

    public override string ToString()
    {
        var expressions = Expression.Select(e => e.ToString()).ToList();

        return string.Join(", ", expressions);
    }
}