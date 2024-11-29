using Prequel.Engine.Core.Physical.Expressions;

namespace Prequel.Engine.Core.Physical;

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
}