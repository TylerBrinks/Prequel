using Prequel.Data;
using Prequel.Logical.Expressions;

namespace Prequel.Logical.Plans;

/// <summary>
/// Logical projection plan
/// </summary>
/// <param name="Plan"></param>
/// <param name="Expression"></param>
/// <param name="Schema"></param>
internal record Projection(ILogicalPlan Plan, List<ILogicalExpression> Expression, Schema Schema) : ILogicalPlanParent
{
    public string ToStringIndented(Indentation? indentation = null)
    {
        var indent = indentation ?? new Indentation();
        var expressions = Expression.Select(e => e.ToString()).ToList();

        var projections = string.Join(", ", expressions);

        return $"Projection: {projections} {indent.Next(Plan)}";
    }
    /// <summary>
    /// Creates a new instance of a projection plan
    /// </summary>
    /// <param name="plan">Parent plan</param>
    /// <param name="expressions">Projection expressions</param>
    /// <returns>New projection plan instance</returns>
    public static Projection TryNew(ILogicalPlan plan, List<ILogicalExpression> expressions)
    {
        var schema = new Schema(expressions.ExpressionListToFields(plan));

        return new Projection(plan, expressions, schema);
    }
}