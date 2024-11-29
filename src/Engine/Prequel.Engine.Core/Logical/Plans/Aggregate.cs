using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Logical.Expressions;

namespace Prequel.Engine.Core.Logical.Plans;

/// <summary>
/// Aggregation logical plan
/// </summary>
/// <param name="Plan">Parent plan</param>
/// <param name="GroupExpressions">Grouping expressions</param>
/// <param name="AggregateExpressions">Aggregation expressions</param>
/// <param name="Schema">Schema with aggregation and grouping field details</param>
internal record Aggregate(
    ILogicalPlan Plan,
    List<ILogicalExpression> GroupExpressions,
    List<ILogicalExpression> AggregateExpressions,
    Schema Schema)
    : ILogicalPlanParent
{
    public string ToStringIndented(Indentation? indentation = null)
    {
        var indent = indentation ?? new Indentation();
        var groups = string.Join(",", GroupExpressions);
        var aggregates = string.Join(",", AggregateExpressions);

        return $"Aggregate: groupBy=[{groups}], aggregate=[{aggregates}]{indent.Next(Plan)}";
    }
    /// <summary>
    /// Creates a new aggregation plan
    /// </summary>
    /// <param name="plan">Parent logical plan</param>
    /// <param name="groupExpressions">Grouping logical expressions</param>
    /// <param name="aggregateExpressions">Aggregation expressions</param>
    /// <returns></returns>
    public static Aggregate TryNew(ILogicalPlan plan, List<ILogicalExpression> groupExpressions, List<ILogicalExpression> aggregateExpressions)
    {
        var allExpressions = groupExpressions.Concat(aggregateExpressions).ToList();
        var schema = new Schema(allExpressions.ExpressionListToFields(plan));

        return new Aggregate(plan, groupExpressions, aggregateExpressions, schema);
    }
}