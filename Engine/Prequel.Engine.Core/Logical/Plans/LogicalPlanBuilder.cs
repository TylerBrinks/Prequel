using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Logical.Expressions;
using Prequel.Engine.Core.Logical.Plans;
using Prequel.Engine.Core;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Logical.Expressions;
using SqlParser.Ast;

namespace Prequel.Engine.Core.Logical.Plans;

/// <summary>
/// Helper class to build logical plan steps
/// </summary>
internal static class LogicalPlanBuilder
{
    /// <summary>
    /// Builds a projection plan with a set of projected expressions
    /// </summary>
    /// <param name="plan">Parent plan</param>
    /// <param name="expressions">Projection expressions</param>
    /// <returns>New projection logical plan</returns>
    internal static ILogicalPlan Project(this ILogicalPlan plan, List<ILogicalExpression> expressions)
    {
        return Projection.TryNew(plan, expressions);
    }
    /// <summary>
    /// Builds an aggregate plan with a set of aggregation and group by expressions
    /// </summary>
    /// <param name="plan">Parent plan</param>
    /// <param name="groupExpressions">Group by expressions</param>
    /// <param name="aggregateExpressions">Aggregation function expressions</param>
    /// <returns>New aggregation logical plan</returns>
    internal static ILogicalPlan Aggregate(
        this ILogicalPlan plan,
        List<ILogicalExpression> groupExpressions,
        List<ILogicalExpression> aggregateExpressions)
    {
        var groups = groupExpressions.Select(g => g.NormalizeColumn(plan)).ToList();
        var aggregates = aggregateExpressions.Select(g => g.NormalizeColumn(plan)).ToList();

        return Plans.Aggregate.TryNew(plan, groups, aggregates);
    }
    /// <summary>
    /// Builds a filter plan with a set of filter expressions
    /// </summary>
    /// <param name="plan">Parent plan</param>
    /// <param name="expression">Filter expressions</param>
    /// <returns>New filter logical plan</returns>
    internal static ILogicalPlan Filter(this ILogicalPlan plan, ILogicalExpression expression)
    {
        return new Filter(plan, expression.NormalizeColumn(plan));
    }
    /// <summary>
    /// Builds a subquery alias plan
    /// </summary>
    /// <param name="plan">Parent plan</param>
    /// <param name="alias">Subquery alias</param>
    /// <returns>New subquery alias logical plan</returns>
    internal static ILogicalPlan SubqueryAlias(this ILogicalPlan plan, string alias)
    {
        return Plans.SubqueryAlias.TryNew(plan, alias);
    }
    /// <summary>
    /// Builds a join USING plan
    /// </summary>
    /// <param name="left">Join left side logical plan</param>
    /// <param name="right">Join right side logical plan</param>
    /// <param name="joinType">Join type</param>
    /// <param name="usingKeys">Keys in the USING statement</param>
    /// <returns>New join logical plan</returns>
    internal static ILogicalPlan JoinUsing(ILogicalPlan left, ILogicalPlan right, JoinType joinType, List<Column> usingKeys)
    {
        var leftKeys = usingKeys.Select(c => Normalize(left, c)).ToList();
        var rightKeys = usingKeys.Select(c => Normalize(right, c)).ToList();

        var on = leftKeys.Zip(rightKeys).ToList();
        var joinOn = new List<(ILogicalExpression, ILogicalExpression)>();
        ILogicalExpression? filters = null;

        foreach (var (l, r) in on)
        {
            if (left.Schema.HasColumn(l) && right.Schema.HasColumn(r))
            {
                joinOn.Add((l, r));
            }
            else
            {
                var expr = new Binary(l, BinaryOperator.Eq, r);
                filters = filters == null
                    ? expr
                    : new Binary(expr, BinaryOperator.And, filters);
            }
        }

        if (!joinOn.Any() && filters != null)
        {
            var join = new CrossJoin(left, right);
            return join.Filter(filters);
        }

        var joinSchema = left.Schema.Join(right.Schema);

        return new Join(left, right, joinOn, filters, joinType, joinSchema);

        static Column Normalize(ILogicalPlan leftPlan, Column leftColumn)
        {
            return LogicalExtensions.NormalizeJoin(leftPlan, leftColumn);
        }
    }
    /// <summary>
    /// Builds a single UNION ALL plan combining two individual plans
    /// </summary>
    /// <param name="leftPlan">Union left plan</param>
    /// <param name="rightPlan">Union right plan</param>
    /// <returns>Union plan</returns>
    /// <exception cref="InvalidOperationException">Thrown if plan column counts do not match</exception>
    internal static ILogicalPlan Union(ILogicalPlan leftPlan, ILogicalPlan rightPlan)
    {
        var leftCount = leftPlan.Schema.Fields.Count;
        var rightCount = rightPlan.Schema.Fields.Count;

        if (leftCount != rightCount)
        {
            throw new InvalidOperationException("Union queries must have the same number of columns.");
        }

        var unionFields = new List<QualifiedField>();

        for (var i = 0; i < leftCount; i++)
        {
            var leftField = leftPlan.Schema.Fields[i];
            var rightField = rightPlan.Schema.Fields[i];
            // TODO nullable

            if (leftField.DataType != rightField.DataType)
            {
                throw new InvalidOperationException(
                    $"Union column types must be compatible. Column {leftField.Name} is not compatible with column {rightField.Name}");
            }

            var field = new QualifiedField(leftField.Name, leftField.DataType, leftField.Qualifier);
            unionFields.Add(field);
        }

        var unionSchema = new Schema(unionFields);

        var inputs = new List<ILogicalPlan> { leftPlan, rightPlan }
            .SelectMany(plan =>
            {
                if (plan is Union union)
                {
                    return union.Inputs;
                }

                return new List<ILogicalPlan> { plan };
            })
            .Select(input =>
            {
                var plan = input.CoercePlanExpressionsForSchema(unionSchema);

                if (plan is Projection projection)
                {
                    return projection.Plan.ProjectWithColumnIndex(projection.Expression, unionSchema);
                }

                return plan;
            }).ToList();

        if (!inputs.Any())
        {
            throw new InvalidOperationException("Empty UNION expression");
        }

        return new Union(inputs, unionSchema);
    }
    /// <summary>
    /// Builds distinct a single UNION plan merging two individual plans
    /// </summary>
    /// <param name="leftPlan">Union left plan</param>
    /// <param name="rightPlan">Union right plan</param>
    /// <returns>Union plan</returns>
    internal static ILogicalPlan UnionDistinct(ILogicalPlan leftPlan, ILogicalPlan rightPlan)
    {
        var left = leftPlan switch
        {
            Distinct distinct => distinct.Plan,
            _ => leftPlan
        };

        var right = rightPlan switch
        {
            Distinct distinct => distinct.Plan,
            _ => rightPlan
        };

        return new Distinct(Union(left, right));
    }
    /// <summary>
    /// Builds an INTERSECT plan to filter data to the overlap
    /// of two execution plans in the form of a left semi join.
    /// </summary>
    /// <param name="leftPlan">Intersection left plan</param>
    /// <param name="rightPlan">Intersection right plan</param>
    /// <param name="isAll">True if all records are kept; otherwise false</param>
    /// <returns>Join plan</returns>
    internal static ILogicalPlan Intersect(ILogicalPlan leftPlan, ILogicalPlan rightPlan, bool isAll)
    {
        return Overlap(leftPlan, rightPlan, JoinType.LeftSemi, isAll);
    }
    /// <summary>
    /// Builds an EXCEPT plan to filter data to the overlap
    /// of two execution plans in the form of a left anti join.
    /// </summary>
    /// <param name="leftPlan">Exception left plan</param>
    /// <param name="rightPlan">Exception right plan</param>
    /// <param name="isAll">True if all records are rejected; otherwise false</param>
    /// <returns>Join plan</returns>
    internal static ILogicalPlan Except(ILogicalPlan leftPlan, ILogicalPlan rightPlan, bool isAll)
    {
        return Overlap(leftPlan, rightPlan, JoinType.LeftAnti, isAll);
    }
    /// <summary>
    /// Creates a join plan from a left and right plan used when constructing
    /// intersection and exception physical execution plans.
    /// </summary>
    /// <param name="leftPlan">Overlap left plan</param>
    /// <param name="rightPlan">Overlap right plan</param>
    /// <param name="joinType">Type of join being performed</param>
    /// <param name="isAll">True if all records are included; false if records are distinct</param>
    /// <returns>Join execution plan</returns>
    private static ILogicalPlan Overlap(ILogicalPlan leftPlan, ILogicalPlan rightPlan, JoinType joinType, bool isAll)
    {
        var leftCount = leftPlan.Schema.Fields.Count;
        var rightCount = rightPlan.Schema.Fields.Count;

        if (leftCount != rightCount)
        {
            throw new InvalidOperationException("Intersect/Except queries must have the same number of columns.");
        }

        var leftColumns = leftPlan.Schema.Fields.Select(f => new Column(f.Name)).ToList();
        var rightColumns = rightPlan.Schema.Fields.Select(f => new Column(f.Name)).ToList();
        var joinKeys = new JoinKey(leftColumns, rightColumns);

        if (!isAll)
        {
            leftPlan = new Distinct(leftPlan);
        }

        return Join.TryNew(leftPlan, rightPlan, joinType, joinKeys, null);
    }
}