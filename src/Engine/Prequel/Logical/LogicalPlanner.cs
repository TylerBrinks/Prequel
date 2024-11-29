using Prequel.Logical.Expressions;
using Prequel.Logical.Plans;
using SqlParser.Ast;

namespace Prequel.Logical;

internal class LogicalPlanner
{
    /// <summary>
    /// Creates a logical execution plan from a Select expression
    /// </summary>
    /// <param name="select">Select expression to build into a logical plan</param>
    /// <param name="context">Plan execution context</param>
    /// <returns>Logical execution plan</returns>
    public static ILogicalPlan CreateLogicalPlan(Select select, PlannerContext context)
    {
        // Logical plans are rooted in scanning a table for values
        var plan = select.From.PlanTableWithJoins(context);

        // Wrap the scan in a filter if a where clause exists
        plan = select.Selection.PlanFromSelection(plan, context);

        // Build a select plan converting each select item into a logical expression
        var selectExpressions = select.Projection.PrepareSelectExpressions(plan, plan is EmptyRelation, context);

        var projectedPlan = plan.PlanProjection(selectExpressions);

        var combinedSchemas = projectedPlan.Schema;
        combinedSchemas.MergeSchemas(plan.Schema);

        var aliasMap = selectExpressions.Where(e => e is Alias).Cast<Alias>().ToDictionary(a => a.Name, a => a.Expression);

        var havingExpression = select.Having.MapHaving(combinedSchemas, aliasMap, context);

        var aggregateExpressionList = selectExpressions.ToList();
        if (havingExpression != null)
        {
            aggregateExpressionList.Add(havingExpression);
        }

        var aggregateExpressions = aggregateExpressionList.FindAggregateExpressions();

        // check group by expressions inside FindGroupByExpressions, select.rs.line 130
        var groupByExpressions = select.GroupBy.FindGroupByExpressions(
            selectExpressions,
            combinedSchemas,
            projectedPlan,
            aliasMap,
            context);

        List<ILogicalExpression>? selectPostAggregate;
        ILogicalExpression? havingPostAggregate;

        if (groupByExpressions.Any() || aggregateExpressions.Any())
        {
            // Wrap the plan in an aggregation
            (plan, selectPostAggregate, havingPostAggregate) = plan.CreateAggregatePlan(
                selectExpressions, havingExpression, groupByExpressions, aggregateExpressions);
        }
        else
        {
            selectPostAggregate = selectExpressions;
            if (havingExpression != null)
            {
                throw new InvalidOperationException("HAVING clause must appear in the GROUP BY clause or be used in an aggregate function.");
            }

            havingPostAggregate = null;
        }

        if (havingPostAggregate != null)
        {
            plan = new Filter(plan, havingPostAggregate);
        }

        // Wrap the plan in a projection
        plan = plan.PlanProjection(selectPostAggregate);

        if (select.Distinct is DistinctFilter.Distinct)
        {
            plan = new Distinct(plan);
        }

        return plan;
    }
}