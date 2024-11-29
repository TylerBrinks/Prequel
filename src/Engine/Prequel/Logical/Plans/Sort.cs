using Prequel.Data;
using Prequel.Logical.Expressions;

namespace Prequel.Logical.Plans;

/// <summary>
/// Logical sort plan
/// </summary>
/// <param name="Plan">Parent plan</param>
/// <param name="OrderByExpressions">Expressions used to order the output data</param>
internal record Sort(ILogicalPlan Plan, List<ILogicalExpression> OrderByExpressions) : ILogicalPlanParent
{
    /// <summary>
    /// Pass through from the parent plan's schema
    /// </summary>
    public Schema Schema => Plan.Schema;

    /// <summary>
    /// Creates a new Sort logical plan by rewriting aggregate expressions, filling in missing
    /// columns, normalizing columns in the sort, and creating a new projection to wrap
    /// the sort operation.
    /// </summary>
    /// <param name="plan">Parent plan</param>
    /// <param name="orderByExpressions">Expressions used to order the output data</param>
    /// <returns>New sort plan instance</returns>
    internal static ILogicalPlan TryNew(ILogicalPlan plan, List<ILogicalExpression> orderByExpressions)
    {
        var expressions = RewriteByAggregates(orderByExpressions, plan);

        var missingColumns = new HashSet<Column>();

        var capturedPlan = plan;
        var missingExpressions = expressions.Select(expr => expr.ToColumns())
            .SelectMany(columns => columns.Where(column => capturedPlan.Schema.GetFieldFromColumn(column) == null));

        foreach (var column in missingExpressions)
        {
            missingColumns.Add(column);
        }

        if (!missingColumns.Any())
        {
            return new Sort(plan, expressions);
        }

        var newExpressions = plan.Schema.Fields.Select(f => (ILogicalExpression)f.QualifiedColumn()).ToList();

        plan = plan.AddMissingColumns(missingColumns, false);

        var sort = new Sort(plan, expressions.NormalizeColumn(plan));

        return new Projection(sort, newExpressions, plan.Schema);
    }
    /// <summary>
    /// Rewrites all order by expressions by transforming columns based on 
    /// qualified or calculated column name
    /// </summary>
    /// <param name="orderByExpressions">Order expressions to rewrite</param>
    /// <param name="plan">Plan containing</param>
    /// <returns>List of rewritten logical expressions</returns>
    private static List<ILogicalExpression> RewriteByAggregates(IEnumerable<ILogicalExpression> orderByExpressions, ILogicalPlan plan)
    {
        return orderByExpressions.Select(e =>
        {
            if (e is OrderBy order)
            {
                return order with { Expression = RewriteByAggregates(order.Expression, plan) };
            }

            return e;

        }).ToList();
    }
    /// <summary>
    /// Rewrites an expression for plans with a single input
    /// </summary>
    /// <param name="expression">Expression to rewrite</param>
    /// <param name="plan">Plan containing inputs and expressions</param>
    /// <returns>Rewritten expression</returns>
    private static ILogicalExpression RewriteByAggregates(ILogicalExpression expression, ILogicalPlan plan)
    {
        var inputs = plan.GetInputs();

        if (inputs.Count != 1)
        {
            return expression;
        }

        var projectedExpressions = plan.GetExpressions();

        return RewriteForProjection(expression, projectedExpressions, inputs[0].Schema);
    }
    /// <summary>
    /// Transforms expressions to columns using a qualified column if the projection field exists in
    /// the schema.  Otherwise, names are calculated used to query instead
    /// </summary>
    /// <param name="expression">Expression to transform</param>
    /// <param name="projectionExpressions">Projection expressions</param>
    /// <param name="schema">Schema containing fields to query for qualified fields</param>
    /// <returns>Rewritten expression as a Column instance</returns>
    private static ILogicalExpression RewriteForProjection(
        ILogicalExpression expression,
        List<ILogicalExpression> projectionExpressions,
        Schema schema)
    {
        return expression.Transform(expression, e =>
        {
            var found = projectionExpressions.Find(ex => ex == expression);
            if (found != null)
            {
                var column = found.ToField(schema).QualifiedColumn();
                return column;
            }

            var name = expression.CreateLogicalName();
            var searchColumn = new Column(name);

            var foundMatch = projectionExpressions.Find(c => searchColumn == c as Column);
            if (foundMatch != null)
            {
                //TODO cast & try cast
                return foundMatch;
            }

            return e;
        });
    }


    public string ToStringIndented(Indentation? indentation = null)
    {
        var indent = indentation ?? new Indentation();
        return $"Sort: {indent.Next(Plan)}";
    }
}