using Prequel.Engine.Core.Logical.Plans;
using Prequel.Engine.Core.Logical.Rules;
using Prequel.Engine.Core;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Logical.Expressions;
using Prequel.Engine.Core.Logical.Plans;
using SqlParser.Ast;
using Join = Prequel.Engine.Core.Logical.Plans.Join;

namespace Prequel.Engine.Core.Logical.Rules;

/// <summary>
/// Optimization rule for rewriting subquery filters to joins
/// </summary>
internal class ScalarSubqueryToJoinRule : ILogicalPlanOptimizationRule
{
    private readonly NumericAliasGenerator _aliasGenerator = new();

    public ApplyOrder ApplyOrder => ApplyOrder.TopDown;
    /// <summary>
    /// Recursively fnds expressions that have a scalar value 
    /// </summary>
    /// <param name="plan">Distinct plan to optimize</param>
    /// <returns>Optimized aggregation</returns>
    public ILogicalPlan? TryOptimize(ILogicalPlan plan)
    {
        switch (plan)
        {
            case Filter filter:
                {
                    var (subqueries, expr) = ExtractSubqueryExpressions(filter.Predicate);

                    if (!subqueries.Any())
                    {
                        return null;
                    }

                    var currentInput = filter.Plan;

                    foreach (var (subquery, alias) in subqueries)
                    {
                        var optimizedSubquery = OptimizeScalar(subquery, currentInput, alias);

                        if (optimizedSubquery != null)
                        {
                            currentInput = optimizedSubquery;
                        }
                        else
                        {
                            return null;
                        }
                    }

                    return currentInput.Filter(expr);
                }
            case Projection projection:
                {
                    var allSubqueries = new List<(ScalarSubquery, string)>();
                    var rewriteExpressions = new List<ILogicalExpression>();

                    foreach (var expression in projection.Expression)
                    {
                        var (subqueries, expr) = ExtractSubqueryExpressions(expression);
                        allSubqueries.AddRange(subqueries);
                        rewriteExpressions.Add(expr);
                    }

                    if (!allSubqueries.Any())
                    {
                        return null;
                    }

                    var currentInput = projection.Plan;

                    foreach (var (subquery, alias) in allSubqueries)
                    {
                        var optimizedSubquery = OptimizeScalar(subquery, currentInput!, alias);

                        currentInput = optimizedSubquery;
                    }

                    return currentInput!.PlanProjection(rewriteExpressions);
                }
            default:
                return null;
        }
    }
    /// <summary>
    /// Rewrites a predicate into a list of scalar subquery and string objects
    /// </summary>
    /// <param name="predicate"></param>
    /// <returns></returns>
    private (List<(ScalarSubquery, string)>, ILogicalExpression) ExtractSubqueryExpressions(ILogicalExpression predicate)
    {
        var extract = new ExtractScalarSubqueryRewriter(_aliasGenerator);

        var newExpressions = predicate.Rewrite(extract);

        return (extract.SubqueryInfo, newExpressions);
    }
    /// <summary>
    /// Creates a join plan from the subquery plan
    /// </summary>
    /// <param name="subquery">Subquery to convert to a join plan</param>
    /// <param name="filterInput">Join filter plan</param>
    /// <param name="subqueryAlias">Subquery alias used for field lookup between the outer and inner queries</param>
    /// <returns>Optimized join plan</returns>
    private static ILogicalPlan? OptimizeScalar(ScalarSubquery subquery, ILogicalPlan filterInput, string subqueryAlias)
    {
        var subqueryPlan = subquery.Plan;
        var projection = subqueryPlan switch
        {
            Projection p => p,
            _ => null
        };

        if (projection == null)
        {
            return null;
        }

        var projExpression = projection.Expression.SingleOrDefault();

        if (projExpression == null)
        {
            throw new InvalidOperationException("Exactly one expression should be projected");
        }

        var projAlias = new Alias(projExpression, "__value");
        var subInputs = subqueryPlan.GetInputs();
        var subInput = subInputs.SingleOrDefault();

        if (subInput == null)
        {
            throw new InvalidOperationException("Exactly one expression should be projected");
        }

        var aggregate = subInput switch
        {
            Aggregate a => a,
            _ => throw new InvalidOperationException("Cannot translate scalar sub-query to a join")
        };

        var (joinFilters, subqueryInput) = ExtractJoinFilters(aggregate.Plan);

        // Only operate if one column is present and the other closed upon from outside scope
        var inputSchema = subqueryInput.Schema;
        var subqueryColumns = CollectSubqueryColumns(joinFilters, inputSchema);
        var joinFilter = Conjunction(joinFilters);

        if (joinFilter != null)
        {
            joinFilter = ReplaceQualifiedName(joinFilter, subqueryColumns, subqueryAlias);
        }

        var groupBy = subqueryColumns.Select(c => (ILogicalExpression)new Column(c.Name, c.Relation)).ToList();
        subqueryPlan = subqueryInput;
        var proj = groupBy.Concat(new ILogicalExpression[] { projAlias }).ToList();

        subqueryPlan = subqueryPlan
            .Aggregate(groupBy, aggregate.AggregateExpressions)
            .Project(proj)
            .SubqueryAlias(subqueryAlias);

        ILogicalPlan newPlan;

        if (joinFilter == null)
        {
            newPlan = filterInput is EmptyRelation
                ? subqueryPlan
                : new CrossJoin(filterInput, subqueryPlan);
        }
        else
        {
            newPlan = Join.TryNew(
                filterInput,
                subqueryPlan,
                JoinType.Left,
                new([], []),
                joinFilter);
        }

        return newPlan;
    }
    /// <summary>
    /// Extracts join filter expressions from a logical filter plan
    /// </summary>
    /// <param name="filter">Filter expression</param>
    /// <returns>List of join filters along with the filter's parent plan</returns>
    internal static (List<ILogicalExpression>, ILogicalPlan) ExtractJoinFilters(ILogicalPlan filter)
    {
        if (filter is Filter planFilter)
        {
            var subqueryFilterExpressions = planFilter.Predicate.SplitConjunction();
            var (joinFilters, subqueryFilters) = FindJoinExpressions(subqueryFilterExpressions);

            var plan = planFilter.Plan;

            var expr = Conjunction(subqueryFilters);

            if (expr != null)
            {
                plan = plan.Filter(expr);
            }

            return (joinFilters, plan);
        }

        return ([], filter);
    }
    /// <summary>
    /// Creates a binary expressions from a filter by aggregating all filters
    /// with an AND clause
    /// </summary>
    /// <param name="filters">Filters to convert into binary AND expressions</param>
    /// <returns>Binary expression hierarchy</returns>
    internal static ILogicalExpression? Conjunction(List<ILogicalExpression> filters)
    {
        return filters.Any()
            ? filters.Aggregate((left, right) => new Binary(left, BinaryOperator.And, right))
            : null;
    }
    /// <summary>
    /// Finds join expressions in the form of outer reference columns
    /// </summary>
    /// <param name="expressions">Expression to search for joins</param>
    /// <returns>Join expressions along with the remaining expression</returns>
    internal static (List<ILogicalExpression>, List<ILogicalExpression>) FindJoinExpressions(List<ILogicalExpression> expressions)
    {
        var joins = new List<ILogicalExpression>();
        var others = new List<ILogicalExpression>();

        foreach (var filter in expressions)
        {
            if (FindExpressionsInExpression(filter, nested => nested is OuterReferenceColumn).Any())
            {
                var match = filter is Binary { Op: BinaryOperator.Eq } b && b.Left == b.Right;
                if (!match)
                {
                    joins.Add(StripOuterReference(filter));
                }
            }
            else
            {
                others.Add(filter);
            }
        }

        return (joins, others);
    }
    /// <summary>
    /// Returns an outer reference's column if the expression is an outer reference.
    /// Otherwise, returns the input expression.
    /// </summary>
    /// <param name="expression">Expression to check and potentially unwrap</param>
    /// <returns>Logical expression</returns>
    internal static ILogicalExpression StripOuterReference(ILogicalExpression expression)
    {
        return expression.Transform(expression, expr =>
        {
            if (expr is OuterReferenceColumn o)
            {
                return o.Column;
            }

            return expr;
        });
    }
    /// <summary>
    /// Recursively collects child expressions contained within the 
    /// results of a supplied callback function 
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="testFn"></param>
    /// <returns></returns>
    internal static IEnumerable<ILogicalExpression> FindExpressionsInExpression(
        ILogicalExpression expression,
        Func<ILogicalExpression, bool> testFn)
    {
        var expressions = new List<ILogicalExpression>();

        expression.Apply(expr =>
        {
            var logicalExpr = (ILogicalExpression)expr;

            if (testFn(logicalExpr))
            {
                if (!expressions.Contains(expr))
                {
                    expressions.Add(StripOuterReference(logicalExpr));
                }

                return VisitRecursion.Skip;
            }

            return VisitRecursion.Continue;
        });

        return expressions;
    }
    /// <summary>
    /// Gets all columns contained in the subquery schema
    /// </summary>
    /// <param name="expressions">List of expressions to check against the schema</param>
    /// <param name="subquerySchema">Schema of the subquery</param>
    /// <returns>List of columns in the subquery</returns>
    internal static List<Column> CollectSubqueryColumns(List<ILogicalExpression> expressions, Schema subquerySchema)
    {
        var allColumns = new List<Column>();

        foreach (var expr in expressions)
        {
            var usedColumns = expr.ToColumns().Where(subquerySchema.HasColumn).ToList();

            allColumns.AddRange(usedColumns);
        }

        return allColumns;
    }
    /// <summary>
    /// Builds a map of columns with a subquery alias and replaces the supplied
    /// column with a mapped column if a match is found.
    /// </summary>
    /// <param name="expression">Expression to interrogate and potentially replace</param>
    /// <param name="columns">Columns to use when building a subquery alias map</param>
    /// <param name="subqueryAlias">Query alias</param>
    /// <returns>Replaced logical expression</returns>
    internal static ILogicalExpression ReplaceQualifiedName(
        ILogicalExpression expression,
        List<Column> columns,
        string subqueryAlias)
    {
        var aliasColumns = columns.Select(c => Column.FromQualifiedName($"{subqueryAlias}.{c.Name}")).ToList();

        var replaceMap = columns.Zip(aliasColumns).ToDictionary(c => c.First, c => c.Second);

        return ReplaceColumn(expression, replaceMap);
    }
    /// <summary>
    /// Replaces an expression column with a column from the map
    /// </summary>
    /// <param name="expression">Expression to interrogate and potentially replace</param>
    /// <param name="replaceMap">Column replace map</param>
    /// <returns>Replaced logical expression</returns>
    internal static ILogicalExpression ReplaceColumn(ILogicalExpression expression, Dictionary<Column, Column> replaceMap)
    {
        return expression.Transform(expression, expr =>
        {
            if (expr is Column c)
            {
                return replaceMap.TryGetValue(c, out var lambda) ? lambda : expr;
            }

            return expr;
        });
    }
}