using Prequel.Engine.Core.Logical;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Logical.Expressions;
using Prequel.Engine.Core.Logical.Plans;

namespace Prequel.Engine.Core.Logical.Rules;

/// <summary>
/// Optimization rule to remove unused projections and aggregations
/// from plans to reduce table scans
/// </summary>
internal class PushDownProjectionRule : ILogicalPlanOptimizationRule
{
    public ApplyOrder ApplyOrder => ApplyOrder.TopDown;
    /// <summary>
    /// Optimizes plan steps by collecting and evaluating plan step expressions
    /// and replacing redundant or unused plans.
    /// </summary>
    /// <param name="plan">Plan to optimize</param>
    /// <returns>Plan after optimization</returns>
    public ILogicalPlan? TryOptimize(ILogicalPlan plan)
    {
        switch (plan)
        {
            case Aggregate aggregate:
                {
                    var requiredColumns = new HashSet<Column>();
                    foreach (var e in aggregate.AggregateExpressions.Concat(aggregate.GroupExpressions))
                    {
                        e.ExpressionToColumns(requiredColumns);
                    }

                    var newExpression = GetExpressions(requiredColumns, aggregate.Plan.Schema);
                    var childProjection = Projection.TryNew(aggregate.Plan, newExpression);

                    var optimizedChild = TryOptimize(childProjection);

                    var newInputs = new List<ILogicalPlan>();

                    if (optimizedChild != null)
                    {
                        newInputs.Add(optimizedChild);
                    }

                    return (ILogicalPlanParent)plan.WithNewInputs(newInputs);
                }
            case TableScan { Projection: null } scan:
                return PushDownScan(new HashSet<Column>(), scan);
        }

        if (plan is not Projection projection)
        {
            return null;
        }

        return FromChildPlan(projection.Plan, plan);
    }
    /// <summary>
    /// Generates a new projection plan where optimizations can be made.
    /// Redundant projections will be eliminated in a subsequent rule. 
    /// </summary>
    /// <param name="childPlan">Plan to optimize</param>
    /// <param name="projection">Projection with expressions to use during optimization</param>
    /// <returns>Optimized plan</returns>
    private ILogicalPlan? FromChildPlan(ILogicalPlan childPlan, ILogicalPlan projection)
    {
        var empty = !projection.GetExpressions().Any();

        switch (childPlan)
        {
            case Projection proj:
                {
                    var replaceMap = CollectProjectionExpressions(proj);

                    var newExpressions = projection.GetExpressions()
                        .Select(e => ReplaceColumnsByName(e, replaceMap))
                        .Select((e, i) =>
                        {
                            var parentName = projection.Schema.Fields[i].QualifiedName;

                            return e.CreateLogicalName() == parentName ? e : new Alias(e, parentName);
                        })
                        .ToList();

                    return TryOptimize(new Projection(proj.Plan, newExpressions, projection.Schema));
                }
            case Aggregate aggregate:
                {
                    var requiredColumns = new HashSet<Column>();
                    projection.GetExpressions().ExpressionListToColumns(requiredColumns);
                    var newAggregate = (
                        from agg in aggregate.AggregateExpressions
                        let col = new Column(agg.CreateLogicalName())
                        where requiredColumns.Contains(col)
                        select agg).ToList();

                    if (!newAggregate.Any() && aggregate.AggregateExpressions.Count == 1)
                    {
                        throw new InvalidOperationException();
                    }

                    var newAgg = Aggregate.TryNew(aggregate.Plan, aggregate.GroupExpressions, newAggregate);

                    return GeneratePlan(empty, projection, newAgg);
                }
            case Filter filter:
                {
                    if (CanEliminate(projection, childPlan.Schema))
                    {
                        // should projection be 'plan'?
                        var newProj = projection.WithNewInputs([filter.Plan]);
                        return childPlan.WithNewInputs([newProj]);
                    }
                    var requiredColumns = new HashSet<Column>();
                    projection.GetExpressions().ExpressionListToColumns(requiredColumns);
                    new List<ILogicalExpression> { filter.Predicate }.ExpressionListToColumns(requiredColumns);

                    var newExpression = GetExpressions(requiredColumns, filter.Plan.Schema);
                    var newProjection = Projection.TryNew(filter.Plan, newExpression);
                    var newFilter = childPlan.WithNewInputs([newProjection]);

                    return GeneratePlan(empty, projection, newFilter);
                }
            case TableScan tableScan:
                {
                    var usedColumns = new HashSet<Column>();

                    var scanExpressions = projection.GetExpressions();
                    if (scanExpressions.Count == 0)
                    {
                        usedColumns.Add(tableScan.Schema.Fields[0].QualifiedColumn());
                        return PushDownScan(usedColumns, tableScan);
                    }

                    foreach (var expr in scanExpressions)
                    {
                        expr.ExpressionToColumns(usedColumns);
                    }
                    var scan = PushDownScan(usedColumns, tableScan);

                    return projection.WithNewInputs([scan]);
                }
            case SubqueryAlias subquery:
                {
                    var replaceMap = GenerateColumnReplaceMap(subquery);
                    var requiredColumns = new HashSet<Column>();
                    var expr = ((Projection)projection).Expression;

                    expr.ExpressionListToColumns(requiredColumns);

                    var newRequiredColumns = requiredColumns.Select(c => replaceMap[c]).ToList();
                    var newExpression = GetExpressions(newRequiredColumns, subquery.Plan.Schema);
                    var newProjection = Projection.TryNew(subquery.Plan, newExpression);
                    var newAlias = childPlan.WithNewInputs([newProjection]);

                    return GeneratePlan(empty, projection, newAlias);
                }
            case Join join:
                {
                    var pushColumns = new HashSet<Column>();
                    foreach (var expr in projection.GetExpressions())
                    {
                        expr.ExpressionToColumns(pushColumns);
                    }

                    foreach (var (left, right) in join.On)
                    {
                        left.ExpressionToColumns(pushColumns);
                        right.ExpressionToColumns(pushColumns);
                    }

                    join.Filter?.ExpressionToColumns(pushColumns);

                    var newLeft = GenerateProjection(pushColumns, join.Plan.Schema, join.Plan);
                    var newRight = GenerateProjection(pushColumns, join.Right.Schema, join.Right);
                    var newJoin = childPlan.WithNewInputs([newLeft, newRight]);

                    return GeneratePlan(empty, projection, newJoin);
                }
            case Sort sort:
                {
                    if (CanEliminate(projection, childPlan.Schema))
                    {
                        var newProjection = projection.WithNewInputs([sort.Plan]);
                        return childPlan.WithNewInputs([newProjection]);
                    }
                    else
                    {
                        var requiredColumns = new HashSet<Column>();
                        projection.GetExpressions().ExpressionListToColumns(requiredColumns);
                        sort.OrderByExpressions.ExpressionListToColumns(requiredColumns);
                        var newExpression = GetExpressions(requiredColumns, sort.Plan.Schema);
                        var newProjection = Projection.TryNew(sort.Plan, newExpression);
                        var newSort = childPlan.WithNewInputs([newProjection]);

                        return GeneratePlan(empty, projection, newSort);
                    }
                }
            case CrossJoin crossJoin:
                {
                    var columns = new HashSet<Column>();
                    projection.GetExpressions().ExpressionListToColumns(columns);

                    var newLeft = GenerateProjection(columns, crossJoin.Plan.Schema, crossJoin.Plan);
                    var newRight = GenerateProjection(columns, crossJoin.Right.Schema, crossJoin.Right);
                    var newJoin = childPlan.WithNewInputs([newLeft, newRight]);

                    return GeneratePlan(empty, projection, newJoin);
                }
            case Union union:
                {
                    var requiredColumns = new HashSet<Column>();
                    projection.GetExpressions().ExpressionListToColumns(requiredColumns);
                    var unionFields = union.Schema.Fields;

                    // When there is no projection, we need to add the first column to the projection
                    // Because if push empty down, children may output different columns.
                    if (!requiredColumns.Any())
                    {
                        requiredColumns.Add(unionFields[0].QualifiedColumn());
                    }

                    var projectionColumnExpressions = GetExpressions(requiredColumns, union.Schema);
                    var inputs = new List<ILogicalPlan>(union.Inputs.Count);

                    foreach (var input in union.Inputs)
                    {
                        var replaceMap = new Dictionary<string, ILogicalExpression>();
                        var inputFields = input.Schema.Fields;

                        for (var i = 0; i < inputFields.Count; i++)
                        {
                            replaceMap.Add(unionFields[i].QualifiedName, inputFields[i].QualifiedColumn());
                        }

                        var expressions = projectionColumnExpressions.Select(e => ReplaceColumnsByName(e, replaceMap)).ToList();
                        inputs.Add(Projection.TryNew(input, expressions));
                    }

                    var schema = new Schema(projectionColumnExpressions.ExpressionListToFields(childPlan));
                    var newUnion = new Union(inputs, schema);

                    return GeneratePlan(empty, projection, newUnion);
                }

            default:
                return null;
                //throw new NotImplementedException("FromChildPlan plan type not implemented yet");
        }
    }
    /// <summary>
    /// Generates a projection from a schema using qualified fields as the columns
    /// </summary>
    /// <param name="usedColumns">Columns used in the projection</param>
    /// <param name="schema">Schema containing the plan's columns</param>
    /// <param name="plan">Plan being surrounded in a new projection</param>
    /// <returns></returns>
    private static ILogicalPlan GenerateProjection(IReadOnlySet<Column> usedColumns, Schema schema, ILogicalPlan plan)
    {
        var columns = schema.Fields.Select(f =>
        {
            var column = f.QualifiedColumn();
            if (usedColumns.Contains(column))
            {
                return (ILogicalExpression)column;
            }

            return null;
        })
        .Where(c => c != null)
        .ToList();

        return Projection.TryNew(plan, columns!);
    }
    /// <summary>
    /// Generates a dictionary of columns as both keys and values.  The dictionary
    /// acts as a map of columns that will be replaced.
    /// </summary>
    /// <param name="alias">SubqueryAlias instance to extract plan schema fields</param>
    /// <returns>Column replacement map</returns>
    private static Dictionary<Column, Column> GenerateColumnReplaceMap(SubqueryAlias alias)
    {
        return alias.Plan.Schema.Fields.Select((f, i) =>
            (
                alias.Schema.Fields[i].QualifiedColumn(),
                f.QualifiedColumn())
            )
            .ToDictionary(d => d.Item1, d => d.Item2);
    }
    /// <summary>
    /// Gets all projection expressions as a dictionary of names and expressions
    /// </summary>
    /// <param name="projection"></param>
    /// <returns></returns>
    private static Dictionary<string, ILogicalExpression> CollectProjectionExpressions(Projection projection)
    {
        return projection.Schema.Fields.SelectMany((f, i) =>
        {
            var expr = projection.Expression[i] switch
            {
                Alias alias => alias.Expression,
                _ => projection.Expression[i]
            };

            var projections = new List<(string Name, ILogicalExpression Expr)>
            {
                (f.Name, Expr: expr),
                (f.QualifiedColumn().FlatName, Expr: expr)
            };

            return projections.Distinct();
        })
        .ToDictionary(f => f.Name, f => f.Expr);
    }
    /// <summary>
    /// Replaces columns by name using the replacement column map
    /// </summary>
    /// <param name="expression">Expressions to replace column expressions</param>
    /// <param name="replaceMap">Map of existing columns and their replacements</param>
    /// <returns>Expression with column expressions replaced</returns>
    private static ILogicalExpression ReplaceColumnsByName(ILogicalExpression expression, Dictionary<string, ILogicalExpression> replaceMap)
    {
        return expression.Transform(expression, e =>
        {
            if (e is Column c)
            {
                return replaceMap[c.Name];
            }

            return expression;
        });
    }
    /// <summary>
    /// Creates a push down table scan limiting the columns needed during table scanning.
    /// This allows table scanning to only retrieve relevant columns when data is
    /// read from the data source
    /// </summary>
    /// <param name="usedColumns">Projection columns used in the table scan</param>
    /// <param name="tableScan">Table scan to optimize</param>
    /// <returns>Optimized table scan with updated schema and projection values</returns>
    private static ILogicalPlan PushDownScan(IEnumerable<Column> usedColumns, TableScan tableScan)
    {
        var projection = usedColumns
            .Where(c => c.Relation == null || c.Relation.Name == tableScan.Name)
            .Select(c =>
                {
                    var index = tableScan.Table.Schema!.IndexOfColumn(c);
                    if (index == null)
                    {
                        return -1;
                    }
                    return index.Value;
                })
            .Where(i => i > -1)
            .ToList();

        var fields = projection
            .Select(i => tableScan.Table.Schema!.Fields[i].FromQualified(new TableReference(tableScan.Name)))
            .ToList();

        var schema = new Schema(fields);

        return tableScan with { Schema = schema, Projection = projection };
    }
    /// <summary>
    /// Generates a new plan with updated inputs
    /// </summary>
    /// <param name="empty">True if the projection has no expressions</param>
    /// <param name="plan">Plan to recreate with new inputs</param>
    /// <param name="newPlan">Newly created plan to use when no expressions exist</param>
    /// <returns>Optimized plan</returns>
    private static ILogicalPlan GeneratePlan(bool empty, ILogicalPlan plan, ILogicalPlan newPlan)
    {
        return empty ? newPlan : plan.WithNewInputs([newPlan]);
    }
    /// <summary>
    /// Checks if a projection can be eliminated
    /// </summary>
    /// <param name="projection">Projection to check</param>
    /// <param name="schema">Schema to compare the projection expressions against</param>
    /// <returns>True if the projection can be eliminated; otherwise false.</returns>
    private static bool CanEliminate(ILogicalPlan projection, Schema schema)
    {
        var expressions = projection.GetExpressions();
        if (expressions.Count != schema.Fields.Count)
        {
            return false;
        }

        for (var i = 0; i < expressions.Count; i++)
        {
            var expr = expressions[i];

            if (expr is Column c)
            {
                var field = schema.Fields[i];
                if (c.Name != field.Name)
                {
                    return false;
                }

            }
            else
            {
                return false;
            }
        }

        return true;
    }
    /// <summary>
    /// Gets all qualified columns from a schema that also exist in the column list
    /// </summary>
    /// <param name="columns">Column list to filter the schema field qualified columns</param>
    /// <param name="schema">Schema used to retrieve qualified columns</param>
    /// <returns>List of expressions in the form of qualified columns</returns>
    private static List<ILogicalExpression> GetExpressions(IEnumerable<Column> columns, Schema schema)
    {
        return schema.Fields.Select(f => (ILogicalExpression)f.QualifiedColumn())
            .Where(columns.Contains)
            .ToList();
    }
}