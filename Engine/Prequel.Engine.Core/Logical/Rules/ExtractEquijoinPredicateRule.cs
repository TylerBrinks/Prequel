using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Logical.Expressions;
using Prequel.Engine.Core.Logical.Plans;
using SqlParser.Ast;
using Join = Prequel.Engine.Core.Logical.Plans.Join;

namespace Prequel.Engine.Core.Logical.Rules;

/// <summary>
/// Optimization rule to extract join predicates into predicates
/// </summary>
internal class ExtractEquijoinPredicateRule : ILogicalPlanOptimizationRule
{
    public ApplyOrder ApplyOrder => ApplyOrder.BottomUp;

    /// <summary>
    /// Optimizes plan execution by converting a JOIN ON filter on AND clauses
    /// and reforming the join with optimized ON and filter expressions 
    /// </summary>
    /// <param name="plan">Plan to optimize</param>
    /// <returns>Plan after optimization</returns>
    public ILogicalPlan? TryOptimize(ILogicalPlan plan)
    {
        if (plan is not Join join)
        {
            return null;
        }

        if (join.Filter == null)
        {
            return null;
        }

        var leftSchema = join.Plan.Schema;
        var rightSchema = join.Right.Schema;

        var (equijoinPredicates, nonEquijoinExpression) = SplitJoinPredicate(join.Filter!, leftSchema, rightSchema);

        if (!equijoinPredicates.Any())
        {
            return join;
        }

        var newOn = join.On.ToList();
        newOn.AddRange(equijoinPredicates);

        return join with { On = newOn, Filter = nonEquijoinExpression };
    }
    /// <summary>
    /// Where possible, splits a binary equality expressions and creates join keys
    /// from the left and right hand side expressions
    /// </summary>
    /// <param name="filter">Join filter to optimize</param>
    /// <param name="leftSchema">Join left side schema</param>
    /// <param name="rightSchema">Join right side schema</param>
    /// <returns>
    /// Lists of predicate join keys for optimization and
    /// non-predicate expressions that cannot be optimized</returns>
    private static (List<(ILogicalExpression, ILogicalExpression)> Predicates, ILogicalExpression? Expression)
        SplitJoinPredicate(ILogicalExpression filter, Schema leftSchema, Schema rightSchema)
    {
        var expressions = filter.SplitConjunction();

        var joinKeys = new List<(ILogicalExpression, ILogicalExpression)>();
        var filters = new List<ILogicalExpression>();

        foreach (var expr in expressions)
        {
            if (expr is Binary { Op: BinaryOperator.Eq } b)
            {
                var left = b.Left;
                var right = b.Right;

                var (leftExpr, rightExpr) = FindValidEquijoinKeyPair(left, right, leftSchema, rightSchema);

                if (leftExpr != null && rightExpr != null)
                {
                    joinKeys.Add((leftExpr, rightExpr));
                }
                else
                {
                    filters.Add(expr);
                }
            }
            else
            {
                filters.Add(expr);
            }
        }

        ILogicalExpression? resultFilter = null;
        if (filters.Any())
        {
            resultFilter = filters.Aggregate((left, right) => new Binary(left, BinaryOperator.And, right));
        }

        return (joinKeys, resultFilter);
    }
    /// <summary>
    /// Finds key equijoin key pairs
    /// </summary>
    /// <param name="leftKey">Predicate left side expression key</param>
    /// <param name="rightKey">Predicate right side expression key</param>
    /// <param name="leftSchema">Left side schema</param>
    /// <param name="rightSchema">Right side schema</param>
    /// <returns>Left and right join key expressions</returns>
    private static (ILogicalExpression?, ILogicalExpression?) FindValidEquijoinKeyPair(
        ILogicalExpression leftKey,
        ILogicalExpression rightKey,
        Schema leftSchema,
        Schema rightSchema)
    {
        var leftUsingColumns = leftKey.ToColumns();
        var rightUsingColumns = rightKey.ToColumns();

        if (!leftUsingColumns.Any() || !rightUsingColumns.Any())
        {
            return (null, null);
        }

        var leftIsLeft = CheckAllColumnsFromSchema(leftUsingColumns, leftSchema);
        var rightIsRight = CheckAllColumnsFromSchema(rightUsingColumns, rightSchema);

        return (leftIsLeft, rightIsRight) switch
        {
            (true, true) => (leftKey, rightKey),
            (_, _) when IsSwapped() => (rightKey, leftKey),

            _ => (null, null)
        };

        bool IsSwapped()
        {
            return CheckAllColumnsFromSchema(rightUsingColumns, leftSchema) &&
                   CheckAllColumnsFromSchema(leftUsingColumns, rightSchema);
        }
    }
    /// <summary>
    /// Checks the index of all columns to determine if each column exists in the schema.
    /// </summary>
    /// <param name="columns">Columns to check against the schema</param>
    /// <param name="schema">Schema used to verify column indices</param>
    /// <returns>True if all columns exist in the schema; otherwise false.</returns>
    private static bool CheckAllColumnsFromSchema(IEnumerable<Column> columns, Schema schema)
    {
        return columns
            .Select(column => schema.IndexOfColumn(column) != null)
            .All(exists => exists);
    }
}