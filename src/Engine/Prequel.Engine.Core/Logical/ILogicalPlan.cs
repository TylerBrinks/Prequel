using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Logical.Expressions;
using Prequel.Engine.Core.Logical.Plans;
using SqlParser.Ast;
using Join = Prequel.Engine.Core.Logical.Plans.Join;

namespace Prequel.Engine.Core.Logical;

public interface ILogicalPlan
{
    /// <summary>
    /// Plan's schema
    /// </summary>
    Schema Schema { get; }
    /// <summary>
    /// Plan's USING columns
    /// </summary>
    List<HashSet<Column>> UsingColumns => [];

    string ToStringIndented(Indentation? indentation = null);
    /// <summary>
    /// Gets the plans (inputs) that supply the current plan 
    /// </summary>
    /// <returns>List of plan inputs; empty if none exist</returns>
    List<ILogicalPlan> GetInputs()
    {
        return this switch
        {
            Join join => [join.Plan, join.Right],
            CrossJoin crossJoin => [crossJoin.Plan, crossJoin.Right],
            Union union => union.Inputs.ToList(),
            ILogicalPlanParent parent => [parent.Plan],

            _ => []
        };
    }
    /// <summary>
    /// Gets the expressions for a given plan type
    /// </summary>
    /// <returns>Plan expression list</returns>
    List<ILogicalExpression> GetExpressions()
    {
        return this switch
        {
            Aggregate aggregate => aggregate.AggregateExpressions.ToList().Concat(aggregate.GroupExpressions).ToList(),
            Filter filter => [filter.Predicate],
            Projection projection => projection.Expression,
            Sort sort => sort.OrderByExpressions,
            Join join => GetJoinExpressions(join),

            _ => []
        };

        static List<ILogicalExpression> GetJoinExpressions(Join join)
        {
            var expressions = join.On
                .Select(j => (ILogicalExpression)new Binary(j.Left, BinaryOperator.Eq, j.Right))
                .ToList();

            if (join.Filter != null)
            {
                expressions.Add(join.Filter);
            }

            return expressions;
        }
    }
    /// <summary>
    /// Creates a plan using the current instance's expressions
    /// and a new set of plan inputs
    /// </summary>
    /// <param name="inputs">New plan inputs</param>
    /// <returns>New plan with updated inputs</returns>
    ILogicalPlan WithNewInputs(List<ILogicalPlan> inputs)
    {
        return FromPlan(GetExpressions(), inputs);
    }
    /// <summary>
    /// Creates a new plan from a set of expressions and plan inputs
    /// </summary>
    /// <param name="expressions">Expressions used in the new plan</param>
    /// <param name="inputs">Inputs feeding data to the new plan</param>
    /// <returns>Newly created plan</returns>
    ILogicalPlan FromPlan(List<ILogicalExpression> expressions, List<ILogicalPlan> inputs)
    {
        switch (this)
        {
            case Projection projection:
                return new Projection(inputs[0], expressions, projection.Schema);

            case Filter:
                var predicate = expressions[0];
                return new Filter(inputs[0], predicate);

            case Aggregate aggregate:
                return aggregate with { Plan = inputs[0] };

            case TableScan scan:
                return scan;  // Not using filters; no need to clone the table object.

            case Sort:
                return new Sort(inputs[0], expressions);

            case Distinct:
                return new Distinct(inputs[0]);

            case Limit limit:
                return limit with { Plan = inputs[0] };

            case SubqueryAlias subquery:
                return SubqueryAlias.TryNew(inputs[0], subquery.Alias);

            case Join join:
                return BuildJoin(join);

            case CrossJoin:
                return new CrossJoin(inputs[0], inputs[1]);

            case Union union:
                return new Union(inputs, union.Schema);

            default:
                throw new NotImplementedException("WithNewInputs not implemented for plan type");
        }


        ILogicalPlan BuildJoin(Join join)
        {
            var expressionCount = join.On.Count;
            var newOn = expressions.Take(expressionCount)
                .Select(expr =>
                {
                    var unaliased = expr.Unalias();

                    if (unaliased is Binary b)
                    {
                        return (b.Left, b.Right);
                    }

                    throw new InvalidOperationException("Expressions must be a binary expression.");
                })
                .ToList();

            ILogicalExpression? filterExpression = null;

            if (expressions.Count > expressionCount)
            {
                filterExpression = expressions[^1];
            }

            var joinSchema = inputs[0].Schema.Join(inputs[1].Schema);
            return new Join(inputs[0], inputs[1], newOn, filterExpression, join.JoinType, joinSchema);//, join.JoinConstraint);
        }
    }
}
/// <summary>
/// Logical plan that has a parent plan that executes first
/// </summary>
internal interface ILogicalPlanParent : ILogicalPlan
{
    ILogicalPlan Plan { get; }
}