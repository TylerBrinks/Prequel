using Prequel.Data;
using Prequel.Logical.Expressions;
using Prequel.Logical.Plans;

namespace Prequel.Logical.Rules;

/// <summary>
/// Optimization rule to eliminate redundant or unnecessary projection plan steps
/// </summary>
internal class EliminateProjectionRule : ILogicalPlanOptimizationRule
{
    public ApplyOrder ApplyOrder => ApplyOrder.TopDown;

    /// <summary>
    /// Optimize the logical plan hierarchy by removing redundant or unnecessary
    /// projections. This is used in tandem with the rule that pushes plans
    /// down the hierarchy.
    /// </summary>
    /// <param name="plan">Plan to optimize</param>
    /// <returns>Plan after optimization</returns>
    public ILogicalPlan? TryOptimize(ILogicalPlan plan)
    {
        switch (plan)
        {
            case Projection projection:
                var childPlan = projection.Plan;
                switch (childPlan)
                {
                    case CrossJoin:
                    case Filter:
                    case Join:
                    case Sort:
                    case SubqueryAlias:
                    case TableScan:
                    case Union:
                        return CanEliminate(projection, childPlan.Schema) ? childPlan : plan;

                    default:
                        return plan.Schema.Equals(childPlan.Schema) ? childPlan : null;
                }

            default:
                return null;
        }
    }
    /// <summary>
    /// Checks if a projection can be removed by comparing the list
    /// of projection fields against the schema fields.  Plans
    /// with identical schema/field configurations can be removed.
    /// </summary>
    /// <param name="projection">Projection with expressions to check</param>
    /// <param name="schema">Schema containing fields</param>
    /// <returns>True if the projection can be removed; otherwise false.</returns>
    private static bool CanEliminate(Projection projection, Schema schema)
    {
        if (projection.Expression.Count != schema.Fields.Count)
        {
            return false;
        }

        for (var i = 0; i < projection.Expression.Count; i++)
        {
            var expr = projection.Expression[i];
            if (expr is Column c)
            {
                var d = schema.Fields[i];
                if (c.Name != d.Name)
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
}