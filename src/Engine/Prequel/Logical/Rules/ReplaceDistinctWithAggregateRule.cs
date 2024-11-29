using Prequel.Logical.Plans;

namespace Prequel.Logical.Rules;

/// <summary>
/// Optimization rule to remove DISTINCT plans and replace them with aggregations
/// </summary>
internal class ReplaceDistinctWithAggregateRule : ILogicalPlanOptimizationRule
{
    public ApplyOrder ApplyOrder => ApplyOrder.BottomUp;
    /// <summary>
    /// Optimize Distinct logical plans by replacing the plan with an 
    /// aggregate group by expression
    /// </summary>
    /// <param name="plan">Distinct plan to convert to an aggregate</param>
    /// <returns>Aggregation plan</returns>
    public ILogicalPlan TryOptimize(ILogicalPlan plan)
    {
        if (plan is not Distinct d)
        {
            return plan;
        }

        var groupExpression = plan.Schema.ExpandWildcard();

        return Aggregate.TryNew(d.Plan, groupExpression, []);
    }
}