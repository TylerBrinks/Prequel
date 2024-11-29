using Prequel.Logical.Rules;

namespace Prequel.Logical;

internal class LogicalPlanOptimizer
{
    private static readonly List<ILogicalPlanOptimizationRule> Rules =
    [
        new ReplaceDistinctWithAggregateRule(),
        new ScalarSubqueryToJoinRule(),
        new ExtractEquijoinPredicateRule(),
        new PushDownProjectionRule(),
        new EliminateProjectionRule(),
    ];

    public ILogicalPlan Optimize(ILogicalPlan logicalPlan)
    {
        var plan = logicalPlan;
        foreach (var result in Rules.Select(rule => OptimizeRecursively(rule, plan)))
        {
            if (result != null)
            {
                plan = result;
            }
        }

        return plan;
    }

    private ILogicalPlan? OptimizeRecursively(ILogicalPlanOptimizationRule rule, ILogicalPlan plan)
    {
        if (rule.ApplyOrder == ApplyOrder.TopDown)
        {
            var optimizeSelfOpt = rule.TryOptimize(plan);
            var optimizeInputsOpt = OptimizeInputs(rule, optimizeSelfOpt ?? plan);

            return optimizeInputsOpt ?? optimizeSelfOpt;
        }
        else
        {
            var optimizeInputsOpt = OptimizeInputs(rule, plan);
            var optimizeSelfOpt = rule.TryOptimize(optimizeInputsOpt ?? plan);

            return optimizeSelfOpt ?? optimizeInputsOpt;
        }
    }

    private ILogicalPlan? OptimizeInputs(ILogicalPlanOptimizationRule rule, ILogicalPlan plan)
    {
        var inputs = plan.GetInputs();
        var result = inputs.Select(p => OptimizeRecursively(rule, p)).ToList();

        if (!result.Any() || result.All(r => r == null))
        {
            return null;
        }

        var newInputs = result.Select((p, i) => p ?? inputs[i]).ToList();

        return plan.WithNewInputs(newInputs);
    }
}