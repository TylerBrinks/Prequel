namespace Prequel.Logical.Rules;

/// <summary>
/// Defines operations for rules that optimize logical plan steps
/// </summary>
internal interface ILogicalPlanOptimizationRule
{
    ApplyOrder ApplyOrder { get; }
    ILogicalPlan? TryOptimize(ILogicalPlan plan);
}