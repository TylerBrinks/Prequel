using Prequel.Data;

namespace Prequel.Logical.Plans;

/// <summary>
/// Distinct logical plan
/// </summary>
/// <param name="Plan">Parent plan</param>
internal record Distinct(ILogicalPlan Plan) : ILogicalPlanParent
{
    public Schema Schema => Plan.Schema;

    public string ToStringIndented(Indentation? indentation = null)
    {
        var indent = indentation ?? new Indentation();
        return $"Distinct: {indent.Next(Plan)}";
    }
}