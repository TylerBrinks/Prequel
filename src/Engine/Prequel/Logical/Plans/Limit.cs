using Prequel.Data;

namespace Prequel.Logical.Plans;

/// <summary>
/// Limit execution plan
/// </summary>
/// <param name="Plan">Execution plan</param>
/// <param name="Skip">Number of records to skip</param>
/// <param name="Fetch">Number of records to fetch</param>
public record Limit(ILogicalPlan Plan, int? Skip = 0, int? Fetch = 0) : ILogicalPlanParent
{
    /// <summary>
    /// Pass through to the plan's schema
    /// </summary>
    public Schema Schema => Plan.Schema;

    public override string ToString()
    {
        return $"Limit: Skip {Skip}, Limit {Fetch}";
    }

    public string ToStringIndented(Indentation? indentation = null)
    {
        var indent = indentation ?? new Indentation();
        return $"Limit: Skip {Skip}, Limit {Fetch}{indent.Next(Plan)}";
    }
}