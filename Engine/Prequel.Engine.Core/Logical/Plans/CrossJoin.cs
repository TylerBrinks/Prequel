using Prequel.Engine.Core.Data;

namespace Prequel.Engine.Core.Logical.Plans;

/// <summary>
/// Cross join logical expression
/// </summary>
/// <param name="Plan">Join left side plan</param>
/// <param name="Right">Join right side plan</param>
internal record CrossJoin(ILogicalPlan Plan, ILogicalPlan Right) : ILogicalPlanParent
{
    /// <summary>
    /// Schema with fields from left and right sides
    /// </summary>
    public Schema Schema => Plan.Schema.Join(Right.Schema);

    public override string ToString()
    {
        return "Cross Join";
    }

    public string ToStringIndented(Indentation? indentation = null)
    {
        var indent = indentation ?? new Indentation();
        return $"{this}: {indent.Next(Plan)}{indent.Repeat(Right)}";
    }
}