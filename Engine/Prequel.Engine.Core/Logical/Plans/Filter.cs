using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Logical.Expressions;

namespace Prequel.Engine.Core.Logical.Plans;

/// <summary>
/// Filter logical plan
/// </summary>
/// <param name="Plan">Parent plan</param>
/// <param name="Predicate">Filter predicate expression</param>
internal record Filter(ILogicalPlan Plan, ILogicalExpression Predicate) : ILogicalPlanParent
{
    /// <summary>
    /// Pass through to the parent plan's schema
    /// </summary>
    public Schema Schema => Plan.Schema;

    public override string ToString()
    {
        return $"Filter: {Predicate}";
    }

    public string ToStringIndented(Indentation? indentation = null)
    {
        var indent = indentation ?? new Indentation();
        return $"Filter: {Predicate}{indent.Next(Plan)}";
    }
}