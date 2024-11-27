using Prequel.Engine.Core.Data;

namespace Prequel.Engine.Core.Logical.Plans;

/// <summary>
/// Placeholder plan for empty relations
/// </summary>
internal record EmptyRelation : ILogicalPlan
{
    public Schema Schema => new([]);

    public override string ToString()
    {
        return "Empty Relation";
    }

    public string ToStringIndented(Indentation? indentation = null)
    {
        return ToString();
    }
}