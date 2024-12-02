using Prequel.Data;

namespace Prequel.Logical.Plans;

internal class Explain(ILogicalPlan plan) : ILogicalPlan
{
    public ILogicalPlan Plan => plan;
    public Schema Schema { get; } = new([
        new QualifiedField("plan_type", ColumnDataType.Utf8) ,
        new QualifiedField("plan", ColumnDataType.Utf8)
    ]);

    public string ToStringIndented(Indentation? indentation = null)
    {
        return plan.ToStringIndented(indentation ?? new Indentation());
    }
}
