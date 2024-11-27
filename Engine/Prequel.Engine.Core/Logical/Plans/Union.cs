using Prequel.Engine.Core.Data;

namespace Prequel.Engine.Core.Logical.Plans;

/// <summary>
/// Union logical plan
/// </summary>
/// <param name="Inputs">Plans that combine to form a union</param>
/// <param name="Schema">Schema for all plans in the union</param>
internal record Union(List<ILogicalPlan> Inputs, Schema Schema) : ILogicalPlan
{
    public override string ToString()
    {
        return "UNION";
    }

    public string ToStringIndented(Indentation? indentation = null)
    {
        var indent = indentation ?? new Indentation();

        var children = string.Join("", Inputs.Select((input, index) => index == 0 ? indent.Next(input) : indent.Repeat(input)));

        return $"{this} {children}";
    }
}