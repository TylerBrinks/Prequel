using Prequel.Data;

namespace Prequel.Logical.Plans;

/// <summary>
/// Creates a subquery execution plan
/// </summary>
/// <param name="Plan">Parent plan</param>
/// <param name="Schema">Schema containing all fields for the query</param>
/// <param name="Alias">Query alias</param>
internal record SubqueryAlias(ILogicalPlan Plan, Schema Schema, string Alias) : ILogicalPlanParent
{
    /// <summary>
    /// Creates a new subquery alias logical plan from an alias table reference
    /// </summary>
    /// <param name="plan">Plan with a schema containing a qualified field with the specified alias</param>
    /// <param name="alias">Alias of the table reference</param>
    /// <returns></returns>
    public static ILogicalPlan TryNew(ILogicalPlan plan, string alias)
    {
        var tableReference = new TableReference(alias);
        var schemaFields = plan.Schema.Fields.Select(f => f.FromQualified(tableReference)).ToList();
        var aliasedSchema = new Schema(schemaFields);

        return new SubqueryAlias(plan, aliasedSchema, alias);
    }

    public string ToStringIndented(Indentation? indentation = null)
    {
        var indent = indentation ?? new Indentation();
        return $"{this}{indent.Next(Plan)}";
    }

    public override string ToString()
    {
        return $"Subquery Alias: {Alias}";
    }
}