using Prequel.Engine.Core.Data;

namespace Prequel.Engine.Core.Logical;

internal record PlannerContext
{
    public PlannerContext()
    {
    }

    public PlannerContext(Dictionary<string, DataTable> dataSources)
    {
        DataSources = dataSources;
    }

    public PlannerContext(Dictionary<string, DataTable> dataSources, List<TableReference> tableReferences)
    {
        DataSources = dataSources;
        TableReferences = tableReferences;
    }

    /// <summary>
    ///Named data sources used in logical execution
    /// </summary>
    internal Dictionary<string, DataTable> DataSources { get; } = [];
    /// <summary>
    /// References to tables used in the logical execution of all execution plans
    /// </summary>
    internal List<TableReference> TableReferences { get; } = [];
    /// <summary>
    /// Outer schema reference used by nested queries
    /// </summary>
    internal Schema? OuterQuerySchema { get; private set; }
    /// <summary>
    /// Sets the outer schema reference and returns the
    /// previous schema reference
    /// </summary>
    /// <param name="outerSchema">New outer schema reference</param>
    /// <returns>Previous schema reference</returns>
    internal Schema? SetOuterQuerySchema(Schema? outerSchema)
    {
        var oldSchema = OuterQuerySchema;
        OuterQuerySchema = outerSchema;
        return oldSchema;
    }
}