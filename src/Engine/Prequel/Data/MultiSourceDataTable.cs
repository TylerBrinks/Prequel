using Prequel.Execution;

namespace Prequel.Data;

/// <summary>
/// Multiple data source wrapper for spanning data tables
/// with data shared across more than one table instance.
/// </summary>
public class MultiSourceDataTable(string tableName, IEnumerable<DataTable> tables, Schema schema) : DataTable
{
    /// <summary>
    /// Table name
    /// </summary>
    public override string Name { get; } = tableName;

    /// <summary>
    /// Schema shared by all child tables
    /// </summary>
    public override Schema? Schema { get; } = schema;

    /// <summary>
    /// Scans all child tables and returns an execution plan that
    /// will enumerate data sequentially in each unique table.
    /// </summary>
    /// <param name="projection">Plan projection with field indices to read</param>
    /// <returns>Execution plan spanning multiple files</returns>
    public override IExecutionPlan Scan(List<int> projection)
    {
        var plans = tables.Select(table => table.Scan(BuildProjection(projection))).ToList();

        return new MultiPlanExecution(plans, Schema!);
    }
}