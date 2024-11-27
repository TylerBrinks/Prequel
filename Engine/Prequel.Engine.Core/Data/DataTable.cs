using Prequel.Engine.Core.Execution;

namespace Prequel.Engine.Core.Data;

/// <summary>
/// Represents a readable data source and the underlying data schema
/// </summary>
public abstract class DataTable
{
    public abstract string Name { get; }
    /// <summary>
    /// Schema of the source's underlying data
    /// </summary>
    public abstract Schema? Schema { get; }
    /// <summary>
    /// Reads only relevant data from the query projection (columns)
    /// </summary>
    /// <param name="projection">Column data to retrieve from the source</param>
    /// <returns>IExecutionPlan instance</returns>
    public abstract IExecutionPlan Scan(List<int> projection);
    /// <summary>
    /// Builds a projection from a valid list of columns or a full
    /// list of columns if none are provided.
    /// </summary>
    /// <param name="projection"></param>
    /// <returns></returns>
    protected List<int> BuildProjection(List<int>? projection)
    {
        return projection ?? Schema!.Fields.Select((_, i) => i).ToList();
    }
}