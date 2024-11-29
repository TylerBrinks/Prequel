using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Metrics;

namespace Prequel.Model.Execution;

/// <summary>
/// Query execution result
/// </summary>
/// <param name="Index">Query index</param>
/// <param name="Query">SQL query</param>
public record QueryExecutionResult(int Index, string? Query)
{
    public List<RecordBatch> Batches { get; } = [];

    public Timing? Timing { get; internal set; }
}