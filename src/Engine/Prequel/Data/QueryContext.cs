using System.ComponentModel.DataAnnotations.Schema;
using Prequel.Execution;
using Prequel.Logical;
using Prequel.Metrics;

namespace Prequel.Data;

/// <summary>
/// Query context used to run against backing data stores, caching
/// layers, and in-memory operations
/// </summary>
public record QueryContext
{
    private readonly Lazy<QueryProfiler> _profiler = new(() => QueryProfiler.Start("Query execution"));
    public int BatchSize { get; init; } = 2048;
    public int MaxResults { get; init; } = 0; // TODO int.max or nullable?
    [NotMapped]
    public QueryProfiler Profiler => _profiler.Value;

    [NotMapped]
    public Func<ILogicalPlan, ILogicalPlan>? ModifyLogicalPlan { get; set; }
    [NotMapped]
    public Func<IExecutionPlan, IExecutionPlan>? ModifyExecutionPlan { get; set; }
}