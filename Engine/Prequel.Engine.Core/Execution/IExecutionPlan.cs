using Prequel.Engine.Core.Data;

namespace Prequel.Engine.Core.Execution;

/// <summary>
/// Outlines the schema and execution operation for executing
/// a physical query plan step.
/// </summary>
public interface IExecutionPlan
{
    Schema Schema { get; }
    IAsyncEnumerable<RecordBatch> ExecuteAsync(QueryContext queryContext, CancellationToken cancellation = default!);
}