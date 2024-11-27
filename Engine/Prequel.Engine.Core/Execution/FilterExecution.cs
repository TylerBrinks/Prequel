using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Metrics;
using Prequel.Engine.Core.Physical.Expressions;
using Prequel.Engine.Core.Values;
using System.Runtime.CompilerServices;

namespace Prequel.Engine.Core.Execution;

/// <summary>
/// Execution that filters out data based on a filter expression.  Data is removed from the incoming
/// record batches and sent to children executions for further processing.
/// </summary>
/// <param name="Predicate">Clause to determine if the data should be filtered or preserved.</param>
/// <param name="Plan">Parent execution plan</param>
internal record FilterExecution(IPhysicalExpression Predicate, IExecutionPlan Plan) : IExecutionPlan
{
    /// <summary>
    /// Pass through of the schema for the parent execution plan
    /// </summary>
    public Schema Schema => Plan.Schema;

    /// <summary>
    /// Creates a new filter execution by validating the predicate expression creates a boolean output
    /// </summary>
    /// <param name="predicate">Predicate expression to validate execution against</param>
    /// <param name="plan">Plan to build into a filter execution</param>
    /// <returns>New filter execution</returns>
    /// <exception cref="InvalidOperationException">Thrown if the predicate does not produce a boolean result.</exception>
    public static FilterExecution TryNew(IPhysicalExpression predicate, IExecutionPlan plan)
    {
        var dt = predicate.GetDataType(plan.Schema);

        if (dt != ColumnDataType.Boolean)
        {
            throw new InvalidOperationException("invalid filter expression");
        }

        return new FilterExecution(predicate, plan);
    }

    /// <summary>
    /// Executes the filter plan against the parent plan's record batch array
    /// </summary>
    /// <param name="queryContext">Execution query queryContext</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Async enumerable array of record batches</returns>
    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        using var filterStep = queryContext.Profiler.Step("Execution Plan, Filter, Execute plan");

        await foreach (var batch in Plan.ExecuteAsync(queryContext, cancellation))
        {
            filterStep.IncrementBatch(batch.RowCount);

            var filterFlags = (BooleanColumnValue)Predicate.Evaluate(batch);

            batch.Filter(filterFlags.Values.Cast<bool>().ToArray());

            if (batch.RowCount > 0)
            {
                yield return batch;
            }
        }
    }
}