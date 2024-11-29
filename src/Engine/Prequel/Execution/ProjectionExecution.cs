using Prequel.Data;
using Prequel.Metrics;
using Prequel.Physical.Expressions;
using Prequel.Values;
using System.Runtime.CompilerServices;

namespace Prequel.Execution;

/// <summary>
/// Execution that builds projected values from a list of physical expressions
/// </summary>
/// <param name="Expressions">Expressions to project</param>
/// <param name="Schema">Schema containing all fields involved in the projection</param>
/// <param name="Plan">Parent execution plan</param>
internal record ProjectionExecution(
    List<(IPhysicalExpression Expression, string Name)> Expressions,
    Schema Schema,
    IExecutionPlan Plan) : IExecutionPlan
{
    /// <summary>
    /// Creates a new projection execution plan
    /// </summary>
    /// <param name="physicalExpressions">List of physical expressions to project</param>
    /// <param name="plan">Parent execution plan</param>
    /// <returns></returns>
    public static IExecutionPlan TryNew(List<(IPhysicalExpression Expression, string Name)> physicalExpressions, IExecutionPlan plan)
    {
        var fields = physicalExpressions.Select(e => QualifiedField.Unqualified(e.Name, e.Expression.GetDataType(plan.Schema))).ToList();
        var schema = new Schema(fields);

        //TODO alias loop
        //TODO output ordering respect alias

        return new ProjectionExecution(physicalExpressions, schema, plan);//todo alias_map
    }

    /// <summary>
    /// Executes a projection for all expressions against record batches
    /// produced by the parent execution plan
    /// </summary>
    /// <param name="queryContext">Execution query queryContext</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Async enumerable array of record batches</returns>
    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        using var step = queryContext.Profiler.Step("Execution Plan, Projection, execute");

        await foreach (var batch in Plan.ExecuteAsync(queryContext, cancellation))
        {
            step.IncrementBatch(batch.RowCount);

            var columns = Expressions.Select(e => e.Expression.Evaluate(batch)).ToList();

            var projection = new RecordBatch(Schema);

            for (var i = 0; i < columns.Count; i++)
            {
                var column = columns[i];

                if (column is ArrayColumnValue array)
                {
                    foreach (var val in array.Values)
                    {
                        projection.AddResult(i, val);
                    }
                }
                else if (column is ScalarColumnValue scalar)
                {
                    projection.AddResult(i, scalar.Value);
                }
            }

            yield return projection;
        }
    }
}