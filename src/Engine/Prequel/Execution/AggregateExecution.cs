using System.Runtime.CompilerServices;
using Prequel.Physical.Aggregation;
using Prequel.Physical.Expressions;
using Prequel.Metrics;
using Prequel.Data;
using Prequel.Physical;

namespace Prequel.Execution;

public enum AggregationMode
{
    Partial,
    Final
}
/// <summary>
/// Execution that runs aggregation functions on all aggregated
/// columns in one or more record batches.
/// </summary>
/// <param name="Mode">Partial Aggregation Mode when running initial aggregations.  Final mode when combining aggregations into new record batches</param>
/// <param name="GroupBy">Clause denoting which in the execution will be grouped according to the aggregate function output</param>
/// <param name="AggregateExpressions">Aggregate expression</param>
/// <param name="Plan">Parent execution plan</param>
/// <param name="Schema">Schema containing field definitions</param>
/// <param name="InputSchema">Input schema containing field definitions</param>
internal record AggregateExecution(
    AggregationMode Mode,
    GroupBy GroupBy,
    List<Aggregate> AggregateExpressions,
    IExecutionPlan Plan,
    Schema Schema,
    Schema InputSchema
    ) : IExecutionPlan
{
    /// <summary>
    /// Creates a new aggregate execution
    /// </summary>
    /// <param name="mode">Partial Aggregation Mode when running initial aggregations.  Final mode when combining aggregations into new record batches</param>
    /// <param name="groupBy">Clause denoting which in the execution will be grouped according to the aggregate function output</param>
    /// <param name="aggregateExpressions">Aggregate expression</param>
    /// <param name="plan">Parent execution plan</param>
    /// <param name="inputSchema">Input schema containing field definitions</param>
    /// <returns>AggregateExecution instance</returns>
    public static AggregateExecution TryNew(
        AggregationMode mode,
        GroupBy groupBy,
        List<Aggregate> aggregateExpressions,
        IExecutionPlan plan,
        Schema inputSchema)
    {
        var schema = CreateSchema(plan.Schema, groupBy.Expression, aggregateExpressions, mode);

        return new AggregateExecution(mode, groupBy, aggregateExpressions, plan, schema, inputSchema);
    }
    /// <summary>
    /// Creates a schema from a list of aggregate expressions
    /// </summary>
    /// <param name="planSchema">Plan to use during schema creating</param>
    /// <param name="groupBy">Clause denoting which in the execution will be grouped according to the aggregate function output</param>
    /// <param name="aggregateExpressions">Aggregate expression</param>
    /// <param name="mode">Partial Aggregation Mode when running initial aggregations.  Final mode when combining aggregations into new record batches</param>
    /// <returns>new schema instance with fields used by either the partial or final aggregation</returns>
    private static Schema CreateSchema(
        Schema planSchema,
        IEnumerable<(IPhysicalExpression Expression, string Name)> groupBy,
        List<Aggregate> aggregateExpressions,
        AggregationMode mode)
    {
        var fields = groupBy
            .Select(group => QualifiedField.Unqualified(group.Name, group.Expression.GetDataType(planSchema)))
            .ToList();

        if (mode == AggregationMode.Partial)
        {
            foreach (var expr in aggregateExpressions)
            {
                fields.AddRange(expr.StateFields);
            }
        }
        else
        {
            fields.AddRange(aggregateExpressions.Select(expr => expr.NamedQualifiedField));
        }

        return new Schema(fields);
    }
    /// <summary>
    /// Converts the GroupBy expressions into a list of column physical expressions that the aggregate will output
    /// </summary>
    /// <returns>List of group by output physical expressions</returns>
    public List<IPhysicalExpression> OutputGroupExpression()
    {
        return GroupBy.Expression.Select((e, i) => (IPhysicalExpression)new Column(e.Name, i)).ToList();
    }
    /// <summary>
    /// Executes the aggregate functions against a plan and schema.  A Non-Grouping algorithm is used
    /// for aggregations without "Group By" expressions.  This strategy must read all values in the
    /// data set repeatedly to apply the aggregated values.  Conversely, a grouped hash algorithm
    /// is used when one or more "Group By" expressions exists.  This approach is almost always more
    /// efficient when groups are used since each hash can be tracked by value and the aggregation
    /// results can be applied to each hash value key thereby minimizing the amount of data scanned.
    /// </summary>
    /// <param name="queryContext">Execution query queryContext</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Async enumerable array of record batches</returns>
    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        RecordBatch aggregateBatch;

        if (!GroupBy.Expression.Any())
        {
            aggregateBatch = await new NoGroupingAggregation(Mode, Schema, AggregateExpressions, Plan, queryContext)
                .AggregateAsync(cancellation);
        }
        else
        {
            aggregateBatch = await new GroupedHashAggregation(Mode, Schema, AggregateExpressions, Plan, GroupBy, queryContext)
                .AggregateAsync(cancellation);
        }

        if (Mode == AggregationMode.Partial)
        {
            // Partial mode creates a single batch with aggregated values
            // therefore the single batch is returned 
            yield return aggregateBatch;
        }
        else
        {
            using var repartitionStep = queryContext.Profiler.Step("Execution Plan, Aggregate Execution, Repartition");

            // Final mode receives a single batch which may be larger than
            // the configured batch size and is therefore repartitioned
            foreach (var batch in aggregateBatch.Repartition(queryContext.BatchSize))
            {
                repartitionStep.IncrementBatch(batch.RowCount);
                yield return batch;
            }
        }
    }
}