using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Execution;
using Prequel.Engine.Core.Metrics;
using Prequel.Engine.Core.Physical.Expressions;
using Prequel.Engine.Core.Physical.Functions;

namespace Prequel.Engine.Core.Physical.Aggregation;

/// <summary>
/// Non-grouping aggregation
/// </summary>
internal class NoGroupingAggregation(AggregationMode aggregationMode,
    Schema schema,
    List<Aggregate> aggregateExpressions,
    IExecutionPlan plan,
    QueryContext queryContext)
{
    /// <summary>
    /// Adds a data point to the accumulator
    /// </summary>
    /// <param name="cancellation">Optional cancellation token</param>
    public async Task<RecordBatch> AggregateAsync(CancellationToken cancellation = new())
    {
        var accumulators = aggregateExpressions.Cast<IAggregation>().Select(fn => fn.CreateAccumulator()).ToList();
        var expressions = MapAggregateExpressions(0);

        using (var step = queryContext.Profiler.Step("Execution Plan, No-grouping Hash, Execute plan"))
        {
            await foreach (var batch in plan.ExecuteAsync(queryContext, cancellation))
            {
                step.IncrementBatch(batch.RowCount);
                AggregateBatch(batch, accumulators, expressions);
            }
        }

        var columns = FinalizeAggregation(accumulators);

        return RecordBatch.TryNew(schema, columns);
    }

    /// <summary>
    /// Gets aggregates from evaluated expressions and returns
    /// the values.  In partial mode the values are run against
    /// each accumulator by updating values.  In final mode,
    /// accumulator values are merged.
    /// </summary>
    /// <param name="batch">Batch to evaluate</param>
    /// <param name="accumulators">Accumulators to run against each batch value</param>
    /// <param name="expressions">Expressions to evaluate and update or merge with each accumulator</param>
    /// <returns>List of column values</returns>
    public void AggregateBatch(
        RecordBatch batch,
        List<Accumulator> accumulators,
        List<List<IPhysicalExpression>> expressions)
    {
        foreach (var (accumulator, exp) in accumulators.Zip(expressions))
        {
            var values = exp.Select(c => c.Evaluate(batch).ToValueArray(batch.RowCount)).ToList();

            if (aggregationMode == AggregationMode.Partial)
            {
                accumulator.UpdateBatch(values);
            }
            else
            {
                accumulator.MergeBatch(values);
            }
        }
    }
    /// <summary>
    /// Maps or merges at a given column index aggregate expressions 
    /// </summary>
    /// <param name="columnIndex">Column index used for merging expressions</param>
    /// <returns>Lists of physical expressions</returns>
    public List<List<IPhysicalExpression>> MapAggregateExpressions(int columnIndex)
    {
        if (aggregationMode == AggregationMode.Partial)
        {
            return aggregateExpressions.Select(ae => ae.Expressions).ToList();
        }

        var index = columnIndex;
        return aggregateExpressions.Select(agg =>
        {
            var expressions = MergeExpressions(index, agg);
            index += expressions.Count;
            return expressions;
        }).ToList();
    }
    /// <summary>
    /// Merges all aggregate state fields into new physical columns
    /// with the name of the state field, preserving their index in
    /// the state field list
    /// </summary>
    /// <param name="index">Index to offset the state field columns</param>
    /// <param name="aggregate">Aggregate containing state fields to merge</param>
    /// <returns>List of physical expressions</returns>
    private static List<IPhysicalExpression> MergeExpressions(int index, Aggregate aggregate)
    {
        return aggregate.StateFields.Select((f, i) => (IPhysicalExpression)new Column(f.Name, index + i)).ToList();
    }
    /// <summary>
    /// Finalizes an aggregation by selecting the raw evaluated values.
    /// In partial mode the values are selected from the accumulator
    /// state.  In final mode, the evaluated raw values are returned.
    /// </summary>
    /// <param name="accumulators">Accumulators to finalize</param>
    /// <returns>List of aggregated values</returns>
    private List<object?> FinalizeAggregation(IEnumerable<Accumulator> accumulators)
    {
        if (aggregationMode == AggregationMode.Partial)
        {
            return accumulators
                .Select(acc => acc.State)
                .Select(val => val.Select(sv => sv.RawValue))
                .SelectMany(a => a)
                .ToList();
        }

        return accumulators
            .Select(acc => acc.Evaluate.RawValue)
            .ToList();
    }
}