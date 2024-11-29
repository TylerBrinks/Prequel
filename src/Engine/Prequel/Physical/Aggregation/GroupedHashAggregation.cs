using Prequel.Data;
using Prequel.Execution;
using Prequel.Metrics;
using Prequel.Physical.Expressions;
using Prequel.Physical.Functions;
using Prequel.Values;

namespace Prequel.Physical.Aggregation;

/// <summary>
/// Grouped hash aggregation
/// </summary>
internal class GroupedHashAggregation(AggregationMode aggregationMode,
    Schema schema,
    List<Aggregate> aggregates,
    IExecutionPlan plan,
    GroupBy groupBy,
    QueryContext queryContext)
{
    /// <summary>
    /// Adds a data point to the accumulator
    /// </summary>
    /// <param name="cancellation">Optional cancellation token</param>
    public async Task<RecordBatch> AggregateAsync(CancellationToken cancellation = new())
    {
        if (aggregationMode == AggregationMode.Partial)
        {
            return await AccumulateAsync(cancellation);
        }

        // Final aggregation operates on a single batch
        var final = plan.ExecuteAsync(queryContext, cancellation)
            .ToBlockingEnumerable(cancellationToken: cancellation)
            .First();

        return final;
    }
    /// <summary>
    /// Aggregates data from the output of a plan execution and creates
    /// hashed keys from the aggregates group expression evaluations.
    /// Aggregate batches are updated per key with the values from
    /// the batch data.
    /// </summary>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    private async Task<RecordBatch> AccumulateAsync(CancellationToken cancellationToken)
    {
        var map = new Dictionary<Sequence<object>, List<Accumulator>>();

        using (var step = queryContext.Profiler.Step("Execution Plan, Grouped Hash aggregation, Execute plan"))
        {
            await foreach (var batch in plan.ExecuteAsync(queryContext, cancellationToken))
            {
                step.IncrementBatch(batch.RowCount);

                var groupKey = groupBy.Expression.Select(e => e.Expression.Evaluate(batch)).ToList();

                var aggregateInputValues = GetAggregateInputs(batch);

                for (var rowIndex = 0; rowIndex < batch.RowCount; rowIndex++)
                {
                    var keyList = groupKey.Select(key => key.GetValue(rowIndex)).ToList();

                    var rowKey = new Sequence<object>(keyList!);

                    map.TryGetValue(rowKey, out var accumulators);

                    if (accumulators == null || accumulators.Count == 0)
                    {
                        accumulators = aggregates.Cast<IAggregation>().Select(fn => fn.CreateAccumulator()).ToList();
                        // Select distinct creates a grouping without an aggregation
                        // so the addition of the accumulator needs to handle possible
                        // duplicate row key values.
                        map.TryAdd(rowKey, accumulators);
                    }

                    for (var i = 0; i < accumulators.Count; i++)
                    {
                        var value = aggregateInputValues[i].GetValue(rowIndex);
                        accumulators[i].Accumulate(value!);
                    }
                }
            }
        }


        // Result batch containing the final aggregate values
        var aggregatedBatch = new RecordBatch(schema);

        for (var i = 0; i < map.Count; i++)
        {
            var groupKey = map.Keys.Skip(i).First();
            var accumulators = map[groupKey];

            var groupCount = groupBy.Expression.Count;

            for (var j = 0; j < groupCount; j++)
            {
                aggregatedBatch.AddResult(j, groupKey[j]);
            }

            for (var j = 0; j < aggregates.Count; j++)
            {
                aggregatedBatch.AddResult(groupCount + j, accumulators[j].Value);
            }
        }

        return aggregatedBatch;
    }
    /// <summary>
    /// Gets aggregates from evaluated expressions and returns
    /// the values.  In partial mode the values are evaluated 
    /// directly; final mode evaluates each batch by expression index
    /// </summary>
    /// <param name="batch">Batch to evaluate</param>
    /// <returns>List of column values</returns>
    private List<ColumnValue> GetAggregateInputs(RecordBatch batch)
    {
        if (aggregationMode == AggregationMode.Partial)
        {
            return aggregates.Select(ae => ae.Expression.Evaluate(batch)).ToList();
        }

        var index = groupBy.Expression.Count;

        return aggregates.Select(ae => ae.Expression.Evaluate(batch, index++)).ToList();
    }
}