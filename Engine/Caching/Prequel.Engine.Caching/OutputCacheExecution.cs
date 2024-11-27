using System.Runtime.CompilerServices;
using Prequel.Engine.Core;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Execution;
using Prequel.Engine.Core.Metrics;
using Prequel.Engine.IO;

namespace Prequel.Engine.Caching;

/// <summary>
/// Execution plan that handles writing source data to a durable cache
/// </summary>
public class OutputCacheExecution : IExecutionPlan
{
    private readonly Schema _schema;
    private readonly CacheOptions _cacheOptions;
    private readonly IExecutionPlan _sourcePlan;
    private readonly IDataWriter? _writer;

    public OutputCacheExecution(
        Schema schema,
        CacheOptions cacheOptions,
        IExecutionPlan fallbackPlan
        )
    {
        _schema = schema;
        _cacheOptions = cacheOptions;
        _sourcePlan = fallbackPlan;

        if (cacheOptions is { ShouldCacheOutput: true, GetDataWriter: not null })
        {
            _writer = cacheOptions.GetDataWriter(_cacheOptions);
        }
    }

    public Schema Schema => _schema;

    /// <summary>
    /// Executes a query from a cached file when one exists and caching is configured. 
    /// Otherwise, executes the query against the source data execution plan
    /// </summary>
    /// <param name="queryContext">Query context</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>IAsyncEnumerable record batch list </returns>
    public virtual async IAsyncEnumerable<RecordBatch> ExecuteAsync(
         QueryContext queryContext,
         [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        RecordBatch? masterBatch = null;

        // Check for existing data so cached data will not be overwritten
        bool cachedDataExists;

        using (queryContext.Profiler.Step("Execution Plan, Output Cache execution, Check cache exists"))
        {
            cachedDataExists = await _cacheOptions.CachedDataExists(cancellation);
        }

        using (var execStep = queryContext.Profiler.Step("Execution Plan, Output Cache execution, Execute source plan"))
        {
            // Not using cache on this iteration; persist the raw data.
            await foreach (var batch in _sourcePlan.ExecuteAsync(queryContext, cancellation))
            {
                execStep.IncrementBatch(batch.RowCount);

                masterBatch ??= new RecordBatch(batch.Schema);

                masterBatch.Concat(batch);

                // Cache if no cached data exists
                if (cachedDataExists) { continue; }

                using var persistStep = queryContext.Profiler.Step("Execution Plan, Output Cache execution, Persist batch");
                await PersistBatchAsync(batch);
            }

            _cacheOptions.SetCacheCreated();
        }

        await DisposeWriterAsync();

        if (masterBatch == null)
        {
            yield break;
        }

        using var repartitionStep = queryContext.Profiler.Step("Execution Plan, Output Cache execution, Repartition");
        // All data exists in the master batch.  Repartition and yield data as a typical execution plan step
        await foreach (var batch in masterBatch.Repartition(queryContext.BatchSize).ToIAsyncEnumerable().WithCancellation(cancellation))
        {
            repartitionStep.IncrementBatch(batch.RowCount);
            yield return batch;
        }
    }
    /// <summary>
    /// Disposes any writer used in caching operations
    /// </summary>
    /// <returns></returns>
    private async Task DisposeWriterAsync()
    {
        if (_writer != null)
        {
            await _writer.DisposeAsync();
        }
    }
    /// <summary>
    /// Sends a batch to the data writer when caching is configured
    /// </summary>
    /// <param name="batch">Record batch</param>
    /// <returns>Awaitable task</returns>
    private async Task PersistBatchAsync(RecordBatch batch)
    {
        if (_writer != null && _cacheOptions.ShouldCacheOutput)
        {
            await _writer.WriteAsync(batch);
        }
    }
}