using Prequel.Data;
using Prequel.Metrics;
using Prequel.Engine.IO;
using System.Runtime.CompilerServices;

namespace Prequel.Engine.Caching;

/// <summary>
/// Reader for retrieving cached data from durable storage
/// </summary>
/// <param name="sourceReader">Source reader instance</param>
public class MemoryCachedDataSourceReader(IDataSourceReader sourceReader) : IDataSourceReader
{
    private bool _sourceReaderQueried;
    //TODO add a memory cache max?  Clear and bail if the max is reached?
    private readonly List<object?[]> _cache = [];

    /// <summary>
    /// Execute the source database reader during the first execution.  Subsequent
    /// executions read from the cache if cache is enabled; otherwise the source
    /// is read again.
    /// </summary>
    /// <param name="queryContext">Query context instance</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Async enumerable list of object arrays read from the database source</returns>
    public async IAsyncEnumerable<object?[]> ReadSourceAsync(QueryContext queryContext, [EnumeratorCancellation] CancellationToken cancellation = default)
    {
        // Read all data from the source if the memory cache is empty.  All data must be read
        // before yielding batches since downstream filters and aggregations depend on a 
        // complete data set

        if (!_sourceReaderQueried)
        {
            _sourceReaderQueried = true;
            using var cacheStep = queryContext.Profiler.Step("Data Source Reader, Memory Cache, Empty, read source");
            // Execute the query against the source data store.  Downstream batching
            // limits would force the query to terminate before all records, so
            // all records are read here in order to cache the output for additional
            // queries executed against the same connection
            await foreach (var row in sourceReader.ReadSourceAsync(queryContext, cancellation))
            {
                cacheStep.IncrementRowCount();
                _cache.Add(row);
            }
        }

        var queryStep = queryContext.Profiler.Step("Data Source Reader, Memory cache, Enumerate cached data");
        await foreach (var row in _cache.ToIAsyncEnumerable().WithCancellation(cancellation))
        {
            queryStep.IncrementRowCount();
            yield return row;
        }

        // Prevent re-entry
        // ReSharper disable once RedundantJumpStatement
        yield break;
    }
}
