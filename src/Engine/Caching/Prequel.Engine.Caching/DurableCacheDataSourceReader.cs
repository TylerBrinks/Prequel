using Prequel.Data;
using Prequel.Metrics;
using Prequel.Engine.IO;
using System.Runtime.CompilerServices;
using System.IO;

namespace Prequel.Engine.Caching;

/// <summary>
/// Reader for retrieving cached data from durable storage
/// </summary>
/// <param name="sourceReader">Source reader instance</param>
/// <param name="cacheOptions">Caching options</param>
public class DurableCacheDataSourceReader(IDataSourceReader sourceReader, CacheOptions cacheOptions) : IDataSourceReader
{
    /// <summary>
    /// Reads data from a durable cache provider if the underlying resource exists
    /// </summary>
    /// <param name="queryContext">Query context</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns></returns>
    public async IAsyncEnumerable<object?[]> ReadSourceAsync(QueryContext queryContext, [EnumeratorCancellation] CancellationToken cancellation = default)
    {
        // Read from storage if the data exists in cache.
        if (cacheOptions.GetDataReader != null)
        {
            bool cacheDataExists;
            using (queryContext.Profiler.Step("Data Source Reader, Durable Cache, Check cached data exists"))
            {
                cacheDataExists = await cacheOptions.CachedDataExists(cancellation);
            }

            if (cacheDataExists)
            {
                var cacheReader = cacheOptions.GetDataReader(cacheOptions);

                using var cacheStep = queryContext.Profiler.Step("Data Source Reader Durable Cache, Read cached data");
                await foreach (var row in cacheReader.ReadSourceAsync(queryContext, cancellation))
                {
                    cacheStep.IncrementRowCount();
                    yield return row;
                }

                // Exit the async enumerable so the source won't be queried 
                yield break;
            }
        }

        using var step = queryContext.Profiler.Step("Data Source Reader, Durable Cache, Read from source");

        // Not cached; read data directly from the source
        await foreach (var row in sourceReader.ReadSourceAsync(queryContext, cancellation))
        {
            step.IncrementRowCount();

            yield return row;
        }

        // Prevent re-entry
        // ReSharper disable once RedundantJumpStatement
        yield break;
    }

    public override string ToString() => $"Cache data source reader: provider={cacheOptions.CacheProvider?.GetType().Name}";

}