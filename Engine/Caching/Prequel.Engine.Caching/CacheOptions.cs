using System.ComponentModel.DataAnnotations.Schema;
using Prequel.Engine.IO;

namespace Prequel.Engine.Caching;

/// <summary>
/// Options for query output caching
/// </summary>
public record CacheOptions
{
    /// <summary>
    /// True to cache output in memory; otherwise false
    /// </summary>
    public bool UseMemoryCache { get; init; }
    /// <summary>
    /// True to persist to physical storage; otherwise false
    /// </summary>
    public bool UseDurableCache { get; init; }
    /// <summary>
    /// Cache key for the durable storage blob
    /// </summary>
    public string? DurableCacheKey { get; init; }
    /// <summary>
    /// Duration in minutes for preserving the cached file
    /// </summary>
    public int DurableCacheDuration { get; set; }
    /// <summary>
    /// Date/time the durable cache object was created
    /// </summary>
    public DateTimeOffset? DurableCacheCreated { get; set; }
    /// <summary>
    /// Date and time the cache object becomes invalid
    /// </summary>
    public DateTimeOffset? DurableCacheExpiration { get; set; }
    /// <summary>
    /// True if the cached object is invalid; otherwise false
    /// </summary>
    public bool DurableCacheExpired { get; set; }
    /// <summary>
    /// Cache provider instance
    /// </summary>
    [NotMapped]
    public ICacheProvider? CacheProvider { get; set; }
    /// <summary>
    /// Gets a data writer to write cached data to storage
    /// </summary>
    [NotMapped]
    public Func<CacheOptions, IDataWriter>? GetDataWriter { get; set; }
    /// <summary>
    /// Gets a data reader to read previously cached data
    /// </summary>
    [NotMapped]
    public Func<CacheOptions, IDataSourceReader>? GetDataReader { get; set; }

    [NotMapped]
    public TimeProvider? Time { get; init; }

    /// <summary>
    /// Calls the underlying cache provider to check if a cached resource exists
    /// </summary>
    /// <param name="cancellation"></param>
    /// <returns>True if cached data exists; otherwise false</returns>
    public async ValueTask<bool> CachedDataExists(CancellationToken cancellation = default!)
    {
        if (CacheProvider == null || DurableCacheKey == null || DurableCacheExpired)
        {
            return false;
        }

        return await CacheProvider.ExistsAsync(DurableCacheKey, cancellation);
    }

    //TODO expand this to include classes not null
    public bool ShouldCacheOutput => UseDurableCache && !string.IsNullOrEmpty(DurableCacheKey);

    public void SetCacheCreated()
    {
        var now = (Time != null ? Time?.GetUtcNow() : DateTimeOffset.UtcNow)!.Value;
        DurableCacheCreated = now;
        DurableCacheExpiration = now.AddMinutes(DurableCacheDuration);
    }
}