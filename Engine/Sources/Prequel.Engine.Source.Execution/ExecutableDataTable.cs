using Prequel.Engine.Caching;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Execution;
using Prequel.Engine.IO;

namespace Prequel.Engine.Source.Execution;

/// <summary>
/// Data table supporting plan data reads and plan
/// execution via table scanning
/// </summary>
public class ExecutableDataTable : DataTable
{
    private readonly IDataSourceReader _reader;
    private readonly CacheOptions _cacheOptions;

    public ExecutableDataTable(
        string name,
        Schema schema,
        IDataSourceReader reader,
        CacheOptions cacheOptions)
    {
        Name = name;
        Schema = schema;
        _cacheOptions = cacheOptions;

        // Wrap reader in durable cache reader
        _reader = cacheOptions.ShouldCacheOutput ? new DurableCacheDataSourceReader(reader, cacheOptions) : reader;
        // Wrap the previous reader in an in-memory reader
        _reader = cacheOptions.UseMemoryCache ? new MemoryCachedDataSourceReader(_reader) : _reader;
    }

    public override string Name { get; }

    public override Schema? Schema { get; } // todo is nullable needed?

    /// <summary>
    /// Creates a new DatabaseExecution instance that will scan the
    /// data by executing a query against the database
    /// </summary>
    /// <param name="projection">Projection used to filter the
    /// data returned form the query</param>
    /// <returns>Database query execution plan</returns>
    public override IExecutionPlan Scan(List<int> projection)
    {
        IExecutionPlan execution = new DataSourceReaderExecution(Schema!, projection, _reader);

        // Caching is enabled.  Use a caching plan with physical fallback.
        if (_cacheOptions.ShouldCacheOutput)
        {
            execution = new OutputCacheExecution(Schema!, _cacheOptions, execution);
        }

        return execution;
    }
}