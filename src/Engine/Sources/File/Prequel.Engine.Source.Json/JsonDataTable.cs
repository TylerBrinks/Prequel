using System.Runtime.CompilerServices;
using Prequel.Engine.Caching;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Metrics;
using Prequel.Engine.IO;
using Prequel.Engine.Source.File;

namespace Prequel.Engine.Source.Json;

/// <summary>
/// Source for reading text data stored in JSON format
/// </summary>
public class JsonDataTable : PhysicalFileDataTable
{
    private readonly IFileStream _fileStream;
    private readonly JsonOptions _readOptions;
    private Schema? _schema;

    internal JsonDataTable(
        string tableName,
        IFileStream fileStream,
        CacheOptions cacheOptions,
        JsonOptions readOptions)
        : base(tableName, cacheOptions)
    {
        _fileStream = fileStream;
        _readOptions = readOptions;
    }

    /// <summary>
    /// JSON file's inferred schema
    /// </summary>
    public override Schema? Schema => _schema;

    /// <summary>
    /// Creates a new instance of a JSON data source with
    /// an initialized schema
    /// </summary>
    /// <param name="tableName">Table name</param>
    /// <param name="fileStream">File source instance</param>
    /// <param name="queryContext">Options for controlling the query output</param>
    /// <param name="cacheOptions">CachingOptions</param>
    /// <param name="readOptions">JSON read context</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>JsonDataSource instance</returns>
    public static async Task<JsonDataTable> FromStreamAsync(
        string tableName,
        IFileStream fileStream,
        QueryContext? queryContext = null,
        CacheOptions? cacheOptions = null,
        JsonOptions? readOptions = null,
        CancellationToken cancellation = default!)
    {
        var table = new JsonDataTable(tableName,
            fileStream,
            cacheOptions ?? new(),
            readOptions ?? new());
        await table.InferSchemaAsync(queryContext ?? new(), cancellation);
        return table;
    }

    /// <summary>
    /// Creates a new instance of a CSV data source using an existing schema
    /// </summary>
    /// <param name="tableName">Table name</param>
    /// <param name="fileStream">File source instance</param>
    /// <param name="schema">File's underlying schema</param>
    /// <param name="cacheOptions">Caching options</param>
    /// <param name="readOptions">CSV read context</param>
    /// <returns>CsvDataSource instance</returns>
    public static JsonDataTable FromSchema(
        string tableName,
        IFileStream fileStream,
        Schema schema,
        CacheOptions? cacheOptions = null,
        JsonOptions? readOptions = null)
    {
        var dataTable = new JsonDataTable(tableName, fileStream, cacheOptions ?? new(), readOptions ?? new())
        {
            _schema = schema
        };
        return dataTable;
    }
    /// <summary>
    /// Reads a DN JSON file taking only fields from the supplied
    /// field indices and yielding rows whenever enough records
    /// have been read to meet the batch size configuration.
    /// </summary>
    /// <param name="indices">Indices of the data to read; all other data is omitted.</param>
    /// <param name="queryContext">Options for controlling the query output</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Async enumerable list of string values</returns>
    public override async IAsyncEnumerable<List<string?[]>> ReadAsync(List<int> indices, QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        var json = new JsonDataSourceReader(_fileStream);

        using var step = queryContext.Profiler.Step("Data Table, Json data table, Read source");

        await foreach (var row in EnumerateDataReader(queryContext, json, cancellation))
        {
            step.IncrementRowCount(row.Count);
            yield return row;
        }
    }

    /// <summary>
    /// Reads a subset of records and uses the values to infer the schema
    /// of each field in the data source.
    /// </summary>
    /// <param name="queryContext">Options for controlling the query output</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Awaitable task</returns>
    public override async Task<Schema> InferSchemaAsync(QueryContext queryContext, CancellationToken cancellation = default!)
    {
        if (_schema != null)
        {
            return _schema;
        }

        var csv = new JsonDataSourceReader(_fileStream/*, _readOptions*/);

        var columnTypes = await GetInferredDataTypes(csv, queryContext, _readOptions.InferMax, cancellation);

        var fields = csv.Headers.Select((h, i) => QualifiedField.Unqualified(h, columnTypes[i].DataType)).ToList();

        _schema = new Schema(fields);
        return _schema;
    }
}