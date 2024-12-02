using System.Runtime.CompilerServices;
using Prequel.Engine.Caching;
using Prequel.Data;
using Prequel.Metrics;
using Prequel.Engine.IO;
using Prequel.Engine.Source.File;

namespace Prequel.Engine.Source.Csv;

/// <summary>
/// Source for reading text data stored in delimited format
/// </summary>
public class CsvDataTable : PhysicalFileDataTable
{
    private readonly CsvReadOptions _readOptions;
    private readonly IFileStream _fileStream;
    private Schema? _schema;

    internal CsvDataTable(
        string tableName,
        IFileStream fileStream,
        CacheOptions cacheOptions,
        CsvReadOptions? options)
        : base(tableName, cacheOptions)
    {
        _fileStream = fileStream;
        _readOptions = options ?? new();
    }

    /// <summary>
    /// CSV file's inferred schema
    /// </summary>
    public override Schema? Schema => _schema;

    /// <summary>
    /// Creates a new instance of a CSV data source with
    /// an initialized schema
    /// </summary>
    /// <param name="tableName">Table name</param>
    /// <param name="fileStream">File source instance</param>
    /// <param name="queryContext">Options for controlling the query output</param>
    /// <param name="cacheOptions">Caching options</param>
    /// <param name="readOptions">CSV read context</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>CsvDataSource instance</returns>
    public static async Task<CsvDataTable> FromStreamAsync(
        string tableName,
        IFileStream fileStream,
        QueryContext? queryContext = null,
        CacheOptions? cacheOptions = null,
        CsvReadOptions? readOptions = null,
        CancellationToken cancellation = default!)
    {
        var dataTable = new CsvDataTable(tableName, fileStream, cacheOptions ?? new(), readOptions ?? new());
        await dataTable.InferSchemaAsync(queryContext ?? new(), cancellation);
        return dataTable;
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
    public static CsvDataTable FromSchema(
        string tableName,
        IFileStream fileStream,
        Schema schema,
        CacheOptions? cacheOptions = null,
        CsvReadOptions? readOptions = null)
    {
        var dataTable = new CsvDataTable(tableName, fileStream, cacheOptions ?? new(), readOptions ?? new())
        {
            _schema = schema,
        };
        return dataTable;
    }
    /// <summary>
    /// Reads a CSV file taking only fields from the supplied
    /// field indices and yielding rows whenever enough records
    /// have been read to meet the batch size configuration.
    /// </summary>
    /// <param name="indices">Indices of the data to read; all other data is omitted.</param>
    /// <param name="queryContext">Options for controlling the query output</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Async enumerable list of string values</returns>
    public override async IAsyncEnumerable<List<string?[]>> ReadAsync(
        List<int> indices,
        QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        var csv = new CsvDataSourceReader(_fileStream, _readOptions);

        using var step = queryContext.Profiler.Step("Data Table, CSV data table, Read source");

        await foreach (var row in EnumerateDataReader(queryContext, csv, cancellation))
        {
            step.IncrementRowCount(row.Count);
            yield return row;
        }
    }
    /// <summary>
    /// Reads a subset of records and uses the values to infer the schema
    /// of each field in the data source.
    /// </summary>
    /// <returns>Awaitable task</returns>
    public override async Task<Schema> InferSchemaAsync(QueryContext queryContext, CancellationToken cancellation = default)
    {
        if (_schema != null)
        {
            return _schema;
        }

        var csv = new CsvDataSourceReader(_fileStream, _readOptions);

        var columnTypes = await GetInferredDataTypes(csv, queryContext, _readOptions.InferMax, cancellation);

        var fields = csv.Headers.Select((h, i) => QualifiedField.Unqualified(h, columnTypes[i].DataType)).ToList();

        _schema = new Schema(fields);
        return _schema;
    }
    
    public override string ToString() => $"CSV file: {_fileStream}, {_readOptions}";
}