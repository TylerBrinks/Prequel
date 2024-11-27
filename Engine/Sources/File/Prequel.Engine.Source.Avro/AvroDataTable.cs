using System.Runtime.CompilerServices;
using Prequel.Engine.Caching;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Metrics;
using Prequel.Engine.IO;
using Prequel.Engine.Source.File;

namespace Prequel.Engine.Source.Avro;

/// <summary>
/// Source for reading text data stored in delimited format
/// </summary>
public class AvroDataTable : PhysicalFileDataTable
{
    private readonly IFileStream _fileStream;
    private readonly AvroReadOptions _readOptions;

    private Schema? _schema;

    internal AvroDataTable(
        string tableName,
        IFileStream fileStream,
        CacheOptions cacheOptions,
        AvroReadOptions? readOptions = null
        )
        : base(tableName, cacheOptions)
    {
        _fileStream = fileStream;
        _readOptions = readOptions ?? new AvroReadOptions();
    }

    /// <summary>
    /// Avro file's inferred schema
    /// </summary>
    public override Schema? Schema => _schema;

    /// <summary>
    /// Creates a new instance of an Avro data source with
    /// an initialized schema
    /// </summary>
    /// <param name="tableName">Table name</param>
    /// <param name="fileStream">File source instance</param>
    /// <param name="queryContext">Options for controlling the query output</param>
    /// <param name="cacheOptions">Caching options</param>
    /// <param name="readOptions">Avro read context</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>AvroDataSource instance</returns>
    public static async Task<AvroDataTable> FromStreamAsync(
        string tableName,
        IFileStream fileStream,
        QueryContext? queryContext = null,
        CacheOptions? cacheOptions = null,
        AvroReadOptions? readOptions = null,
        CancellationToken cancellation = default!)
    {
        var dataSource = new AvroDataTable(tableName, fileStream, cacheOptions ?? new(), readOptions);
        await dataSource.InferSchemaAsync(queryContext ?? new(), cancellation);
        return dataSource;
    }

    /// <summary>
    /// Creates a new instance of an Avro data source using an existing schema
    /// </summary>
    /// <param name="tableName">Table name</param>
    /// <param name="fileStream">File source instance</param>
    /// <param name="schema">File's underlying schema</param>
    /// <param name="cacheOptions">Caching options</param>
    /// <param name="readOptions">Avro read context</param>
    /// <returns>AvroDataTable instance</returns>
    public static AvroDataTable FromSchema(
        string tableName,
        IFileStream fileStream,
        Schema schema,
        CacheOptions? cacheOptions = null,
        AvroReadOptions? readOptions = null)
    {
        var dataTable = new AvroDataTable(tableName, fileStream, cacheOptions ?? new(), readOptions)
        {
            _schema = schema
        };
        return dataTable;
    }
    /// <summary>
    /// Reads an Avro file taking only fields from the supplied
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
        var avro = new AvroDataSourceReader(_fileStream);

        using var step = queryContext.Profiler.Step("Data Table, Avro data table, Read source");

        await foreach (var row in EnumerateDataReader(queryContext, avro, cancellation))
        {
            step.IncrementRowCount(row.Count);
            yield return row;
        }
    }
    /// <summary>
    /// Reads a subset of records and uses the values to infer the schema
    /// of each field in the data source.
    /// </summary>
    /// <param name="queryContext">Query context</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Awaitable task</returns>
    public override async Task<Schema> InferSchemaAsync(QueryContext queryContext, CancellationToken cancellation = default!)
    {
        if (_schema != null)
        {
            return _schema;
        }

        var reader = new AvroDataSourceReader(_fileStream);

        var columnTypes = await GetInferredDataTypes(reader, queryContext, _readOptions.InferMax, cancellation);

        var fields = reader.Headers.Select((h, i) => QualifiedField.Unqualified(h, columnTypes[i].DataType)).ToList();

        _schema = new Schema(fields);
        return _schema;
    }
}
