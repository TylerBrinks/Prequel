using System.Runtime.CompilerServices;
using ParquetSharp;
using Prequel.Engine.Caching;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Metrics;
using Prequel.Engine.IO;
using Prequel.Engine.Source.File;

namespace Prequel.Engine.Source.Parquet;

/// <summary>
/// Parquet file data table
/// </summary>
public class ParquetDataTable : PhysicalFileDataTable
{
    private readonly IFileStream _fileStream;

    private Schema? _schema;

    internal ParquetDataTable(
        string tableName,
        IFileStream fileStream,
        CacheOptions cacheOptions
        ) : base(tableName, cacheOptions)
    {
        _fileStream = fileStream;
    }

    /// <summary>
    /// Parquet file's inferred schema
    /// </summary>
    public override Schema? Schema => _schema;

    /// <summary>
    /// Creates a new instance of a Parquet data source with
    /// an initialized schema
    /// </summary>
    /// <param name="tableName">Table name</param>
    /// <param name="fileStream">File source instance</param>
    /// <param name="queryContext">Options for controlling the query output</param>
    /// <param name="cacheOptions">Caching options</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>ParquetDataSource instance</returns>
    public static async Task<ParquetDataTable> FromStreamAsync(
        string tableName,
        IFileStream fileStream,
        QueryContext? queryContext = null,
        CacheOptions? cacheOptions = null,
        CancellationToken cancellation = default!)
    {
        var dataSource = new ParquetDataTable(tableName, fileStream, cacheOptions ?? new());
        await dataSource.InferSchemaAsync(queryContext ?? new(), cancellation);
        return dataSource;
    }

    /// <summary>
    /// Creates a new instance of a CSV data source using an existing schema
    /// </summary>
    /// <param name="tableName">Table name</param>
    /// <param name="fileStream">File source instance</param>
    /// <param name="schema">File's underlying schema</param>
    /// <param name="cacheOptions">Caching options</param>
    /// <returns>CsvDataSource instance</returns>
    public static ParquetDataTable FromSchema(
        string tableName,
        IFileStream fileStream,
        Schema schema,
        CacheOptions? cacheOptions = null)
    {
        var dataTable = new ParquetDataTable(tableName, fileStream, cacheOptions ?? new())
        {
            _schema = schema
        };
        return dataTable;
    }

    /// <summary>
    /// Reads a subset of records and uses the values to infer the schema
    /// of each field in the data source.
    /// </summary>
    /// <param name="queryContext">Options for controlling the query output</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Awaitable task</returns>
    public override async Task InferSchemaAsync(QueryContext queryContext, CancellationToken cancellation = default!)
    {
        if (_schema != null)
        {
            return;
        }

        var reader = new ParquetFileReader(await _fileStream.GetReadStreamAsync(cancellation));

        var descriptor = reader.FileMetaData.Schema;
        var fields = new List<QualifiedField>(descriptor.NumColumns);

        for (var columnIndex = 0; columnIndex < descriptor.NumColumns; ++columnIndex)
        {
            var column = descriptor.Column(columnIndex);
            var columnType = GetColumnType(column.LogicalType.Type, column.PhysicalType);

            fields.Add(new QualifiedField(column.Name, columnType));
        }

        _schema = new Schema(fields);
        return;

        static ColumnDataType GetColumnType(LogicalTypeEnum logicalType, PhysicalType physicalType)
        {
            if (logicalType == LogicalTypeEnum.None)
            {
                return physicalType switch
                {
                    PhysicalType.Boolean => ColumnDataType.Boolean,
                    PhysicalType.Double or PhysicalType.Float => ColumnDataType.Double,
                    PhysicalType.Int32 or PhysicalType.Int64 => ColumnDataType.Integer,
                    PhysicalType.Int96 => ColumnDataType.TimestampNanosecond,

                    _ => ColumnDataType.Utf8
                };
            }

            return logicalType switch
            {
                LogicalTypeEnum.Int => ColumnDataType.Integer,

                LogicalTypeEnum.Decimal
                    or LogicalTypeEnum.Float16 => ColumnDataType.Double,

                LogicalTypeEnum.Date
                    or LogicalTypeEnum.Time
                    or LogicalTypeEnum.Timestamp
                    or LogicalTypeEnum.Interval => ColumnDataType.Date32,

                _ => ColumnDataType.Utf8
            };
        }
    }
    /// <summary>
    /// Reads a Parquet file taking only fields from the supplied
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
        var parquet = new ParquetDataSourceReader(_fileStream);

        using var step = queryContext.Profiler.Step("Data Table, Parquet data table, Read source");

        await foreach (var row in EnumerateDataReader(queryContext, parquet, cancellation))
        {
            step.IncrementRowCount(row.Count);
            //step.IncrementBatch(row.Count);
            yield return row;
        }
    }
}
