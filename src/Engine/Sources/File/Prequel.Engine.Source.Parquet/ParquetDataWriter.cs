using System.Runtime.InteropServices;
using ParquetSharp;
using Prequel.Data;
using Prequel.Engine.IO;
using Column = ParquetSharp.Column;
using RecordBatch = Prequel.Data.RecordBatch;

namespace Prequel.Engine.Source.Parquet;

/// <summary>
/// Parquet file data writer
/// </summary>
/// <param name="fileStream">File stream instance</param>
public class ParquetDataWriter(IFileStream fileStream) : IDataWriter
{
    private ParquetFileWriter? _writer;
    private Stream? _stream;
    private RowGroupWriter? _rowGroupWriter;

    public async ValueTask DisposeAsync()
    {
        var cancellation = new CancellationTokenSource();

        await _stream!.FlushAsync(cancellation.Token);
        // await _stream!.DisposeAsync();
        _rowGroupWriter!.Close();
        _rowGroupWriter.Dispose();
        _writer?.Dispose();
    }

    /// <summary>
    /// Writes data to a file stream in Parquet format
    /// </summary>
    /// <param name="batch">Records containing data to write</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Value Task</returns>
    public async ValueTask WriteAsync(RecordBatch batch, CancellationToken cancellation = default)
    {
        if (_writer == null)
        {
            var columns = batch.Schema.Fields.Select(field => new Column(field.DataType.GetNullablePrimitiveType(), field.Name)).ToArray();
            _stream = await fileStream.GetWriteStreamAsync(cancellation);
            _writer = new ParquetFileWriter(_stream, columns);
        }

        _rowGroupWriter = _writer.AppendRowGroup();

        for (var i = 0; i < batch.Results.Count; i++)
        {
            var column = _rowGroupWriter!.NextColumn();
            _ = column.LogicalWriter().Apply(new ParquetRecordColumnWriter(batch.Results[i], batch.Schema.Fields[i].DataType));
        }
    }
}

internal sealed class ParquetRecordColumnWriter(RecordArray results, ColumnDataType dataType) : ILogicalColumnWriterVisitor<bool>
{
    public bool OnLogicalColumnWriter<TValue>(LogicalColumnWriter<TValue> columnWriter)
    {
        var data = dataType switch
        {
            ColumnDataType.Integer => ConvertNumericArray(),

            _ => CollectionsMarshal.AsSpan(results.Values as List<TValue>),
        };

        columnWriter.WriteBatch(data);

        return true;

        ReadOnlySpan<TValue> ConvertNumericArray()
        {
            long?[] longs = [];
            if (results.Values is List<byte?> b)
            {
                longs = b.Select(x => CoerceNumberTypes(x)).ToArray();
            }

            return new ReadOnlySpan<TValue>(longs as TValue[]);
        }

        static long? CoerceNumberTypes(object? value)
        {
            return value is byte or short or int or long ? Convert.ToInt64(value) : null;
        }
    }
}