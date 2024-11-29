using System.Globalization;
using nietras.SeparatedValues;
using Prequel.Data;
using Prequel.Engine.IO;

namespace Prequel.Engine.Source.Csv;

/// <summary>
/// CSV file data writer
/// </summary>
/// <param name="fileStream">File stream instance</param>
/// <param name="readOptions">CSV read options</param>
public class CsvDataWriter(IFileStream fileStream, CsvReadOptions? readOptions = null) : IDataWriter
{
    private readonly CsvReadOptions _readOptions = readOptions ?? new CsvReadOptions();
    private bool _open;
    private SepWriter? _writer;
    private Schema? _schema;

    private bool _headerWritten;
    private bool _isFirstBatch = true;

    public async ValueTask DisposeAsync()
    {
        _writer?.Dispose();
    }

    /// <summary>
    /// Writes data to a file stream in CSV format
    /// </summary>
    /// <param name="batch">Records containing data to write</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Awaitable task</returns>
    public async ValueTask WriteAsync(RecordBatch batch, CancellationToken cancellation = default)
    {
        if (!_open)
        {
            _open = true;
            _writer = Sep.New(_readOptions.Delimiter[0])
                .Writer(r => r with { CultureInfo = CultureInfo.InvariantCulture, WriteHeader = false})
                .To(await fileStream.GetWriteStreamAsync(cancellation));
        }

        _schema ??= batch.Schema;

        if (_isFirstBatch)
        {
            _isFirstBatch = false;
            WriteHeader();
        }

        WriteBatch(batch);
    }

    private void WriteBatch(RecordBatch batch)
    {
        for (var i = 0; i < batch.RowCount; i++)
        {
            var row = _writer!.NewRow();
            var colIndex = 0;
            foreach (var column in batch.Results)
            {
                var val = column.GetStringValue(i);
                var colName = _schema!.Fields[colIndex++].Name;
                row[colName].Set(val);
            }
            row.Dispose();
        }
    }

    private void WriteHeader()
    {
        if (_headerWritten || !_readOptions.HasHeader)
        {
            return;
        }

        _headerWritten = true;

        using var headerRow = _writer!.NewRow();

        for (var i = 0; i < _schema!.Fields.Count; i++)
        {
            headerRow[_schema.Fields[i].Name].Set(_schema.Fields[i].Name);
        }
    }
}