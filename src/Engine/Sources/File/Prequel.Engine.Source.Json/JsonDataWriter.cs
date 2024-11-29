using System.Text;
using System.Text.Json;
using Prequel.Data;
using Prequel.Engine.IO;

namespace Prequel.Engine.Source.Json;

/// <summary>
/// JSON file data writer
/// </summary>
/// <param name="fileStream">File stream instance</param>
public class JsonDataWriter(IFileStream fileStream) : IDataWriter
{
    private bool _open;
    private TextWriter? _textWriter;
    private Schema? _schema;

    public async ValueTask DisposeAsync()
    {
        if (_textWriter != null)
        {
            await _textWriter.DisposeAsync();
        }
    }

    /// <summary>
    /// Writes data to a file stream in JSON format
    /// </summary>
    /// <param name="batch">Records containing data to write</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Awaitable task</returns>
    public async ValueTask WriteAsync(RecordBatch batch, CancellationToken cancellation = default)
    {
        if (!_open)
        {
            _open = true;
            _textWriter = new StreamWriter(await fileStream.GetWriteStreamAsync(cancellation), Encoding.UTF8);
        }

        _schema ??= batch.Schema;

        var document = new Dictionary<string, object>();

        for (var i = 0; i < batch.RowCount; i++)
        {
            for (var j = 0; j < _schema.Fields.Count; j++)
            {
                document[_schema.Fields[j].Name] = batch.Results[j].Values[i] ?? "";
            }

            if (_textWriter != null)
            {
                await _textWriter?.WriteLineAsync(JsonSerializer.Serialize(document))!;
            }
        }
    }
}