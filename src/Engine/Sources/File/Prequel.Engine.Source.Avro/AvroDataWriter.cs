using System.Text.Json;
using Prequel.Data;
using Prequel.Engine.IO;
using SolTechnology.Avro;

namespace Prequel.Engine.Source.Avro;

/// <summary>
/// Avro file data writer
/// </summary>
/// <param name="fileStream">File stream instance</param>
public class AvroDataWriter(IFileStream fileStream) : IDataWriter
{
    private RecordBatch? _writeBatch;

    public async ValueTask DisposeAsync()
    {
        await using var stream = await fileStream.GetWriteStreamAsync();

        var rows = new List<Dictionary<string, object>>();
        var schema = _writeBatch!.Schema;

        for (var i = 0; i < _writeBatch.RowCount; i++)
        {
            var document = new Dictionary<string, object>();

            for (var j = 0; j < schema.Fields.Count; j++)
            {
                document[schema.Fields[j].Name] = _writeBatch.Results[j].Values[i] ?? "";
            }

            rows.Add(document);
        }

        var json = JsonSerializer.Serialize(rows);
        var avroBytes = AvroConvert.Json2Avro(json);
        await stream.WriteAsync(avroBytes);
        await stream.FlushAsync();
    }

    /// <summary>
    /// Writes data to a file stream in Avro format
    /// </summary>
    /// <param name="batch">Records containing data to write</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Awaitable task</returns>
    public async ValueTask WriteAsync(RecordBatch batch, CancellationToken cancellation = default)
    {
        _writeBatch ??= new RecordBatch(batch.Schema);

        _writeBatch.Concat(batch);
        await ValueTask.CompletedTask;
    }
}