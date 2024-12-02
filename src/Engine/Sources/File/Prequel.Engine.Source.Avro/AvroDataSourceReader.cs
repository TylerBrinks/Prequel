using SolTechnology.Avro;
using System.Text.Json;
using System.Runtime.CompilerServices;
using Prequel.Metrics;
using Prequel.Data;
using Prequel.Engine.IO;

namespace Prequel.Engine.Source.Avro;

/// <summary>
/// Avro file data reader
/// </summary>
/// <param name="fileStream">File stream instance</param>
internal class AvroDataSourceReader(IFileStream fileStream) : IDataSourceReader
{
    private readonly List<string> _headers = [];

    private readonly JsonDocumentOptions _documentOptions = new()
    {
        AllowTrailingCommas = true,
        CommentHandling = JsonCommentHandling.Skip
    };

    public async IAsyncEnumerable<object?[]> ReadSourceAsync(QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        using var reader = new StreamReader(await fileStream.GetReadStreamAsync(cancellation));

        var json = $"{AvroConvert.Avro2Json(await fileStream.ReadAllBytesAsync(cancellation))}";
        using var document = JsonDocument.Parse(json, _documentOptions);

        using var step = queryContext.Profiler.Step("Data Source Reader, Avro data source, Read file");
        var line = new List<object?>();

        foreach (var record in document.RootElement.EnumerateArray())
        {
            foreach (var property in record.EnumerateObject().OrderBy(o => o.Name))
            {
                if (!_headers.Contains(property.Name))
                {
                    _headers.Add(property.Name);
                }

                var value = property.Value.GetRawText().TrimQuotes();
                line.Add(value);
            }

            step.IncrementRowCount();

            yield return line.ToArray();
            line.Clear();
        }
    }

    public string[] Headers => [.. _headers.OrderBy(h => h)];

    public override string ToString() => $"Avro data source reader: {fileStream}";
}
