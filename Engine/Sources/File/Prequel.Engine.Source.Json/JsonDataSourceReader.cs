using System.Runtime.CompilerServices;
using System.Text.Json;
using Prequel.Engine.Core;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Metrics;
using Prequel.Engine.IO;

namespace Prequel.Engine.Source.Json;

/// <summary>
/// JSON file data reader
/// </summary>
/// <param name="fileStream">File stream instance</param>
internal class JsonDataSourceReader(IFileStream fileStream) : IDataSourceReader
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
        using var step = queryContext.Profiler.Step("Data Source Reader, Json data source, Read file");

        var line = new List<object?>();

        while (await reader.ReadLineAsync(cancellation) is { } json)
        {
            using var document = JsonDocument.Parse(json, _documentOptions);
            var record = document.RootElement;

            foreach (var property in record.EnumerateObject().OrderBy(o => o.Name))
            {
                if (!_headers.Contains(property.Name))
                {
                    _headers.Add(property.Name);
                }

                var value = property.Value.GetRawText().TrimQuotes();
                line.Add(value);
            }

            if (line.Count == _headers.Count)
            {
                step.IncrementRowCount();
                yield return line.ToArray();
            }

            line.Clear();
        }
    }

    public string[] Headers => [.. _headers.OrderBy(h => h)];
}