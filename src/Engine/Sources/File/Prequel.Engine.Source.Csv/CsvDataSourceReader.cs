using System.Globalization;
using System.Runtime.CompilerServices;
using nietras.SeparatedValues;
using Prequel.Engine.Core.Metrics;
using Prequel.Engine.Core.Data;
using Prequel.Engine.IO;

namespace Prequel.Engine.Source.Csv;

/// <summary>
/// CSV file data source reader
/// </summary>
/// <param name="fileStream">File stream instance</param>
/// <param name="readOptions">CSV read options</param>
public class CsvDataSourceReader(IFileStream fileStream, CsvReadOptions? readOptions = null) : IDataSourceReader
{
    private readonly CsvReadOptions _readOptions = readOptions ?? new();
    private List<string> _headers = [];

    public async IAsyncEnumerable<object?[]> ReadSourceAsync(QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        using var csv = Sep.New(_readOptions.Delimiter[0]).Reader(r => r with
        {
            HasHeader = _readOptions.HasHeader,
            CultureInfo = CultureInfo.InvariantCulture,
            DisableColCountCheck = true
        })
        .From(await fileStream.GetReadStreamAsync(cancellation));

        _headers = ProcessHeaderRow(csv);

        using var step = queryContext.Profiler.Step("Data Source Reader, CSV data source, Read file");

        foreach (var row in ReadInternal(csv, step))
        {
            step.IncrementRowCount();
            yield return row;
        }
    }

    public IEnumerable<object?[]> ReadInternal(SepReader reader, Timing step)
    {
        foreach (var row in reader)
        {
            step.IncrementRowCount();

            if (_headers.Count == 0)
            {
                for (var i = 0; i < row.ColCount; i++)
                {
                    _headers.Add($"column_{i + 1}");
                }
            }

            var line = new object?[_headers.Count];

            for (var i = 0; i < _headers.Count; i++)
            {
                var value = row[i];

                try
                {
                    line[i] = value.ToString();
                }
                catch
                {
                    /**/
                }
            }

            yield return line;
        }
    }

    /// <summary>
    /// Reads headers from a CSV file if any exist.
    /// </summary>
    /// <param name="reader">CSV Reader</param>
    /// <returns>List of header names</returns>
    private List<string> ProcessHeaderRow(SepReader reader)
    {
        var headers = new List<string>();

        if (!_readOptions.HasHeader)
        {
            return headers;
        }

        headers = [.. reader.Header.ColNames];

        return headers;
    }

    public string[] Headers => [.. _headers];
}
