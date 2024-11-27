using System.Runtime.CompilerServices;
using ParquetSharp;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Metrics;
using Prequel.Engine.IO;

namespace Prequel.Engine.Source.Parquet;

/// <summary>
/// Parquet data source reader
/// </summary>
/// <param name="fileStream">File stream instance</param>
public class ParquetDataSourceReader(IFileStream fileStream) : IDataSourceReader
{
    public async IAsyncEnumerable<object?[]> ReadSourceAsync(QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        List<ParquetColumnReader> readers = null!;

        try
        {
            await using var readStream = await fileStream.GetReadStreamAsync(cancellation);
            using var parquet = new ParquetFileReader(readStream);
            using var step = queryContext.Profiler.Step("Data Source Reader, Parquet data source, Read file");

            for (var rowGroup = 0; rowGroup < parquet.FileMetaData.NumRowGroups; ++rowGroup)
            {
                using var rowGroupReader = parquet.RowGroup(rowGroup);

                readers = new(rowGroupReader.MetaData.NumColumns);

                for (var columnIndex = 0; columnIndex < rowGroupReader.MetaData.NumColumns; columnIndex++)
                {
                    var columnReader = rowGroupReader.Column(columnIndex);
                    readers.Add(new(
                        columnReader,
                        columnReader.LogicalReader(),
                        new ParquetRecordColumnReader(queryContext.BatchSize, columnReader.Type)));
                }

                while (readers[0].LogicalReader.HasNext)
                {
                    foreach (var reader in readers)
                    {
                        reader.LogicalReader.Apply(reader.Visitor);
                    }

                    var rowCount = readers[0].Visitor.Results.Count;

                    for (var count = 0; count < rowCount; ++count)
                    {
                        step.IncrementRowCount();
                        var row = readers.Select(r => r.Visitor.Results[count]).ToArray();
                        yield return row;
                    }
                }
            }
        }
        finally
        {
            foreach (var reader in readers)
            {
                reader.Dispose();
            }
        }
    }
}