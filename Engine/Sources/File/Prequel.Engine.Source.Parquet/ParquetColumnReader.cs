using ParquetSharp;

namespace Prequel.Engine.Source.Parquet;

internal record ParquetColumnReader(
    ColumnReader Reader,
    LogicalColumnReader LogicalReader,
    ParquetRecordColumnReader Visitor) : IDisposable
{
    public void Dispose()
    {
        LogicalReader.Dispose();
        Reader.Dispose();
    }
}