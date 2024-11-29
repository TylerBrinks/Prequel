using ParquetSharp;

namespace Prequel.Engine.Source.Parquet;

internal sealed class ParquetRecordColumnReader(int batchSize, PhysicalType columnType) : ILogicalColumnReaderVisitor<bool>
{
    internal List<object?> Results { get; private set; } = null!;

    public bool OnLogicalColumnReader<TValue>(LogicalColumnReader<TValue> columnReader)
    {
        var buffer = new TValue[batchSize];
        var readCount = columnReader.ReadBatch(buffer);

        Results = [];
        Results.AddRange(buffer[..readCount].Cast<object?>());

        switch (columnType)
        {
            case PhysicalType.Int96:
            {
                for (var i = 0; i < Results.Count; i++)
                {
                    if (Results[i] == null || Results[i] is not Int96) { continue; }

                    Results[i] = ConvertToDateTime((Int96)Results[i]!);
                }

                break;
            }
            case PhysicalType.ByteArray:
            {
                for (var i = 0; i < Results.Count; i++)
                {
                    if (Results[i] == null || Results[i] is not byte[]) { continue; }

                    Results[i] = System.Text.Encoding.UTF8.GetString((byte[])Results[i]!);
                }

                break;
            }
        }

        return true;

        static DateTime ConvertToDateTime(Int96 typed)
        {
            var date = DateTime.FromOADate(typed.C - 2415018.5);
            var a = BitConverter.GetBytes(typed.A);
            var b = BitConverter.GetBytes(typed.B);

            var nano = a.Concat(b).ToArray();

            var nanoLong = BitConverter.ToInt64(nano, 0);

            var result = date.AddTicks(nanoLong / 100);
            return result;
        }
    }
}