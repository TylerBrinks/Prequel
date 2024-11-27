using Prequel.Engine.Core.Data;
using Prequel.Engine.Source.Avro;
using Prequel.Tests.Fakes;
using Prequel.Engine.IO;
using Prequel.Engine.Source.Csv;
using Prequel.Engine.Source.Json;
using Prequel.Engine.Source.Parquet;

namespace Prequel.Tests.IO;

public class DataWriteTests
{
    [Fact]
    public async Task CsvDataSource_Writes_Csv_File()
    {
        var stream = new FakeFileStream();
        var writer = new CsvDataWriter(stream, new CsvReadOptions{HasHeader = false});
        await WriteData(writer);
        await writer.DisposeAsync();
        await ValidateRead(await CsvDataTable.FromStreamAsync("", stream, readOptions: new CsvReadOptions{HasHeader = false}));
    }

    [Fact]
    public async Task JsonDataSource_Writes_Json_File()
    {
        var stream = new FakeFileStream();
        var writer = new JsonDataWriter(stream);
        await WriteData(writer);
        await writer.DisposeAsync();
        await ValidateRead(await JsonDataTable.FromStreamAsync("", stream));
    }

    [Fact]
    public async Task AvroDataSource_Write_Avro_File()
    {
        var stream = new FakeFileStream();
        var writer = new AvroDataWriter(stream);
        await WriteData(writer);
        await writer.DisposeAsync();
        await ValidateRead(await AvroDataTable.FromStreamAsync("", stream));
    }

    [Fact]
    public async Task ParquetDataSource_Write_Parquet_File()
    {
        var stream = new FakeFileStream();
        var writer = new ParquetDataWriter(stream);
        await WriteData(writer);
        await writer.DisposeAsync();
        await ValidateRead(await ParquetDataTable.FromStreamAsync("", stream));
    }

    private static async Task ValidateRead(DataTable table)
    {
        var execution = table.Scan([0, 1]);
        var batches = await execution.ExecuteAsync(new QueryContext { BatchSize = 2 }).ToListAsync();
        ValidateRecords(batches);
    }

    private static async Task WriteData(IDataWriter writer)
    {
        foreach(var batch in CreateTestRecords())
        {
            await writer.WriteAsync(batch);
        }
    }

    private static void ValidateRecords(IReadOnlyList<RecordBatch> values)
    {
        Assert.Equal(2, values.Count);

        Assert.Equal("red", values[0].Results[0].Values[0]);
        Assert.Equal("blue", values[0].Results[0].Values[1]);
        Assert.Equal("green", values[1].Results[0].Values[0]);
        Assert.Equal("orange", values[1].Results[0].Values[1]);

        Assert.Equal((byte)1, values[0].Results[1].Values[0]);
        Assert.Equal((byte)2, values[0].Results[1].Values[1]);
        Assert.Equal((byte)3, values[1].Results[1].Values[0]);
        Assert.Equal((byte)4, values[1].Results[1].Values[1]);
    }

    private static IEnumerable<RecordBatch> CreateTestRecords()
    {
        var schema = new Schema([
            new("color", ColumnDataType.Utf8),
            new("number", ColumnDataType.Integer)
        ]);

        var batch1 = new RecordBatch(schema);
        batch1.Results[0].Add("red");
        batch1.Results[0].Add("blue");
        batch1.Results[1].Add(1);
        batch1.Results[1].Add(2);

        var batch2 = new RecordBatch(schema);
        batch2.Results[0].Add("green");
        batch2.Results[0].Add("orange");
        batch2.Results[1].Add(3);
        batch2.Results[1].Add(4);

        return new List<RecordBatch> { batch1, batch2 };
    }
}