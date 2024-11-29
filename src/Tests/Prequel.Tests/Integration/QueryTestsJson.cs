using Prequel.Engine.IO;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Source.Json;
using Exec = Prequel.Engine.Core.Execution.ExecutionContext;

namespace Prequel.Tests.Integration;

public class QueryTestsJson
{
    private readonly Exec _context;

    public QueryTestsJson()
    {
        _context = new Exec();
        var root = Directory.GetCurrentDirectory().TrimEnd('\\', '/');
      
        Task.Run(async () =>
        {
            await _context.RegisterJsonFileAsync("json", new LocalFileStream($"{root}/Integration/db_json.json"));
        }).Wait();
    }

    private async Task<RecordBatch> ExecuteSingleBatchAsync(string sql, QueryContext? options = null)
    {
        var execution = _context.ExecuteQueryAsync(sql, options ?? new QueryContext());

        var enumerator = execution.GetAsyncEnumerator();

        await enumerator.MoveNextAsync();
        return enumerator.Current;
    }

    [Fact]
    public async Task Query_Handles_Empty_Relations()
    {
        var batch = await ExecuteSingleBatchAsync("select * from json");
        Assert.Equal(3, batch.RowCount);
        Assert.Equal("a", batch.Schema.Fields[0].QualifiedName);
        Assert.Equal("b", batch.Schema.Fields[1].QualifiedName);
        Assert.Equal("c", batch.Schema.Fields[2].QualifiedName);
        Assert.Equal("d", batch.Schema.Fields[3].QualifiedName);
        Assert.Equal("e", batch.Schema.Fields[4].QualifiedName);
        Assert.Equal("f", batch.Schema.Fields[5].QualifiedName);
        Assert.Equal("g", batch.Schema.Fields[6].QualifiedName);

        Assert.Equal(ColumnDataType.Double, batch.Schema.Fields[0].DataType);
        Assert.Equal(ColumnDataType.Utf8, batch.Schema.Fields[1].DataType);
        Assert.Equal(ColumnDataType.Utf8, batch.Schema.Fields[2].DataType);
        Assert.Equal(ColumnDataType.Integer, batch.Schema.Fields[3].DataType);
        Assert.Equal(ColumnDataType.Boolean, batch.Schema.Fields[4].DataType);
        Assert.Equal(ColumnDataType.Date32, batch.Schema.Fields[5].DataType);
        Assert.Equal(ColumnDataType.TimestampNanosecond, batch.Schema.Fields[6].DataType);

        Assert.Equal(-10D, batch.Results[0].Values[0]);
        Assert.Equal("[2.0, 1.3, -6.1]", batch.Results[1].Values[0]);
        Assert.Equal("[true, true]", batch.Results[2].Values[0]);
        Assert.Equal((byte)4, batch.Results[3].Values[0]);
        Assert.Equal(true, batch.Results[4].Values[0]);
        Assert.Equal(DateTime.Parse("2003-02-17"), batch.Results[5].Values[0]);
        Assert.Equal(DateTime.Parse("2001-02-03T11:22:33.123"), batch.Results[6].Values[0]);
    }
}