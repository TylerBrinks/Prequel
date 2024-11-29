using Prequel.Engine.IO;
using Prequel.Data;
using Prequel.Engine.Source.Parquet;
using Exec = Prequel.Execution.ExecutionContext;

namespace Prequel.Tests.Integration;

public class QueryTestsParquet
{
    private readonly Exec _context;

    public QueryTestsParquet()
    {
        _context = new Exec();
        var root = Directory.GetCurrentDirectory().TrimEnd('\\', '/');

        Task.Run(async () =>
        {
            await _context.RegisterParquetFileAsync("parquet", new LocalFileStream($"{root}/Integration/test.parquet"));
        }).Wait();
    }

    private async Task<RecordBatch> ExecuteSingleBatchAsync(string sql, QueryContext? options = null)
    {
        var execution = _context.ExecuteQueryAsync(sql, options ?? new QueryContext());

        var enumerator = execution.GetAsyncEnumerator();

        try
        {
            await enumerator.MoveNextAsync();
            return enumerator.Current;
        }
        finally
        {
            await enumerator.DisposeAsync();
        }
    }

    [Fact]
    public async Task Query_Handles_Empty_Relations()
    {
        var batch = await ExecuteSingleBatchAsync("select * from parquet");
        Assert.Equal(8, batch.RowCount);
        Assert.Equal("id", batch.Schema.Fields[0].QualifiedName);
        Assert.Equal("bool_col", batch.Schema.Fields[1].QualifiedName);
        Assert.Equal("tinyint_col", batch.Schema.Fields[2].QualifiedName);
        Assert.Equal("smallint_col", batch.Schema.Fields[3].QualifiedName);
        Assert.Equal("int_col", batch.Schema.Fields[4].QualifiedName);
        Assert.Equal("bigint_col", batch.Schema.Fields[5].QualifiedName);
        Assert.Equal("float_col", batch.Schema.Fields[6].QualifiedName);
        Assert.Equal("double_col", batch.Schema.Fields[7].QualifiedName);
        Assert.Equal("date_string_col", batch.Schema.Fields[8].QualifiedName);
        Assert.Equal("string_col", batch.Schema.Fields[9].QualifiedName);
        Assert.Equal("timestamp_col", batch.Schema.Fields[10].QualifiedName);

        Assert.Equal(ColumnDataType.Integer, batch.Schema.Fields[0].DataType);
        Assert.Equal(ColumnDataType.Boolean, batch.Schema.Fields[1].DataType);
        Assert.Equal(ColumnDataType.Integer, batch.Schema.Fields[2].DataType);
        Assert.Equal(ColumnDataType.Integer, batch.Schema.Fields[3].DataType);
        Assert.Equal(ColumnDataType.Integer, batch.Schema.Fields[4].DataType);
        Assert.Equal(ColumnDataType.Integer, batch.Schema.Fields[5].DataType);
        Assert.Equal(ColumnDataType.Double, batch.Schema.Fields[6].DataType);
        Assert.Equal(ColumnDataType.Double, batch.Schema.Fields[7].DataType);
        Assert.Equal(ColumnDataType.Utf8, batch.Schema.Fields[8].DataType);
        Assert.Equal(ColumnDataType.Utf8, batch.Schema.Fields[9].DataType);
        Assert.Equal(ColumnDataType.TimestampNanosecond, batch.Schema.Fields[10].DataType);

        Assert.Equal((byte)4, batch.Results[0].Values[0]);
        Assert.Equal(true, batch.Results[1].Values[0]);
        Assert.Equal((byte)0, batch.Results[2].Values[0]);
        Assert.Equal((byte)0, batch.Results[3].Values[0]);
        Assert.Equal((byte)0, batch.Results[4].Values[0]);
        Assert.Equal((byte)0, batch.Results[5].Values[0]);
        Assert.Equal(0D, batch.Results[6].Values[0]);
        Assert.Equal(0D, batch.Results[7].Values[0]);
        Assert.Equal("03/01/09", batch.Results[8].Values[0]);
        Assert.Equal("0", batch.Results[9].Values[0]);
        Assert.Equal(DateTime.Parse("2009-03-01T12:00:00.0000000"), batch.Results[10].Values[0]);
    }
}