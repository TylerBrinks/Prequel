using Prequel.Data;
using Prequel.Engine.Source.Database;
using Prequel.Engine.Source.MsSql;

namespace Prequel.Tests.Database;

public class DatabaseTests
{
    [Fact]
    public async Task MsSqlDatabaseAdapter_Reads_Raw_Results()
    {
        var adapter = new MsSqlDatabaseReader("select * from data", () => new TestDbConnection());

        var rows = new List<object?[]>();

        await foreach (var result in adapter.ReadSourceAsync(new QueryContext()))
        {
            rows.Add(result);
        }

        Assert.Single(rows);
        var batch = rows.First();

        Assert.Equal(1, batch[0]);
    }

    [Fact]
    public async Task LimitDatabaseAdapter_Reads_Raw_Results()
    {
        var adapter = new LimitDatabaseReader("select * from data", () => new TestDbConnection());

        var rows = new List<object?[]>();

        await foreach (var result in adapter.ReadSourceAsync(new QueryContext()))
        {
            rows.Add(result);
        }

        Assert.Single(rows);
        var batch = rows.First();

        Assert.Equal(1, batch[0]);
    }

    [Fact]
    public async Task MsSqlDatabaseAdapter_Queries_Schema()
    {
        var adapter = new MsSqlDatabaseReader("select * from data", () => new TestDbConnection());
        var schema = await adapter.QuerySchemaAsync();

        var expectedSchema = new Schema([new("FieldName", ColumnDataType.Integer)]);

        Assert.Equal(expectedSchema, schema);
    }

    [Fact]
    public async Task LimitDatabaseAdapter_Queries_Schema()
    {
        var adapter = new LimitDatabaseReader("select * from data", () => new TestDbConnection());
        var schema = await adapter.QuerySchemaAsync();

        var expectedSchema = new Schema([new("FieldName", ColumnDataType.Integer)]);

        Assert.Equal(expectedSchema, schema);
    }
}