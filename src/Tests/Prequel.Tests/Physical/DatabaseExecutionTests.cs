using System.Data;
using Prequel.Engine.Caching;
using Moq;
using Prequel.Tests.Fakes;
using Prequel.Engine.Source.Execution;
using Prequel.Data;

namespace Prequel.Tests.Physical;

public class DatabaseExecutionTests
{
    [Fact]
    public async Task DatabaseExecution_Queries_Schema_Column_In_Batches()
    {
        var schema = new Schema([
            new("one", ColumnDataType.Integer),
            new("two", ColumnDataType.Integer),
            new("three", ColumnDataType.Integer)
        ]);

        var projection = new List<int> { 0, 2 };
        var reader = new FakeDataSourceReader([
            [1, 2, 3],
            [4, 5, 6],
            [7, 8, 9]
        ]);

        var execution = new DataSourceReaderExecution(schema, projection, reader);

        var batches = new List<RecordBatch>();

        await foreach (var batch in execution.ExecuteAsync(new QueryContext { BatchSize = 2 }))
        {
            batches.Add(batch);
        }

        Assert.Equal(2, batches.Count);
        Assert.Equal((byte)1, batches[0].Results[0].Values[0]);
        Assert.Equal((byte)4, batches[0].Results[0].Values[1]);

        Assert.Equal((byte)3, batches[0].Results[1].Values[0]);
        Assert.Equal((byte)6, batches[0].Results[1].Values[1]);

        Assert.Equal((byte)7, batches[1].Results[0].Values[0]);
        Assert.Equal((byte)9, batches[1].Results[1].Values[0]);

    }

    [Fact]
    public async Task CachedDatabaseReader_Reads_From_Source_Initially()
    {
        var read = false;
        var dbConnection = new Mock<IDbConnection>();
        var command = new Mock<IDbCommand>();
        var dbReader = new Mock<IDataReader>();

        dbConnection.Setup(db => db.CreateCommand()).Returns(command.Object);
        command.Setup(cmd => cmd.ExecuteReader()).Returns(dbReader.Object);
        dbReader.Setup(r => r.FieldCount).Returns(1);
        dbReader.Setup(r => r.GetValues(It.IsAny<object[]>()));
        dbReader.Setup(r => r.Read()).Returns(() =>
        {
            if (read) { return false; }
            read = true;
            return true;

        });

        var testReader = new FakeDatabaseReader("query", () => dbConnection.Object);
        var reader = new MemoryCachedDataSourceReader(testReader);

        await foreach (var _ in reader.ReadSourceAsync(new QueryContext()))
        {
        }

        await foreach (var _ in reader.ReadSourceAsync(new QueryContext ()))
        {
        }

        Assert.Equal(1, testReader.QueryCount);
    }

    [Fact]
    public void DatabaseDataTable_Builds_Tables_From_Readers()
    {
        var read = false;
        var dbConnection = new Mock<IDbConnection>();
        var command = new Mock<IDbCommand>();
        var dbReader = new Mock<IDataReader>();

        dbConnection.Setup(db => db.CreateCommand()).Returns(command.Object);
        command.Setup(cmd => cmd.ExecuteReader()).Returns(dbReader.Object);
        dbReader.Setup(r => r.FieldCount).Returns(1);
        dbReader.Setup(r => r.GetValues(It.IsAny<object[]>()));
        dbReader.Setup(r => r.Read()).Returns(() =>
        {
            if (read) { return false; }
            read = true;
            return true;

        });

        var testReader = new FakeDatabaseReader("query", () => dbConnection.Object);
        var schema = new Schema([]);
       
        var table = new ExecutableDataTable("name", schema, testReader, new CacheOptions());
        var plan = table.Scan([1]);
        Assert.IsType<DataSourceReaderExecution>(plan);
    }
}