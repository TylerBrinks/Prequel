using Prequel.Engine.Caching;
using Prequel.Data;
using Prequel.Engine.Source.Execution;
using Prequel.Tests.Fakes;

namespace Prequel.Tests.Database;

public class DatabaseDataTableTests
{
    [Fact]
    public void ExecutableDataTable_Uses_Output_Cache()
    {
        var schema = new Schema([]);
        var reader = new FakeDatabaseReader("query", () => null);
        var dataTable = new ExecutableDataTable("name", schema, reader, new CacheOptions { 
            UseDurableCache = true,
            DurableCacheKey = "key",
            CacheProvider = new FakeOutputCache(),
            GetDataWriter = _ => new FakeDataWriter()
        });
        var execution = dataTable.Scan([]);

        Assert.IsType<OutputCacheExecution>(execution);
    }

    [Fact]
    public void DatabaseDataTable_Bypasses_Output_Cache()
    {
        var schema = new Schema([]);
        var reader = new FakeDatabaseReader("query", () => null);
        var dataTable = new ExecutableDataTable("name", schema, reader, new CacheOptions());
        var execution = dataTable.Scan([]);

        Assert.IsType<DataSourceReaderExecution>(execution);
    }
}