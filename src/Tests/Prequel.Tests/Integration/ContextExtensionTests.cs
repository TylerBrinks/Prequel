using Prequel.Engine.IO;
using Prequel.Data;
using Prequel.Engine.Source.Avro;
using Prequel.Engine.Source.Csv;
using Prequel.Engine.Source.Json;
using Prequel.Engine.Source.Parquet;
using Exec = Prequel.Execution.ExecutionContext;

namespace Prequel.Tests.Integration;

public class ContextExtensionTests
{
    [Fact]
    public async Task Context_Registers_Multiple_Files()
    {
        var context = new Exec();
        var root = Directory.GetCurrentDirectory().TrimEnd('\\', '/') + "/Integration";

        await context.RegisterAvroDirectoryAsync("avro", new LocalDirectory(root));
        await context.RegisterCsvDirectoryAsync("csv", new LocalDirectory(root));
        await context.RegisterJsonDirectoryAsync("json", new LocalDirectory(root));
        await context.RegisterParquetDirectoryAsync("parquet", new LocalDirectory(root));

        var avroTable = (MultiSourceDataTable)context.Tables["avro"];
        var csvTable = (MultiSourceDataTable)context.Tables["csv"];
        var jsonTable = (MultiSourceDataTable)context.Tables["json"];
        var parquetTable = (MultiSourceDataTable)context.Tables["parquet"];

        Assert.Equal(1, await avroTable.Scan([0]).ExecuteAsync(new QueryContext()).CountAsync());
        Assert.Equal(5, await csvTable.Scan([0]).ExecuteAsync(new QueryContext()).CountAsync());
        Assert.Equal(2, await jsonTable.Scan([0]).ExecuteAsync(new QueryContext()).CountAsync());
        Assert.Equal(1, await parquetTable.Scan([0]).ExecuteAsync(new QueryContext()).CountAsync());
    }

    [Fact]
    public async Task Context_Throws_For_Empty_Directories()
    {
        var context = new Exec();
        var directory = Directory.CreateTempSubdirectory();
        var root = directory.FullName;

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await context.RegisterAvroDirectoryAsync("avro", new LocalDirectory(root)));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await context.RegisterCsvDirectoryAsync("csv", new LocalDirectory(root)));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await context.RegisterJsonDirectoryAsync("json", new LocalDirectory(root)));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await context.RegisterParquetDirectoryAsync("parquet", new LocalDirectory(root)));

        Directory.Delete(root);
    }
}