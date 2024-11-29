using Prequel.Engine.Core.Data;
using Prequel.Engine.Source.Memory;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Execution;

namespace Prequel.Tests.Sources;

public class MultiDataTableTests
{
    [Fact]
    public async Task MultiSourceDataTable_Wraps_Child_Source_Execution()
    {
        var schema = new Schema([new("one", ColumnDataType.Utf8)]);

        var table1 = new InMemoryDataTable("table1");
        var table2 = new InMemoryDataTable("table1");

        var batch1 = new RecordBatch(schema);
        batch1.AddResult(0, "x");
        var batch2 = new RecordBatch(schema);
        batch2.AddResult(0, "y");

        table1.AddBatch(batch1);
        table2.AddBatch(batch2);

        var multi = new MultiSourceDataTable("multi", new[] { table1, table2 }, schema);

        var plan = multi.Scan([0]);

        Assert.IsType<MultiPlanExecution>(plan);

        var batches = new List<RecordBatch>();
        await foreach (var batch in plan.ExecuteAsync(new QueryContext()))
        {
            batches.Add(batch);
        }

        Assert.Equal(2, batches.Count);
    }
}