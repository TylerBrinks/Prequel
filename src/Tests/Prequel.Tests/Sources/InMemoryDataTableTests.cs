using Prequel.Data;
using Prequel.Engine.Source.Memory;

namespace Prequel.Tests.Sources
{
    public class InMemoryDataTableTests
    {
        [Fact]
        public async Task InMemoryWriter_Writes_Batches_To_source()
        {
            var schema = new Schema(
            [
                new("0", ColumnDataType.Utf8),
                new("1", ColumnDataType.Utf8),
            ]);

            var batch1 = new RecordBatch(schema);
            var batch2 = new RecordBatch(schema);

            batch1.Results[0].Add("one");
            batch1.Results[1].Add("two");
            batch2.Results[0].Add("three");
            batch2.Results[1].Add("four");

            var writer = new InMemoryDataWriter("", new QueryContext());

            await writer.WriteAsync(new List<RecordBatch> { batch1, batch2 }.ToIAsyncEnumerable());

            var mem = (InMemoryDataTable)writer.DataTable;
            var plan = mem.Scan([0, 1]);
            Assert.Equal(schema, mem.Schema);

            await foreach (var batch in plan.ExecuteAsync(new QueryContext { BatchSize = 2 }))
            {
                Assert.Equal(2, batch.RowCount);
            }
        }

        [Fact]
        public async Task InMemoryDable_Enumerates_Without_Batch_Data()
        {
            var table = new InMemoryDataTable("name");

            var batches = new List<RecordBatch>();

            await foreach (var batch in table.ExecuteAsync(new QueryContext()))
            {
                batches.Add(batch);
            }

            Assert.Empty(batches);
            Assert.Equal("name", table.Name);
        }
    }
}
