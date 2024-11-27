using Prequel.Engine.Caching;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Execution;
using Prequel.Engine.Source.File;

namespace Prequel.Tests.Fakes;

public class FakeDataTable : PhysicalFileDataTable
{
    public FakeDataTable(string tableName, Schema schema, ICollection<int> projection)
        : base(tableName, new CacheOptions())
    {
        var fields = schema.Fields.Where((_, i) => projection.Contains(i)).ToList();
        Schema = new Schema(fields);
    }

    public override Schema Schema { get; }

    public override IExecutionPlan Scan(List<int> projection)
    {
        return new FakeTableExecution(Schema, projection);
    }

    public override IAsyncEnumerable<List<string?[]>> ReadAsync(List<int> indices, QueryContext queryContext, CancellationToken cancellation = default!)
    {
        throw new NotImplementedException();
    }

    //public override Task WriteAsync(IAsyncEnumerable<RecordBatch> records, CancellationToken cancellation = default)
    //{
    //    throw new NotImplementedException();
    //}

    public override Task InferSchemaAsync(QueryContext queryContext, CancellationToken cancellation = default)
    {
        throw new NotImplementedException();
    }
}