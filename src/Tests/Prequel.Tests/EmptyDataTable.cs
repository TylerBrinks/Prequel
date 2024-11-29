using Prequel.Engine.Caching;
using Prequel.Data;
using Prequel.Execution;
using Prequel.Engine.Source.File;

namespace Prequel.Tests;

public class EmptyDataTable : PhysicalFileDataTable
{
    public EmptyDataTable(
        string tableName, 
        Schema schema, 
        ICollection<int> projection,
        CacheOptions? cacheOptions = null
        )
        : base(tableName, cacheOptions ?? new())
    {
        var fields = schema.Fields.Where((_, i) => projection.Contains(i)).ToList();
        Schema = new Schema(fields);
    }

    public override Schema Schema { get; }

    public override IExecutionPlan Scan(List<int> projection)
    {
        return new EmptyTableExecution(Schema);
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