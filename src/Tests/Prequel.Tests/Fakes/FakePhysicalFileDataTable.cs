using Prequel.Tests.Fakes;
using Prequel.Engine.Caching;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Execution;
using Prequel.Engine.Source.File;

public class FakePhysicalFileDataTable : PhysicalFileDataTable
{
    public FakePhysicalFileDataTable(
        string tableName, 
        QueryContext queryContext,
        CacheOptions? cacheOptions = null
        ) : base(tableName, cacheOptions ?? new())
    {
    }

    public override Schema? Schema => new ([new("field1", ColumnDataType.Integer)]);

    public override IExecutionPlan Scan(List<int> projection)
    {
        return new FakeTableExecution(Schema, projection);
    }

    public override async IAsyncEnumerable<List<string?[]>> ReadAsync(List<int> indices, QueryContext queryContext, CancellationToken cancellation = default)
    {
        await Task.CompletedTask;
        yield return
        [
            Array.Empty<string?>(),
            Array.Empty<string>()
        ];
        yield return
        [
            Array.Empty<string?>(),
            Array.Empty<string>()
        ];
    }

    public override Task InferSchemaAsync(QueryContext queryContext, CancellationToken cancellation = default)
    {
        return Task.CompletedTask;
    }
}