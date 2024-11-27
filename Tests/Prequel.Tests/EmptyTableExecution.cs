using System.Runtime.CompilerServices;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Execution;

namespace Prequel.Tests;

public class EmptyTableExecution : IExecutionPlan
{
    public EmptyTableExecution(Schema schema)
    {
        Schema = schema;
    }

    public Schema Schema { get; }


    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        await Task.CompletedTask;
        yield return new RecordBatch(Schema);
    }
}