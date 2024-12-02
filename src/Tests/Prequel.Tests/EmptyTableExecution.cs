using System.Runtime.CompilerServices;
using Prequel.Data;
using Prequel.Execution;
using Prequel.Logical;

namespace Prequel.Tests;

public class EmptyTableExecution(Schema schema) : IExecutionPlan
{
    public Schema Schema { get; } = schema;


    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        await Task.CompletedTask;
        yield return new RecordBatch(Schema);
    }

    public string ToStringIndented(Indentation? indentation = null) => string.Empty;
}