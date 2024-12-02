using System.Runtime.CompilerServices;
using Prequel.Data;
using Prequel.Logical;
using Prequel.Logical.Plans;

namespace Prequel.Execution;

internal class ExplainExecution(Explain explain) : IExecutionPlan
{
    public Schema Schema => explain.Schema;

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(QueryContext queryContext, 
        [EnumeratorCancellation] CancellationToken cancellation = default)
    {
        var steps = explain.ToStringIndented(new Indentation()).Split(Environment.NewLine);
        var batch = new RecordBatch(Schema);

        foreach (var step in steps)
        {
           batch.AddResult(0, step);
        }

        yield return batch;
    }
}
