using System.Runtime.CompilerServices;
using Prequel.Data;
using Prequel.Logical;
using Prequel.Logical.Plans;

namespace Prequel.Execution;

internal class ExplainExecution(Explain explain, IExecutionPlan? executionPlan) : IExecutionPlan
{
    public Schema Schema => explain.Schema;

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default)
    {
        var logicalSteps = explain.ToStringIndented().Split(Environment.NewLine);
        var batch = new RecordBatch(Schema);

        var index = 0;
        foreach (var step in logicalSteps)
        {
            var stepText = index++ == 0 ? "logical" : string.Empty;
            batch.AddResult(0, stepText);
            batch.AddResult(1, step);
        }

        yield return batch;

        var physicalSteps = executionPlan.ToStringIndented().Split(Environment.NewLine);
        executionPlan.ToStringIndented();

        var batch2 = new RecordBatch(Schema);

        index = 0;
        foreach (var step in physicalSteps)
        {
            var stepText = index++ == 0 ? "physical" : string.Empty;
            batch2.AddResult(0, stepText);
            batch2.AddResult(1, step);
        }

        yield return batch2;
    }

    public string ToStringIndented(Indentation? indentation = null) => string.Empty;
}
