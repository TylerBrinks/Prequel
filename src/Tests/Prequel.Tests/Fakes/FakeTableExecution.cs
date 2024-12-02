using System.Runtime.CompilerServices;
using Prequel.Data;
using Prequel.Execution;
using Prequel.Logical;

namespace Prequel.Tests.Fakes;

public class FakeTableExecution : IExecutionPlan
{
    private readonly Random _random = new();
    private readonly List<int> _projection;
    private bool _executed;

    public FakeTableExecution(Schema schema, List<int> projection)
    {
        Schema = schema;
        _projection = projection;
    }

    public Schema Schema { get; }
    public bool Executed => _executed;

    public async IAsyncEnumerable<RecordBatch> ExecuteAsync(QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        _executed = true;

        await foreach (var slice in Read(_projection, queryContext.BatchSize).WithCancellation(cancellation))
        {
            var batch = new RecordBatch(Schema);

            foreach (var line in slice)
            {
                for (var i = 0; i < line.Length; i++)
                {
                    batch.AddResult(i, line[i]);
                }
            }

            yield return batch;
        }
    }

    public string ToStringIndented(Indentation? indentation = null) => string.Empty;

    public async IAsyncEnumerable<List<string?[]>> Read(List<int> indices, int batchSize)
    {
        var data = new List<string?[]>(batchSize);

        for (var i = 0; i < batchSize; i++)
        {
            var line = new string?[indices.Count];

            for (var j = 0; j < indices.Count; j++)
            {
                var value = _random.Next(-100, 100);
                line[j] = value.ToString();
            }

            data.Add(line);
        }

        yield return await Task.FromResult(data);
    }
}