using Prequel.Data;
using Prequel.Engine.IO;

namespace Prequel.Tests.Fakes;

public class FakeDataWriter : IDataWriter
{
    public List<RecordBatch> Records { get; } = new();

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    public ValueTask WriteAsync(RecordBatch batch, CancellationToken cancellation = default)
    {
        Records.Add(batch);

        return ValueTask.CompletedTask;
    }
}