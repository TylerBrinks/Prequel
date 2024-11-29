using Prequel.Data;

namespace Prequel.Engine.IO;

/// <summary>
/// Defines a type that writes queried data to a storage medium
/// </summary>
public interface IDataWriter : IAsyncDisposable
{
    ValueTask WriteAsync(RecordBatch batch, CancellationToken cancellation = default!);
}