using System.Runtime.CompilerServices;
using Prequel.Engine.Core.Data;
using Prequel.Engine.IO;

namespace Prequel.Tests.Fakes;

public class FakeDataSourceReader : ISchemaDataSourceReader
{
    private readonly List<object?[]> _data;
    public bool SourceRead;

    public FakeDataSourceReader(List<object?[]> data)
    {
        _data = data;
    }

    public async IAsyncEnumerable<object?[]> ReadSourceAsync(QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default)
    {
        SourceRead = true;
        await Task.CompletedTask;
        foreach (var row in _data)
        {
            yield return row;
        }
    }

    public ValueTask<Schema> QuerySchemaAsync(CancellationToken cancellation = default)
    {
        throw new NotImplementedException();
    }
}
