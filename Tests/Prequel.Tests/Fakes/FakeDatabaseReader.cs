using System.Data;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Source.Database;

namespace Prequel.Tests.Fakes;

public class FakeDatabaseReader : DatabaseDataSourceReader
{
    public int QueryCount;

    public FakeDatabaseReader(string query, Func<IDbConnection> connectionFactory) : base(query, connectionFactory)
    {
    }

    public override IAsyncEnumerable<object?[]> ReadSourceAsync(QueryContext queryContext, CancellationToken cancellation = default)
    {
        QueryCount++;
        return base.ReadSourceAsync(queryContext, cancellation);
    }

    public override ValueTask<Schema> QuerySchemaAsync(CancellationToken cancellation = default)
    {
        return ValueTask.FromResult(new Schema([]));
    }
}
