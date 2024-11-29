using Prequel.Engine.Caching;
using Prequel.Engine.Core.Data;
using Prequel.Model.Execution.Database;

namespace Prequel.Model.Execution;

public class DatabaseDataTableReference : DataTableReference
{
    public override async ValueTask<DataTable> BuildAsync(CacheOptions cacheOptions, CancellationToken cancellation = default!)
    {
        var dbConnection = (IDatabaseDataSourceConnection)Connection;

        return await dbConnection.BuildAsync(Name, Query!, Schema, cacheOptions, cancellation);
    }

    public required string Query { get; set; }
}