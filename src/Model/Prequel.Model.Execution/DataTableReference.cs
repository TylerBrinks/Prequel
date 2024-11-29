using System.ComponentModel.DataAnnotations.Schema;
using Prequel.Engine.Caching;
using Prequel.Data;
using Prequel.Model.Execution.Database;

namespace Prequel.Model.Execution;

public abstract class DataTableReference
{
    public required string Name { get; init; }

    [NotMapped]
    public CacheOptions CacheOptions { get; set; } = new();

    public int TenantId { get; set; }

    public required DataTableSchema Schema { get; init; }

    public required DataSourceConnection Connection { get; init; }

    public abstract ValueTask<DataTable> BuildAsync(CacheOptions cacheOptions, CancellationToken cancellation = default!);
}