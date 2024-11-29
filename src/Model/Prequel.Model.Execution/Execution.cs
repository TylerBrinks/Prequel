using Prequel.Engine.Source.Memory;
using Prequel.Data;
using ExecutionContext = Prequel.Execution.ExecutionContext;

namespace Prequel.Model.Execution;

/// <summary>
/// Query execution
/// </summary>
public class Execution
{
    private readonly ExecutionContext _context = new();
    private List<Query> _queries = [];

    private bool _initialized;

    public Execution() { }

    /// <summary>
    /// Initialize all connection tables
    /// </summary>
    private async Task Initialize()
    {
        if (_initialized)
        {
            return;
        }

        _initialized = true;

        _queries = [.. _queries.OrderBy(q => q.Index)];

        foreach (var refTable in ConnectionTables)
        {
            _context.RegisterDataTable(await refTable.BuildAsync(refTable.CacheOptions));
        }
    }
    /// <summary>
    /// Executes a specific query
    /// </summary>
    /// <param name="index">Query index</param>
    /// <param name="cancellation">Cancellation token</param>
    /// <returns>Query execution result</returns>
    public async Task<QueryExecutionResult> ExecuteAsync(
        int index,
        CancellationToken cancellation = default!)
    {
        await Initialize();

        var query = _queries[index];

        var result = await query.ExecuteQueryAsync(_context, cancellation: cancellation);

        result.Timing = query.QueryContext.Profiler.Root;

        await RegisterInMemoryTable(query.Name, result.Batches.ToAsyncEnumerable());

        return result;
    }
    /// <summary>
    /// Executes all queries sequentially
    /// </summary>
    /// <param name="cancellation">Cancellation token</param>
    /// <returns>WorkspaceResult</returns>
    public async Task<QueryResult> ExecuteAllAsync(CancellationToken cancellation = default!)
    {
        await Initialize();

        var workspaceResult = new QueryResult();

        for (var i = 0; i < Queries.Count; i++)
        {
            var cellResult = await ExecuteAsync(i, cancellation);
            workspaceResult.QueryResults.Add(cellResult);
        }

        return workspaceResult;
    }

    public async Task<DataTable> RegisterInMemoryTable(string name, IAsyncEnumerable<RecordBatch> executeQueryAsync)
    {
        var table = new InMemoryDataTable(name);

        await foreach (var batch in executeQueryAsync)
        {
            table.AddBatch(batch.CloneBatch());
        }

        _context.RegisterDataTable(table);
        return table;
    }

    public void AddQuery(Query query)
    {
        _queries.Add(query);
        query.Index = Queries.Count;
    }


    public ICollection<Query> Queries
    {
        get => _queries;
        set => _queries = value.ToList();
    }

    public List<DataTableReference> ConnectionTables { get; } = [];
}