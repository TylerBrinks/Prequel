using SqlParser.Ast;
using System.Runtime.CompilerServices;
using Prequel.Physical;
using Prequel.Metrics;
using Prequel.Data;
using Prequel.Logical;

namespace Prequel.Execution;

public class ExecutionContext
{
    private readonly Dictionary<string, DataTable> _tables = new();

    /// <summary>
    /// Registers a data source table
    /// </summary>
    /// <param name="table">Name used when querying the data source</param>
    public DataTable RegisterDataTable(DataTable table)
    {
        if (!_tables.TryAdd(table.Name, table))
        {
            throw new InvalidOperationException($"A table with the name {table.Name} already exists.");
        }

        return table;
    }

    /// <summary>
    /// Executes a SQL query against the registered data sources
    /// </summary>
    /// <param name="sql">SQL query</param>
    /// <param name="queryContext">Query context</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Enumerable record batches containing query results</returns>
    public async IAsyncEnumerable<RecordBatch> ExecuteQueryAsync(string sql, QueryContext? queryContext = null,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        queryContext ??= new QueryContext();

        var logicalPlan = BuildLogicalPlan(sql);
        if (queryContext.ModifyLogicalPlan != null)
        {
            logicalPlan = queryContext.ModifyLogicalPlan?.Invoke(logicalPlan);
        }

        var physicalPlan = BuildPhysicalPlan(logicalPlan!);
        if (queryContext.ModifyExecutionPlan != null)
        {
            physicalPlan = queryContext.ModifyExecutionPlan?.Invoke(physicalPlan)!;
        }

        using var step = queryContext.Profiler.Step("Execution Plan, Execution Context, Enumerate physical plan");

        await foreach (var batch in physicalPlan.ExecuteAsync(queryContext, cancellation))
        {
            step.IncrementBatch(batch.RowCount);
            yield return batch;
        }

        queryContext.Profiler.Stop();
    }
    /// <summary>
    /// Builds a logical execution plan from a SQL query
    /// </summary>
    /// <param name="sql">SQL query</param>
    /// <returns>Logical execution plan</returns>
    internal ILogicalPlan BuildLogicalPlan(string sql)
    {
        var ast = new Parser().ParseSql(sql);

        if (ast.Count > 1)
        {
            throw new InvalidOperationException("Only 1 SQL statement is supported");
        }

        var plan = ast.First() switch
        {
            Statement.Select select => LogicalExtensions.CreateQuery(select.Query, new PlannerContext(_tables)),
            _ => throw new NotImplementedException()
        };

        return new LogicalPlanOptimizer().Optimize(plan);
    }
    /// <summary>
    /// Builds a physical execution plan from a logical execution plan
    /// </summary>
    /// <param name="logicalPlan">Logical execution plan source</param>
    /// <returns>Physical execution plan</returns>
    internal static IExecutionPlan BuildPhysicalPlan(ILogicalPlan logicalPlan)
    {
        if (logicalPlan == null)
        {
            throw new InvalidOperationException("Must build a logical plan first");
        }

        return new PhysicalPlanner().CreateInitialPlan(logicalPlan);
    }
    /// <summary>
    /// Lists all tables registered with the context instance
    /// </summary>
    public IReadOnlyDictionary<string, DataTable> Tables => _tables;
}