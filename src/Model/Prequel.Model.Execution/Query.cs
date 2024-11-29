using SqlParser.Ast;
using ExecutionContext = Prequel.Execution.ExecutionContext;
using Prequel.Logical.Plans;
using Prequel.Data;
using Prequel.Logical;

namespace Prequel.Model.Execution;

public class Query
{
    public required string Text { get; set; }
    public required string Name { get; set; }
    public int Index { get; set; }
    public QueryContext QueryContext { get; set; } = new();
    public int? EnforceLimit { get; set; }

    /// <summary>
    /// Executes a query against the execution context
    /// </summary>
    /// <param name="context">Execution context containing all data table references</param>
    /// <param name="cancellation">Cancellation token</param>
    /// <returns>Query execution result</returns>
    public async ValueTask<QueryExecutionResult> ExecuteQueryAsync(
        ExecutionContext context,
        CancellationToken cancellation = default!)
    {
        if (EnforceLimit.HasValue)
        {
            QueryContext.ModifyLogicalPlan = LimitLogicalPlan;
        }

        var result = new QueryExecutionResult(1, Text);

        await foreach (var batch in context.ExecuteQueryAsync(Text, QueryContext, cancellation))
        {
            result.Batches.Add(batch);
        }

        result.Timing = QueryContext.Profiler.Root;

        return result;
    }
    /// <summary>
    /// Wraps a query with a limit clause to force data read operation
    /// to return no more than the configured number of rows
    /// </summary>
    /// <param name="plan">Logical execution plan to wrap</param>
    /// <returns>ILogicalPlan instance</returns>
    private ILogicalPlan LimitLogicalPlan(ILogicalPlan plan)
    {
        var max = new Expression.LiteralValue(new Value.Number(EnforceLimit!.ToString()!));

        if (plan is Limit limit)
        {
            // Remove the existing limit (user defined) in favor of a limit controlled by the execution
            return limit.Plan.Limit(null, max);
        }

        // Query is not limited; wrap with an explicit limit
        return plan.Limit(null, max);
    }
}