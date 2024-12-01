# Async Execution
One of the decisions added to the project early on was to make heavy use of C# and .NET's asynchronous `foreach` loops.  The execution engine (as outlined above) will create a physical plan containing several steps for reading, filtering, and so on.  Each step is wrapped in an `async foreach` loop, each iteration from which a batch is produced.  

## Async Enumeration
The benefit with the async enumeration is that only the required data needs to be read from a source database.  Querying can stop once any filtering, limiting, or joining constraints are satisfied.

Each physical execution step implements the same interface for execution:

```c#
public interface IExecutionPlan
{
    Schema Schema { get; }
    IAsyncEnumerable<RecordBatch> ExecuteAsync(QueryContext queryContext, CancellationToken cancellation = default!);
}
```

The implementations all use cancellable cancellation tokens through the whole execution chain.

```c#
public async IAsyncEnumerable<RecordBatch> ExecuteAsync(
    QueryContext queryContext,
    [EnumeratorCancellation] CancellationToken cancellation = default!)
```

---

## Continue Reading

1.   [Data Types](data-types.md)
2.   [Data Sources](data-sources.md)
3.   [Logical Plans](logical-plans.md)
4.   [Physical Plans](physical-plans.md)
5.   [Query Planning](query-planning.md)
6.   [Query Optimization](query-optimization.md)
7.   [Query Execution](query-execution.md)
8.   [Async Enumeration](async-execution.md)
9.   [Rows vs. Columns](rows-and-columns.md)
10.  [SQL Dialect](sql-dialect.md)