# Query Optimization

Hopefully you've familiarized yourself with [logical plans](logical-plans.md) and [query planning](query-planning.md) in general. 

Technically, a logical plan can be executed without being optimized.  However, it's unlikely the user has composed the most efficient query possible.  The order in which a user is sorting, filtering, joining, and so on is likely suboptimal.  Even if a query were optimized, the query engine needs to take some liberties like swapping `DISTINCT` with aggregation (`DISTINCT` is actually a shortcut for an aggregation).

If you have access to a database engine like **Postgres**, **MS Sql**, **MySql**, try composing a simple `SELECT` query.  Now add the `EXPLAIN` keyword before the `SELECT` and run it.  You should see the database engine's plan for running your query.  Notice that the execution may take steps you didn't expect, or omit steps you expected.

```sql
EXPLAIN SELECT 
    s.id, c.id
FROM
    sales s
JOIN 
    customers c
ON 
    s.customer_id = c.id
WHERE 
    customer_zip = '12345'
LIMIT
    100;
```

```text
Limit  (cost=0.28..581.63 rows=1 width=32)
  ->  Nested Loop  (cost=0.28..581.63 rows=1 width=32)
        ->  Seq Scan on sales s  (cost=0.00..577.33 rows=1 width=32)
              Filter: (customer_zip  = '12345'::text)
        ->  Index Only Scan using customer_id on customers c  (cost=0.28..4.30 rows=1 width=16)
              Index Cond: (id = s.customer_id)
```

In this contrived example, **Postgres** intends to use a **Nested Loop** join, a filtered sequential scan, and an index scan over a condition.  The engine is making optimizations, like using an index, on your behalf.

## Optimization Rules
Prequel uses a simple set of rules to optimize queries.  You can find the rules in the `LogicalPlanOptimizer` class.  In a full query engine implementation, there would be far more rules to squeeze every drop of performance out of the logical plan.  For this project, a simple set of 5 fuels is used.

```c#
private static readonly List<ILogicalPlanOptimizationRule> Rules =
[
    new ReplaceDistinctWithAggregateRule(),
    new ScalarSubqueryToJoinRule(),
    new ExtractEquijoinPredicateRule(),
    new PushDownProjectionRule(),
    new EliminateProjectionRule(),
];
```

The optimizer runs the logical plan through each rule.  Each rule, in turn, will modify the plan in some way and use it as the input for the next rule.  The result is an optimized plan that is ready to execute as a physical plan.

## Replace Distinct with Aggregate
It turns out `DISTINCT` is a SQL shortcut.  Underneath, `DISTINCT` is actually an aggregate function.  This rule starts by finding all the fields involved in the query.  If a wildcard (*) is encountered, the schema ise expanded into the full list of fields.  A grouping expression is created with the expanded list of field and used as the basis for a simple aggregate.

This rule is helpful to simplify queries.  For example, these two queries produce identical results:

```sql
SELECT DISTINCT id FROM some_table;
```

```sql
SELECT id FROM some_table GROUP BY id;
```

The benefit here is that we can reuse the existing GROUP BY aggregation expression code instead of building a new execution for a single SQL keyword.

## Scalar Subquery Joins
This is another example of taking a user's input and finding an optimal way to execute the code.  Oftentimes subqueries are suboptimal.  Left unoptimized, they may result in multiple table scans or retrieving too much data.  Take this simple SQL query as an example

```sql
SELECT 
    * 
FROM 
    table_abc abc 
WHERE abc.column_name 
IN 
    (
        SELECT 
            xyz.other_column_name 
        FROM 
            table_xyz
    )
```

It looks as though we would need to read data from the data store to satisfy the inner query, and then run a second, outer query filtered on the first data set.  That would be an expensive operation.

Instead, we can rewrite the query as a join
```sql
SELECT 
    * 
FROM 
    table_abc abc 
JOIN 
    table_xyz xyz 
ON 
    xyz.other_column_name = abc.column_name
```

These two queries produce the same results, but the optimized version only requires a single read operation.

## Equijoin Predicates
An equijoin predicate is predicate that compares records from two tables using an equality operator `('=')`

This rule optimizes plan execution by converting a **JOIN ON** filter on **AND** clauses
and reformats them. The reformed join is built with an optimized **ON** clause and filter expressions. Wherever possible, binary equality expressions are split and transformed into join keys

## Projection Pushdown
Projection Pushdown is widely used across virtually all database engines.  The rule involves "pushing" column projections to the data source so only the columns required for a query are read from storage.  Rather than reading all columns and then selecting the necessary ones, this rule reduces how much data is read from disk.  Likewise projection pushdown can improve performance as they reduce how much memory is required to read data.  Only the required columns occupy memory. 

The rule works by collecting and evaluating all expressions for a given logical step and either replacing or removing unused plans.

Here's a sample query to demonstrate what's happening within the optimization.

```sql
SELECT 
    address, state, zip
FROM 
    orders
WHERE 
    city = ‘Rome'
```

The resulting logical plan would be something like

```
Projection Logical Step: address, state
  Filter Logical Step: city = ‘Rome'
    Scan Logical Step; Fields: *
```

Notice the Scan step will read all columns because it has no awareness of data needed in the previous steps.

The optimizer will "push" the fields down through each plan to optimize the scan step.  The optimized version would look like this

```
Projection Logical Step: address, state
  Filter Logical Step: city = ‘Rome'
    Scan Logical Step; Fields: city, address, state
````

## Eliminate Projections
This rule checks if a projection can be removed by comparing the list of projection fields against the step's schema fields.   Plans with identical schemas and fields can be removed.

```c#
switch (plan)
{
    case Projection projection:
        var childPlan = projection.Plan;
        switch (childPlan)
        {
            case CrossJoin:
            case Filter:
            case Join:
            case Sort:
            case SubqueryAlias:
            case TableScan:
            case Union:
                return CanEliminate(projection, childPlan.Schema) ? childPlan : plan;

            default:
                return plan.Schema.Equals(childPlan.Schema) ? childPlan : null;
        }

    default:
        return null;
}
```

## Additional Optimization?
These rules are far from comprehensive.  They give a good picture of how to optimize a query, but there are many more rules that could be employed.  For example, a user might have a redundant filter that should be removed

```sql
SELECT 
    color
FROM 
    table_name
WHERE
    color = 'red'
OR 
    color = 'red'
OR 
    color = 'red'
```

The extra comparison is redundant and would incur real compute resource if left unchecked.  

These additional rules have not been implemented (yet) in the project.  However enough rules exists for the project to run effectively.

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