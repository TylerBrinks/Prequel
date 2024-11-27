# Query Planning

Queries in Prequel are build and executed using an [ExecutionContext class](../Engine/Prequel.Engine.Core/Execution/ExecutionContext.cs).  This class is responsible for building both [logical](logical-plans.md) and [physical](physical-plans.md) execution plans.

## Building Logical Plans
Before a logical plan can be composed, the user's query must be parsed.  Parsing SQL queries is a complex topic on its own, and beyond the scope of this tutorial.  This project uses the SqlParser C# library to parse SQL queries into an abstract syntax tree.  Once the query has been parsed into its individual components, a logical plan can be composed.

At the time of this writing, the project only creates plans for `SELECT` queries.  Future work would add additional operations (`INSERT`, `DROP`, `UPDATE`, and so on), but for querying purposes the project onlys reads data from data stores.

As such, the `ExecutionContext` limits execution to queries with a `SELECT` root element

```c#
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
```

The logical plan is composed using the static CreateQuery method on the `LogicalExtensions` class.  

Careful observers will notice the `CreateQuery` method is not only called externally, but internally as well.  This method may be called recursively to handle subqueries.  

For example, this contrived query would call the CreateQuery recursively

```sql
SELECT 
    name
FROM
	(SELECT * FROM some_table)
```

The process of creating logical queries is a linear process.  The first step in composition is identifying the table relationships contained in the query.  This is another feature handled by the [SQL Parser library](https://github.com/TylerBrinks/SqlParser-cs), however the implementation is handled in the Prequel project.  The SQL syntax tree is traversed by the parser using a `RelationVisitor` class.  Each table identified in the syntax tree is instantiated as a `TableReference` object.  A `TableReference` is a lightweight object holding the Name and Alias for the table.  Tables can be aliased in SQL, but when not aliased they assume the table's name

```c#
internal class RelationVisitor : Visitor
{
    public override ControlFlow PostVisitTableFactor(TableFactor relation)
    {
        if (relation is not TableFactor.Table table) { return ControlFlow.Continue; }

        string? alias = null;

        if (table.Alias != null)
        {
            alias = table.Alias.Name;
        }

        var reference = new TableReference(table.Name, alias);
        if (!TableReferences.Contains(reference))
        {
            TableReferences.Add(reference);
        }

        return ControlFlow.Continue;
    }


    internal List<TableReference> TableReferences { get; } = new();
}
```

After the tables have been identified, the logical planner parses the syntax tree building logical elements in the following order:
1. **Joined Tables** - Drives which tables are scanned for projected values and builds join relations when tables are 
2. **Selection** - Builds logical expressions from the `SELECT` plan.  In this step, the schema is normalized.  Any filtering expressions become a hash set and the step is wrapped in a filter operation (which may or may not exist)
3. **Projection** - Turns columns, aliases, and wildcards into the fields needed for scanning tables.  This step enumerates all the select items in the projection and decides if the select item can be simply looked up (named), referenced (unnamed), or potentially contains multiple fields that need to be resolved. The last case is the behavior that emerges when wildcard is used `SELECT * FROM table_name`
4. Schema concatenation - merges schemas from the tables and projections into the output schema.  Merging must yield a distinctly unique list of fields.
5. Having - Groups results when aggregated values are bundled with a HAVING statement.  While seemingly complex, this step simply resolves an aliased field and rewrites the query with that alias

    `SELECT column_a, MAX(c2) AS abc FROM tbl GROUP BY c1 HAVING abc > 10;`

    is rewritten

    `SELECT c1, MAX(c2) AS abc FROM tbl GROUP BY c1 HAVING MAX(c2) > 10;`

6. Aggregations - Creates the accumulators that will run aggregate functions like min, avg, and so on.  This is another textbook example of recursion.  The code here traverses the expression tree looking for any expression that is an AggregateFunction
7. Grouping - Builds `GROUP BY` expressions.  This step is one of the most complex in terms of the implementation.  The step must resolve grouping columns and then, for each column in the grouping, resolve the column's aliases, position, expression, and then normalize the expression.  This is achieved by wrapping an initial logical expression with a series of parent expressions that, together, build an expression chain. Take a look at the `LogicalExtensions.FindGroupByExpressions` method to see the nesting logic.
8. Final Projection - Builds the final projected values from all previous steps.  This step calls the same function that was used in step 3 above.  The difference here is that the plan has grown and changed shape significantly.  The output here will represent the fields the user is requesting in their `SELECT` statement after all other operations have completed.
9. Distinct - The final, optional, step.  While this is simply a wrapper class, the physical implementation depends on the presence or absence of this step to filter values uniquely

These are the discrete steps required to build the logical plan.  These steps are enough to logically represent what data should be read from the various data sources.  At this point, however, the query is likely inefficient and redundant.  

While it would be possible to execute the plan in this state, optimizing the plan will produce far better results.  Optimization is a topic unto itself.   [Continue here for the Query Optimization process](query-optimization.md).

