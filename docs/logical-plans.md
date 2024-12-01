# LogicaL Plans

Query planning takes place in two phases; logical planning and [physical planning](physical-plans.md).  The logical planning phase builds relationship representations against a schema.  Logical plans may have inputs, though not required.

During the logical plan phase, the query engine will construct aggregations (min, max, etc...), joins, filters, limits, ordering, projections, table scans, etc...

## Logical Expressions
Logical plans are built from a series of logical expressions.  Expressions take on numerous forms, and represent the runtime evaluation of all elements in a query.  

Expressions may be arithmetic, equality, conditional, and so on.

### Examples:
**Literal Expressions**
-  `"Monday"`
-  `987.55`

**Arithmetic Expressions**
-  `[field_name] * 10`
-  `[subtotal] + [tax]`

**Boolean Expressions**
-  `Age >= 65`
- `Item_count = 100`

**Aggregate Expressions**
-  `min(stock_price)`
-  `avg(participant_count)`

These examples are a subset of the numerious Expression types.  Expressions can can be combined and nested to create rich expression trees.  For example, this expression combines an aggregation with a math operation.

```sql
SELECT
    avg(cart_total / item_price)
FROM 
    shopping_orders
```

## Outputs
Unlike a physical column in a data store, expressions are not always named.  Naming expressions becomes critical to reference not only the value, but the underlying data type.  For example, a Boolean expression will produce a boolean output; either true or false.  Conversely, an aggregation or arithmetic operation may produce an integer or floating point value.  Knowing the types an expression will produce is critical to logical planning.  In the example above, `item_count = 100` is only valid if `item_count` is a value that can be compared numerically meaning multiple data types can act as the input (integer vs. floating point), but the output will always be the same.

## Inputs
Expressions will typically produce an output, but some expressions also take inputs.  In the example above, aggregate functions like `min` and `avg` are used to calculate a value.  They depend on input which must follow similar constraints.  In the case of [Posgres, the following types are valid for the `avg` function](https://www.postgresql.org/docs/17/functions-aggregate.html)
`smallint, integer, bigint, real, double precision, numeric, or interval`

## Expression Literals
Some expression values are not derived from the database itself, but from user input.  In the examples above, the values `"Monday"` and `987.55` are expressions that carry a value and a type, but are external inputs to the query engine.


# Logical Planning
The first step in logical planning is to gather the expressions, some of which are outlined above.  Once expressions are cataloged, the logical planning step can begin to compose steps required to satisfy the expression tree.

## Projections
Projections are among the most complex but necessary part of query planning.  In the projection step, the engine evaluates which fields, calculations, and aggregations are required to produce the desired output.  Take the following SQL for example.

```sql
SELECT
    name, 
    cart_total * tax_rate as total_cost
FROM 
    shopping_orders
```

In this case, one projection is simple; `name` is simply an alias to a column in the orders table.  The `total_cost` alias refers to a calculated result from the product `cart_total` and `tax_rate`.  This expression will need to run against 2 columns in the orders table with an arithmetic operation and yield a single value per row.  Expressions range in complexity, and it's the job of the projection logic to sort out the and walk the tree structure for the values being queried.

## Filtering (a.k.a. Selection)
Projections limit which columns are involved in a query.  Filtering limits which rows are involved.  The same query above could be filtered to include only data where the sale occurred on a weekend.  Filters are represented with a WHERE clause

```sql
SELECT
    name, 
    cart_total * tax_rate as total_cost 
FROM 
    shopping_orders
WHERE 
    sale_date in ("Monday", "Tuesday")
```

## Ordering
Projections control columns, filtering controls rows, and finally ordering controls the sequence of the data being scanned.  Ordering restructures the data where the most relevant results are output in the desired order.  Without ordering data, query results may be unexpected or simply reflect the order in which data was added to the system.  By ordering data, the query engine will output order predictable by using another expression evaluated against the data set.  In this case, an order expression has been added to show the most recent orders first.

```sql
SELECT 
    name, 
    cart_total * tax_rate as total_cost 
FROM 
    shopping_orders
WHERE 
    sale_date in ("Monday", "Tuesday")
ORDER by date_received DESC
```

## Aggregation
Aggregate values are more complex in nature than the other logical steps.  Many expressions can operate on a row-by-row basis.  For example, the output of `cart_total * tax_rate` only depends on data within the row itself.

Aggregations, on the other hand, typically depend on the subset of data.  It stands to reason that taking a sum or average of values in a first needs all the data in the query.  It is for this reason that aggregations cannot be combined with raw column values without the support of a `GROUP BY` expression.  

```sql
SELECT 
    avg(cart_total * tax_rate) as average_spend
FROM 
    shopping_orders
WHERE 
    sale_date in ("Monday", "Tuesday")
```

## Scanning
Scanning table data is the final step in logical planning.  Scanning is the step during which data is physically read from the data source.  Other steps in the logical plan can act as inputs or outputs, but scanning is always the final step in execution.  When a table is scanned, data is read from the source until the previous filtering, ordering, aggregation, and projecting have been satisfied.

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