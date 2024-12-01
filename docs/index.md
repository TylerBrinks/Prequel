# Prequel 
Prequel is a query engine capable of querying any data source using common SQL syntax.

---

This project was originally inspired by Apache Data Fusion.  The intent of this project is to demonstrate how query engines work while bringing a user-friendly SQL query engine to the .NET community

The code in this project uses the [SqlParser C# SQL grammar parser](https://github.com/TylerBrinks/SqlParser-cs) project to parse SQL statements and, in turn, transform the statements into a series of executable read operations against any number of backing data sources.  

This project’s documentation outlines every step involved in building a generic query engine.

## Table of Contents

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