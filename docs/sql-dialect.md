# SQL Dialects

Each database engine has its own SQL dialect.  Different databases understand the general grammatical flow of Structured Query Language, but, like spoken languages, there are many unique dialects.

Take Postgres and Microsoft SQL server as examples.

Microsoft uses this syntax to limit records to 10 results

```sql
SELECT TOP 10 name FROM some_table
```

Postgres uses similar, but slightly different syntax to generate the same output

```sql
SELECT name FROM some_table LIMIT 10
```

You don't need to be fluent in either dialect to intuit the similarity.  Both dialects begin with a SELECT statement, scan the some_table, project the name column, and, with subtle differences, limit the record count to 10

In this project in particular, the most basic grammar is used.  The grammar is closest to an [ANSI SQL standard](https://en.wikipedia.org/wiki/SQL)

Should this project grow, it would make sense either to adopt the grammar of an existing engine, or, more logically, create a unique grammar.  If, for example, the code were extended to include custom .NET code that interprets custom functions to execute in C#, the grammar would then be unique to the implementation.  

The [SQL Parser project](https://github.com/TylerBrinks/SqlParser-cs) supports injecting custom grammars, so generating a custom grammar is a simple task. 
