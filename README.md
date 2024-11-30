# Prequel - C# SQL Query Engine

[![NuGet Status](https://img.shields.io/nuget/v/Prequel.QueryEngine.svg)](https://www.nuget.org/packages/Prequel.QueryEngine/)  [![CD](https://github.com/TylerBrinks/Prequel/actions/workflows/cd.yml/badge.svg)](https://github.com/TylerBrinks/Prequel/actions/workflows/cd.yml) 

[Prequel](/docs/index.md) is a SQL query engine built in C#.  The goal of this project is to demonstrate how query engines are built, and to bring a query engine to the .NET community.

The project contains a [step-by-step overview](/docs/index.md) explaining how database query engines are built.  The project is also available as a NuGet package and can be integrated into your project to support querying virtually any source where data can be collected. 

The project contains several example data sources including CSV, Avro, Parquet, JSON, MS Sql, etc..  Adding new data sources is trivial and allows any data source to be queried with SQL, even querying data across disparate sources (e.g. join JSON to CSV or tables in a RDBMS instance).

## Run the Examples
Open the source code in your IDE of choice.  Set the Prequel.Console as the startup project and/or compile the project and run.  

- **Visual Studio** - Set **Prequel.Console** as the startup project and run the project
- **VS Code** - Use the included launch profile, run from the "RUN AND DEBUG" window
- **CLI** - Open a terminal to the projects root

    ```bash
    cd src/Tests/Prequel.Console
    dotnet run
    ```
---

## NuGet
```bash
dotnet add package Prequel.QueryEngine
```

## What is a Query Engine?
A query engine is software that interprets a query (Structured Query Language) and interacts with a data source to manipulate the underlying data.  Take a look at [the full project overview](/docs/index.md) for details on how every part of a query engine works.


## ✨ Main Features

- **Simple 🤲:** Query engines are not simple by nature.  However this project uses the lowest common denominator approach to standardize unrelated data sources and data types (e.g. JSON vs CSV).
- **Extensible 🧩:** Plug in any data source.  All runtime data is queried from your data source under your control.
- **Caching 💵:** In-memory and durable caching options make it simple to control how and when your data source is read.
- **Open Source 📖:** All code and features are available, transparent, and free to use.
- **Performant 🏎️:** Queries are optimized before execution on a minimum-data-needed approach to reading data.  Only the data required is read from data sources, even across unrelated sources.  Profiling is built into the engine.

## SQL Support
 - [X] `SELECT` statements
 - [X] Wildcards
 - [X] `JOIN` - inner/right/left/full/cross
 - [X] `GROUP BY`
 - [X] `HAVING`
 - [X] `UNION`
 - [X] `LIMIT`
 - [X] `LIKE`, `ILIKE` comparisions
 - [X] `IN` list comparisions
 - [X] Subqueries
 - [X] Aggregations - `min`/`max`/`stddev`/etc...
 - [X] Math pperations
 - [X] `CAST` functions
 - [ ] `EXCLUDE`/`EXCEPT` - Not yet implemented
 - [ ] `EXPLAIN` - Not yet implemented
 - [ ] Insert, Update, Drop operations

## Motivation
This project contains a [full overview](/docs/index.md) of how query engines are built.  Part of the goal with this project is to demonstrate the mechanics of a query engine.  Chances are you use some form of database, BI tool, or ETL process as part of your daily development.  While the code here is fully functional, it’s also academic in the sense that it gives a full picture of how data is gathered, aggregated, sorted, filtered, and so on, by your favorite data engine.  Whether Postgres, MySql, BigQuery, Snowflake, or any of the dozens of database systems or cloud-hosted data platforms.

## Contribution
Contributions are what make open-source work.  We greatly appreciate any contributions.  Thank you for being a part of our community! 🥰

## Sponsorship
If you find Prequel useful, or the tutorial helpful, please consider sponsoring us.  Your support will help the project continue to grow.

→ Your Name Here ← if you become a sponsor 👏.


<a href="https://buymeacoffee.com/tylerbrinks" target="_blank"><img src="https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png" alt="Buy Me A Coffee" style="height: 41px !important;width: 174px !important;box-shadow: 0px 3px 2px 0px rgba(190, 190, 190, 0.5) !important;" ></a>

## Other Projects
[C# SqlParser - SQL Grammar Parser](https://github.com/TylerBrinks/SqlParser-cs)

## License
[MIT](https://github.com/mingrammer/diagrams/blob/master/LICENSE)