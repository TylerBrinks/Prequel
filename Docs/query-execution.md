# Query Execution

## Run the Test Project
Open the source code in your IDE of choice.  Set the Prequel.Console as the startup project and/or compile the project and run.  

### Visual Studio
- Set Prequel.Console as the startup project and run the project

## VS Code
- Use the included launch profile, run from the "RUN AND DEBUG" window

## CLI
- Open a terminal to the projects root

    ```bash
    cd Tests/Prequel.Console
    dotnet run
    ```

All 3 options will produce console output from a series of queries against CSV files.  See the [Program.cs](../Tests/Prequel.Console/Program.cs) for details.

## Working with the Query Engine

This project takes a simple approach to executing queries.  However, from the example, you can build complex query orchestrations.  

Included in the project is an `Execution` class.  The class is merely a wrapper around tables and queries.  Tables are registered as "data sources" that can be queried with SQL syntax.  The results of each query are then held in memory as a new data source that can likewise be queried.

## Registering a Data Source
The **Prequel.Console** project has examples of the longhand way to register a table.  There are also extension methods to simplify the process.  Demonstrated below is the complete code to create and query a data source

## Data Source Connection
This block of code creates a connection to one of the CSV files included in the project.

```c#
var csvDirectory = $"{Directory.GetCurrentDirectory().TrimEnd('\\', '/')}";
var colorsConnection = new CsvFileDataSourceConnection
{
    Alias = "db_colors",
    FileStreamProvider = new LocalFileStreamProvider
    {
        FilePath = $"{csvDirectory}\\db_colors.csv",
    }
};
```

The query engine needs the data source's schema in order to execute queries.  In a proper RDBMS you have access to schema metadata through the `public` or `master` schemas.  In this case, the CSV implementation will read a sample of data from CSV file and infer the schema based on the data it encounters.

```c#
var colorsCsvTable = await CsvDataTable.FromStreamAsync("colorsTable", colorsConnection.FileStreamProvider.GetFileStream());

var colorsSchema = DataTableSchema.FromSchema(colorsCsvTable.Schema!);
```

A "table reference" maps the table's schema, means to connect and read data, as well as "pre-query" if we wanted to filter the data directly from the source before our queries run.

```c#
var colorsTableReference = new FileDataTableReference
{
    Name = "colors",
    Schema = colorsSchema,
    Connection = colorsConnection,
    Query = "select * from db_colors"
};
```

Finally, execute the query.

```c#
var execution = new Execution();
execution.ConnectionTables.Add(colorsTableReference);

execution.AddQuery(new Query
{
    Name = "all_colors",
    Text = """
           SELECT
             *
           FROM colors
           """
});

var result = await execution.ExecuteAllAsync();

Results.Display(result);
```

Try running the `Prequel.Console` project.  The same code above, ase well as several additional examples, will output results to your console.

## Query Context
Each `ExecuteAsync` step in the physical execution plan takes a `QueryContext` as an input parameter.

The query context provides the desired batch size and maximum results.  Batch size controls how many results are added to a record batch while reading from the data source.  A batch is yielded once it has sufficient data, or the end of the source's data is reached.  Batches may contain less data that the batch size as they flow through the various steps (e.g. filtering out unneeded data).

The `MaxResults` property was added for demonstration purposes.  It shows how, despite a SQL query's syntax (say `"SELECT ... LIMIT 1,000,000,000"`), you can control the data flow independent of the query (e.g. `QueryContext.MaxResults = 100`).  In other words, rather than trying to rewrite a user's query to enforce some behavior, the behavior can be enforced within the query execution chain.

## Profiling
The query context also contains a basic profiler.  The profiler gives a friendly name to each step as it measures timing and record count for each step.  The overall timings are displayed in the console output for the example queries in the `Prequel.Console` project
