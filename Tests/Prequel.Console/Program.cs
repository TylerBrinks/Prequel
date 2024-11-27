using Prequel.Console;
using Prequel.Engine.Caching;
using Prequel.Engine.IO;
using Prequel.Model.Execution;
using Prequel.Model.Execution.Files;
using Prequel.Engine.Source.Csv;
using Prequel.Engine.Caching.File;
using Query = Prequel.Model.Execution.Query;

// The example below is the long-hand way of creating and registering data
// sources.  The various implementations have convenience methods to make
// this process more concise.  This example takes the longer route to 
// demonstrate the full process of accessing data from various sources.

#region 1 Create providers to support query operations
// Table scanning and reading full data sets repeatedly can be an
// expensive operation; caching can help cut down on read repetition.
// This example uses a 'local file stream provider' to cache data read
// from a data source and saves it to a CSV file.  Subsequent scans
// will read from the CSV file instead of the data source.
//
// This is a contrived example.  A realistic example would use a cloud
// data source, for example a Postgres database, and save the cached 
// data to a storage medium like Azure Blob Storage, S3, Redis, etc...
//
// The engine accepts any type of provider that implements the ICacheProvider
// interface.  Several simple implementations are included in this project.
var localFileCacheProvider = new LocalFileCacheProvider(Directory.GetCurrentDirectory());

// The engine needs a way to access data from a data source.  This example
// creates a file stream provider and a new file connection using the provider.
// Reading CSV data is only for example. Any medium that produces data
// can act as a data source.
var csvDirectory = $"{Directory.GetCurrentDirectory().TrimEnd('\\', '/')}";

// This creates a data source that retrieves CSV data from a file named
// 'db_colors.csv' and gives the source a queryable alias `db_colors`
// which can be used for SQL queries against the connection.
var colorsConnection = new CsvFileDataSourceConnection
{
    Alias = "db_colors",
    FileStreamProvider = new LocalFileStreamProvider
    {
        FilePath = $"{csvDirectory}\\db_colors.csv",
    }
};

var primaryColorsConnection = new CsvFileDataSourceConnection
{
    Alias = "db_primary",
    FileStreamProvider = new LocalFileStreamProvider
    {
        FilePath = $"{csvDirectory}\\db_primary.csv",
    }
};


// Additional data sources containing employee sample data connections
var employeesConnection = new CsvFileDataSourceConnection
{
    Alias = "db_employees",
    FileStreamProvider = new LocalFileStreamProvider
    {
        FilePath = $"{csvDirectory}\\db_employees.csv",
    }
};
var departmentsConnection = new CsvFileDataSourceConnection
{
    Alias = "db_departments",
    FileStreamProvider = new LocalFileStreamProvider
    {
        FilePath = $"{csvDirectory}\\db_departments.csv",
    }
};
#endregion

#region 3 Read the CSV file's schema
// CSV schemas are tricky; data has no constraints.  Reading the schema
// is a best guess process based on the first N rows of data. In the
// case of a RDBMS, the schemas are well known before a query executes.
// 
// A table will be opened from a file stream provider.  The provider can be physical or
// virtual (cloud), but in this case, it is a local file stream provider reading from 
// the CSV files included in this project.
var colorsCsvTable = await CsvDataTable.FromStreamAsync("colorsTable", colorsConnection.FileStreamProvider.GetFileStream());
// A data table maps field positions and types.  The CSV schema is created from 
// the output of the CSV table schema stream read.
var colorsSchema = DataTableSchema.FromSchema(colorsCsvTable.Schema!);

var primaryColorsCsvTable = await CsvDataTable.FromStreamAsync("primaryColorsTable", colorsConnection.FileStreamProvider.GetFileStream());
// A data table maps field positions and types.  The CSV schema is created from 
// the output of the CSV table schema stream read.
var primaryColorsSchema = DataTableSchema.FromSchema(primaryColorsCsvTable.Schema!);


// Employee sample data schemas
var employeesCsvTable = await CsvDataTable.FromStreamAsync("employeesTable", employeesConnection.FileStreamProvider.GetFileStream());
var employeesSchema = DataTableSchema.FromSchema(employeesCsvTable.Schema!);
var departmentsCsvTable = await CsvDataTable.FromStreamAsync("departmentsTable", departmentsConnection.FileStreamProvider.GetFileStream());
var departmentsSchema = DataTableSchema.FromSchema(departmentsCsvTable.Schema!);
#endregion

#region 4 Crate a table reference based on the CSV table
// This step builds a queryable new table called 'colors' by querying
// the previously created CSV table and running a new SQL query
// on the data.  The output is stored in a new table reference
// that future SQL queries can interact with.
var colorsTableReference = new FileDataTableReference
{
    Name = "colors",
    Schema = colorsSchema,
    Connection = colorsConnection, // This connection refers to `db_colors` created above
    Query = "select * from db_colors",
    // Caching options demonstrate multiple reads with only a single
    // scan of the actual CSV file.  Additional scans will read from
    // the cached object (also a CSV file).  This could be an
    // in-memory, distributed cache, etc...
    //CacheOptions = new CacheOptions 
    //{
    //    UseDurableCache = false,
    //    DurableCacheKey = "cached_colors.csv", // Where the cached data will be written/read
    //    GetDataWriter = CreateWriter, // See below
    //    GetDataReader = CreateReader, // See below
    //    CacheProvider = localFileCacheProvider // See above
    //    //UseMemoryCache = true
    //}
};
var primaryColorsTableReference = new FileDataTableReference
{
    Name = "primarycolors",
    Schema = primaryColorsSchema,
    Connection = primaryColorsConnection, // This connection refers to `db_colors` created above
    Query = "select * from db_primary",
};
// Additional sample data tables containing employee data
var employeesTableReference = new FileDataTableReference
{
    Name = "employees",
    Schema = employeesSchema,
    Connection = employeesConnection, // This connection refers to `db_colors` created above
    Query = "select * from db_employees"
};
var departmentsTableReference = new FileDataTableReference
{
    Name = "departments",
    Schema = departmentsSchema,
    Connection = departmentsConnection, // This connection refers to `db_colors` created above
    Query = "select * from db_departments"
};
#endregion

#region 5 Create a query execution
// Executions orchestrate table registration and query execution.  
// It is the main entry point for executing SQL queries against
// the data sources.
var execution = new Execution();
#endregion

#region 6 Add the source tables
// Add the source tables as a queryable references to the execution
execution.ConnectionTables.Add(colorsTableReference);
execution.ConnectionTables.Add(primaryColorsTableReference);

//execution.ConnectionTables.Add(primaryColorsTableReference);
execution.ConnectionTables.Add(employeesTableReference);
execution.ConnectionTables.Add(departmentsTableReference);
#endregion

#region 7 Write queries against the tables
// The most basic possible query has no underlying data source and no schema.
// The select simply returns the scalar value with the inferred data type.
//
// Concepts in this query: basic SQP parsing and execution
execution.AddQuery(new Query
{
    Name = "scalar_value",
    Text = """
           SELECT 1
           """
});

// Slightly more complex, scalar values can be calculated using basic
// SQL-style arithmetic.  The query returns a table with three columns
// with the calculated values.
//
// The column names will represent the math operations.  Try giving
// each operation an alias (e.g. `1 + 2 as sum`) to see the difference.
// The commented line won't execute, but it demonstrates how to alias
// a field in the output schema.
//
// Concepts in this query: basic SQP operations on scalar values
execution.AddQuery(new Query
{
    Name = "calculated_values",
    Text = """
           SELECT 1 + 2, 3 * 5, 100 / 20
           
           -- SELECT 1 + 2 as sum, 3 * 5, 100 / 20
           """
});

// Using the tables created above, the next few queries run against the table
// references.  Each query will build a new table from the output data,
// and each new table can be queried by subsequent queries.
//
// The first data source query is a simple `select *` from the `colors` table.
// The resulting data is stored as a table with a new name, `all_colors.`
// After the query executes, `all_colors` can be queried as a data source
//
// Concepts in this query: Scan a data source, project all schema columns
execution.AddQuery(new Query
{
    Name = "all_colors",
    Text = """
           SELECT
             *
           FROM colors
           """
});

// This query also queries the newly created `all_colors` table, but filters the results
// to primary colors only. The query creates a new table named `primary_colors`
// that can be queried, joined, etc.
//
// This query demonstrates a simple filter operation against the table created by the
// previous query.  This could query directly against the `colors` table as well
// and yield the same results.  Which table to query may depend on caching needs, etc.
//
// Concepts in this query: Scan with filtering
execution.AddQuery(new Query
{
    Name = "primary_colors",
    Text = """
           SELECT
             *
           FROM colors 
           WHERE 
             c2 in ('red', 'yellow', 'blue')
           """
});

// This query reads from the `colors` table and (LEFT) joins the `primary_colors` table
// to exclude primary colors from the results.  This demonstrates a join operation
// with a simple filter operation.
// 
// Concepts in this query: projections, aliases, multiple sources, filtering, joins.
execution.AddQuery(new Query
{
    Name = "secondary_colors",
    Text = """
           SELECT
             ac.c2 as secondary_color_names
           FROM colors ac
           LEFT JOIN primary_colors pc
           ON pc.c2 = ac.c2
           where pc.c1 <= 0
           """
});

// Aggregations work as they would in any SQL engine.  Data is read and accumulated by a
// set of 'accumulators' that perform arithmetic operations on the result set.
//
// All data produced by the queries is required for aggregations to calculate their values,
// but the data can be run through multiple aggregations in a single pass.  In this 
// example the query uses max, min, average, and median
//
// Concepts in this query: Scan with aggregations
execution.AddQuery(new Query
{
    Name = "aggregations",
    Text = """
           SELECT
             max(c1), min(c3), avg(c1), median(c1)
           FROM colors
           """
});


// The next set of queries use a different set of data sources showing multiple operations
// executing across the data sources. In a typical RDBMS, these would be tables in the same
// database, but in this example, they are separate CSV files.  The engine can handle
// reading data from any data source meaning you can read data across different databases
// and even different data stores.  For example the same query would work against a Postgres
// joined to a CSV file.
//
// This query joins employee data and department data sharing the same manager, sorted
// by manager ID.  The query also makes use of aliased column names to show mapping
// of the underlying schema(s) to the output schema.
//
// Concepts in this query: Operations against multiple sources (think of them as tables)
execution.AddQuery(new Query
{
    Name = "workplace",
    Text = """
           SELECT 
             m.employee_id as ManagerId, 
             e.employee_id as EmpId, 
             m.first_name ManagerFN, 
             m.last_name ManagerLN,
             e.first_name EmployeeFN, 
             e.last_name EmployeeLN
           FROM employees m
           JOIN employees e
           ON m.employee_id = e.manager_id
           WHERE e.manager_id in (100, 101)
           ORDER BY e.manager_id
           """
});

// This query joins employee data with department data, filtering the results to only
// those employees in the 'Sales' department.  The query demonstrates a LEFT JOIN
// to exclude data from the result set.
//
// Concepts in this query: Join targets.  Left/Right/Full/Inner/Outer
execution.AddQuery(new Query
{
    Name = "department_left",
    Text = """
           SELECT 
             e.employee_id, 
             e.first_name, 
             e.last_name
           FROM employees e
           LEFT JOIN departments d
           ON e.department_id = d.department_id
           WHERE d.department_name = 'Sales'
           """
});

#endregion

var result = await execution.ExecuteAllAsync();

Results.Display(result);
return;

// Creates a CSV writer using the local file cache provider
IDataWriter CreateWriter(CacheOptions cache) => new CsvDataWriter(
    localFileCacheProvider.GetFileStream(cache.DurableCacheKey!));

// Creates a CSV reader using the local file cache provider
IDataSourceReader CreateReader(CacheOptions cache) => new CsvDataSourceReader(
    localFileCacheProvider.GetFileStream(cache.DurableCacheKey!));
