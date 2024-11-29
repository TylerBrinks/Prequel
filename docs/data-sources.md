# Data Sources

Data sources contain the answer to the question that a SQL query asks.  Whether raw data is stored in a proprietary database, like Postgres, in a flat file like wotj comma or tab delimited values, or even an HTTP call to a web API, the process of querying the data is the same.  A data source is simply the wrapper around the data being queried.

Prequel is data source agnostic, and supports runtime-configured sources, several of which are included in the project’s source code.

When a query is executed, the engine will plan which sources physically need to be read, and which fields within the data source are relevant for the query.  Where possible, and for performance, only the relevant data is read.  RDBMS systems like Postgres natively support reading only the requested fields.  

However, more primitive data sources, such as CSV files, do not have the ability to selectively read data.  In the case of a CSV file, each line of text must be readin full (scanned) in order to account for field data before the next line can be read.

CSV files are good examples of a row-based format.  Each line of text represents one database row or record.  Formats such as Parquet files store data in columnar format, and are typically much better suited for analytics.  

## Table Scanning
Whether row-based or column-based, query plans often require reading an entire data source to produce query results.  Take the following example SQL query that counts the number of records in the data source where the color column has rows matching "blue".

```sql
SELECT count(*)
FROM [table_name]
WHERE color = 'blue'
```

Executing this query against a CSV file requires reading every line in the CSV file (possibly adjusting for a header row or empty End of File (EOF) rows).  

The same query against a Parquet file may be able to count the number of entries in a single, filtered column given data is stored in a column-oriented format, but the entire column would still require scanning.

## Data Tables
Data source interfaces with the raw data storage, while data tables sit a step above the underlying data.  A data table the product of a schema and an [execution plan](./physical-plans.md).  Typically the data table will coordinate how to read data from the data source, limit the amount of data that is read, and extract only the fields that are required by the query.

Data tables have a simple interface in the **Prequel** codebase:

```c#
public abstract IExecutionPlan Scan(List<int> projection);
```

The projection contains the indicies of the fields under query, and the scan operation reads all the relevant records from the source.