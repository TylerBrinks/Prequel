using Prequel.Engine.Caching;
using Prequel.Data;
using Prequel.Engine.IO;
using ExecutionContext = Prequel.Execution.ExecutionContext;

namespace Prequel.Engine.Source.Csv;

public static class ExecutionContextExtensions
{
    /// <summary>
    /// Registers a CSV file as a table data source using provided CSV read context
    /// </summary>
    /// <param name="context">Execution context instance</param>
    /// <param name="tableName">Name used when querying the data source</param>
    /// <param name="fileStream">File source instance</param>
    /// <param name="queryContext">Options for controlling the query output</param>
    /// <param name="cacheOptions">Caching options</param>
    /// <param name="readOptions">CSV read context</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Awaitable task</returns>
    public static async Task RegisterCsvFileAsync(
        this ExecutionContext context,
        string tableName,
        IFileStream fileStream,
        QueryContext? queryContext = null,
        CacheOptions? cacheOptions = null,
        CsvReadOptions? readOptions = null,
        CancellationToken cancellation = default!)
    {
        var table = await CsvDataTable.FromStreamAsync(tableName, fileStream, queryContext ?? new(), cacheOptions ?? new(), readOptions, cancellation);
        context.RegisterDataTable(table);
    }

    /// <summary>
    /// Registers all files in a directory as table data sources using provided CSV read context
    /// </summary>
    /// <param name="context">Execution context instance</param>
    /// <param name="tableName">Name used when querying the data source</param>
    /// <param name="directory">Directory containing files that can be enumerated</param>
    /// <param name="queryContext">Options for controlling the query output</param>
    /// <param name="cacheOptions">Caching options</param>
    /// <param name="readOptions">CSV read context</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Awaitable task</returns>
    public static async Task RegisterCsvDirectoryAsync(
        this ExecutionContext context,
        string tableName,
        IDirectory directory,
        QueryContext? queryContext = null,
        CacheOptions? cacheOptions = null,
        CsvReadOptions? readOptions = null,
        CancellationToken cancellation = default!)
    {
        var files = (await directory.GetFilesWithExtensionAsync(".csv", cancellation)).ToList();

        if (!files.Any())
        {
            throw new InvalidOperationException("No files found in the directory.");
        }

        Schema? schema = null;
        var tables = new List<CsvDataTable>();

        foreach (var file in files)
        {
            if (schema == null)
            {
                var table = await CsvDataTable.FromStreamAsync(tableName, file, queryContext, cacheOptions, readOptions, cancellation);
                schema = table.Schema;
                tables.Add(table);
            }
            else
            {
                var table = CsvDataTable.FromSchema(tableName, file, schema, cacheOptions, readOptions);
                tables.Add(table);
            }
        }

        context.RegisterDataTable(new MultiSourceDataTable(tableName, tables, schema!));
    }
}