using Prequel.Engine.Caching;
using Prequel.Engine.Core.Data;
using Prequel.Engine.IO;
using ExecutionContext = Prequel.Engine.Core.Execution.ExecutionContext;

namespace Prequel.Engine.Source.Json;

public static class ExecutionContextExtensions
{
    /// <summary>
    /// Registers a JSON file as a table data source using default JSON read context
    /// </summary>
    /// <param name="context">Execution context instance</param>
    /// <param name="tableName">Name used when querying the data source</param>
    /// <param name="fileStream">File source instance</param>
    /// <param name="queryContext">Options for controlling the query output</param>
    /// <param name="cacheOptions">Caching options</param>
    /// <returns>Awaitable task</returns>
    public static async Task RegisterJsonFileAsync(
        this ExecutionContext context,
        string tableName,
        IFileStream fileStream,
        QueryContext? queryContext = null,
        CacheOptions? cacheOptions = null
        )
    {
        await context.RegisterJsonFileAsync(tableName, fileStream, new JsonOptions(), queryContext ?? new(), cacheOptions);
    }

    /// <summary>
    /// Registers a JSON file as a table data source using provided JSON read context
    /// </summary>
    /// <param name="context">Execution context instance</param>
    /// <param name="tableName">Name used when querying the data source</param>
    /// <param name="fileStream">File source instance</param>
    /// <param name="readOptions">JSON read context</param>
    /// <param name="queryContext">Options for controlling the query output</param>
    /// <param name="cacheOptions">Caching options</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Awaitable task</returns>
    public static async Task RegisterJsonFileAsync(
        this ExecutionContext context,
        string tableName,
        IFileStream fileStream,
        JsonOptions readOptions,
        QueryContext? queryContext = null,
        CacheOptions? cacheOptions = null,
        CancellationToken cancellation = default!)
    {
        var table = await JsonDataTable.FromStreamAsync(tableName, fileStream, queryContext ?? new(), cacheOptions, readOptions, cancellation);
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
    public static async Task RegisterJsonDirectoryAsync(
        this ExecutionContext context,
        string tableName,
        IDirectory directory,
        QueryContext? queryContext = null,
        CacheOptions? cacheOptions = null,
        JsonOptions? readOptions = null,
        CancellationToken cancellation = default!)
    {
        queryContext ??= new();
        var files = (await directory.GetFilesWithExtensionAsync(".json", cancellation)).ToList();

        if (!files.Any())
        {
            throw new InvalidOperationException("No files found in the directory.");
        }

        Schema? schema = null;
        var tables = new List<JsonDataTable>();

        foreach (var file in files)
        {
            if (schema == null)
            {
                var table = await JsonDataTable.FromStreamAsync(tableName, file, queryContext, cacheOptions, readOptions, cancellation);
                schema = table.Schema;
                tables.Add(table);
            }
            else
            {
                var table = JsonDataTable.FromSchema(tableName, file, schema, cacheOptions, readOptions);
                tables.Add(table);
            }
        }

        context.RegisterDataTable(new MultiSourceDataTable(tableName, tables, schema!));
    }
}