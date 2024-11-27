using Prequel.Engine.Caching;
using Prequel.Engine.Core.Data;
using Prequel.Engine.IO;
using ExecutionContext = Prequel.Engine.Core.Execution.ExecutionContext;

namespace Prequel.Engine.Source.Avro;

public static class ExecutionContextExtensions
{
    /// <summary>
    /// Registers a Avro file as a table data source
    /// </summary>
    /// <param name="context">Execution context instance</param>
    /// <param name="tableName">Name used when querying the data source</param>
    /// <param name="fileStream">File source instance</param>
    /// <param name="queryContext">Options for controlling the query output</param>
    /// <param name="cacheOptions">Caching options</param>
    /// <param name="readOptions">Avro read context</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Awaitable task</returns>
    public static async Task RegisterAvroFileAsync(
        this ExecutionContext context,
        string tableName,
        IFileStream fileStream,
        QueryContext? queryContext = null,
        CacheOptions? cacheOptions = null,
        AvroReadOptions? readOptions = null,
        CancellationToken cancellation = default!)
    {
        var table = await AvroDataTable.FromStreamAsync(
            tableName,
            fileStream,
            queryContext ?? new(),
            cacheOptions ?? new(),
            readOptions ?? new AvroReadOptions(),
            cancellation);
        context.RegisterDataTable(table);
    }

    /// <summary>
    /// Registers all files in a directory as table data sources using provided Avro read context
    /// </summary>
    /// <param name="context">Execution context instance</param>
    /// <param name="tableName">Name used when querying the data source</param>
    /// <param name="directory">Directory containing files that can be enumerated</param>
    /// <param name="queryContext">Options for controlling the query output</param>
    /// <param name="cacheOptions">Caching options</param>
    /// <param name="readOptions">Avro read context</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Awaitable task</returns>
    public static async Task RegisterAvroDirectoryAsync(
        this ExecutionContext context,
        string tableName,
        IDirectory directory,
        QueryContext? queryContext = null,
        CacheOptions? cacheOptions = null,
        AvroReadOptions? readOptions = null,
        CancellationToken cancellation = default!)
    {
        var files = (await directory.GetFilesWithExtensionAsync(".avro", cancellation)).ToList();
        queryContext ??= new();

        if (files.Count == 0)
        {
            throw new InvalidOperationException("No files found in the directory.");
        }

        Schema? schema = null;
        var tables = new List<AvroDataTable>();

        foreach (var file in files)
        {
            if (schema == null)
            {
                var table = await AvroDataTable.FromStreamAsync(tableName, file, queryContext, cacheOptions, readOptions, cancellation);
                schema = table.Schema;
                tables.Add(table);
            }
            else
            {
                var table = AvroDataTable.FromSchema(tableName, file, schema, cacheOptions, readOptions);
                tables.Add(table);
            }
        }

        context.RegisterDataTable(new MultiSourceDataTable(tableName, tables, schema!));
    }
}
