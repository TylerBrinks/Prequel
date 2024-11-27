﻿using Prequel.Engine.Caching;
using Prequel.Engine.Core.Data;
using Prequel.Engine.IO;
using ExecutionContext = Prequel.Engine.Core.Execution.ExecutionContext;

namespace Prequel.Engine.Source.Parquet;

public static class ExecutionContextExtensions
{
    /// <summary>
    /// Registers a Parquet file as a table data source
    /// </summary>
    /// <param name="context">Execution context instance</param>
    /// <param name="tableName">Name used when querying the data source</param>
    /// <param name="fileStream">File source instance</param>
    /// <param name="queryContext">Options for controlling the query output</param>
    /// <returns>Awaitable task</returns>
    public static async Task RegisterParquetFileAsync(
        this ExecutionContext context,
        string tableName,
        IFileStream fileStream,
        QueryContext? queryContext = null)
    {
        var table = await ParquetDataTable.FromStreamAsync(tableName, fileStream, queryContext);
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
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Awaitable task</returns>
    public static async Task RegisterParquetDirectoryAsync(
        this ExecutionContext context,
        string tableName,
        IDirectory directory,
        QueryContext? queryContext = null,
        CacheOptions? cacheOptions = null,
        CancellationToken cancellation = default!)
    {
        var files = (await directory.GetFilesWithExtensionAsync(".parquet", cancellation)).ToList();

        if (!files.Any())
        {
            throw new InvalidOperationException("No files found in the directory.");
        }

        Schema? schema = null;
        var tables = new List<ParquetDataTable>();

        foreach (var file in files)
        {
            if (schema == null)
            {
                var table = await ParquetDataTable.FromStreamAsync(tableName, file, queryContext, cacheOptions, cancellation);
                schema = table.Schema;
                tables.Add(table);
            }
            else
            {
                var table = ParquetDataTable.FromSchema(tableName, file, schema, cacheOptions);
                tables.Add(table);
            }
        }

        context.RegisterDataTable(new MultiSourceDataTable(tableName, tables, schema!));
    }
}
