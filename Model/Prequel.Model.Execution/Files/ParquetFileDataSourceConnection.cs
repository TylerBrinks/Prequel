using Prequel.Engine.Core.Data;
using Prequel.Engine.Source.File;
using Prequel.Engine.Source.Parquet;
using Prequel.Model.Execution.Database;
using CacheOptions = Prequel.Engine.Caching.CacheOptions;

namespace Prequel.Model.Execution.Files;

/// <summary>
/// CSV file connection data table
/// </summary>
public class ParquetFileDataSourceConnection : FileDataSourceConnection, IFileDataSourceConnection
{
    public override FileQueryDataSourceReader CreateReader(
        string name,
        string query,
        Schema schema,
        CacheOptions cacheOptions)
    {
        var fileStream = FileStreamProvider.GetFileStream();

        var csvTable = ParquetDataTable.FromSchema(
            name,
            fileStream,
            schema,
            cacheOptions);

        return new FileQueryDataSourceReader(name, query, csvTable);
    }
}