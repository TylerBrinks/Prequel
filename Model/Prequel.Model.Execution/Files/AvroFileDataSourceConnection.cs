using Prequel.Engine.Core.Data;
using Prequel.Engine.Source.Avro;
using Prequel.Engine.Source.File;
using Prequel.Model.Execution.Database;
using CacheOptions = Prequel.Engine.Caching.CacheOptions;

namespace Prequel.Model.Execution.Files;

/// <summary>
/// CSV file connection data table
/// </summary>
public class AvroFileDataSourceConnection : FileDataSourceConnection, IFileDataSourceConnection
{
    public override FileQueryDataSourceReader CreateReader(
        string name,
        string query,
        Schema schema,
        CacheOptions cacheOptions)
    {
        var readOptions = new AvroReadOptions
        {
            InferMax = InferMax
        };

        var fileStream = FileStreamProvider.GetFileStream();

        var csvTable = AvroDataTable.FromSchema(
            name,
            fileStream,
            schema,
            cacheOptions,
            readOptions);

        return new FileQueryDataSourceReader(name, query, csvTable);
    }
}