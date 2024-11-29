using Prequel.Data;
using Prequel.Engine.Source.Csv;
using Prequel.Engine.Source.File;
using Prequel.Model.Execution.Database;
using CacheOptions = Prequel.Engine.Caching.CacheOptions;

namespace Prequel.Model.Execution.Files;

/// <summary>
/// CSV file connection data table
/// </summary>
public class CsvFileDataSourceConnection : FileDataSourceConnection, IFileDataSourceConnection
{
    public string Delimiter { get; set; } = ",";
    public bool HasHeader { get; set; } = true;

    public override FileQueryDataSourceReader CreateReader(
        string name,
        string query,
        Schema schema,
        CacheOptions cacheOptions)
    {
        var readOptions = new CsvReadOptions
        {
            Delimiter = Delimiter,
            HasHeader = HasHeader,
            InferMax = InferMax
        };

        var fileStream = FileStreamProvider.GetFileStream();

        var csvTable = CsvDataTable.FromSchema(
            name,
            fileStream,
            schema,
            cacheOptions,
            readOptions);

        return new FileQueryDataSourceReader(name, query, csvTable);
    }
}