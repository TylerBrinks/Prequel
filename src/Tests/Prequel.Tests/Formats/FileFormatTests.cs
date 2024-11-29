using Prequel.Data;
using Prequel.Engine.Source.Memory;
using Prequel.Engine.Source.Avro;
using Prequel.Engine.Source.Csv;
using Prequel.Engine.Source.File;
using Prequel.Engine.Source.Json;
using Prequel.Engine.Source.Parquet;

namespace Prequel.Tests.Formats
{
    public class FileFormatTests
    {
        private readonly string _csvPath;
        private readonly string _avroPath;
        private readonly string _jsonPath;
        private readonly string _parquetPath;

        public FileFormatTests()
        {
            var root = $"{Directory.GetCurrentDirectory().TrimEnd('\\', '/')}//Integration";
            _csvPath = $"{root}/db_departments.csv";
            _avroPath = $"{root}/test.avro";
            _jsonPath = $"{root}/test.json";
            _parquetPath = $"{root}/test.parquet";
        }

        private async Task<CsvDataTable> GetCsvTable(CsvReadOptions readOptions)
        {
            var bytes = await File.ReadAllBytesAsync(_csvPath);
            var source = new InMemoryStream(bytes);
            return await CsvDataTable.FromStreamAsync("csv", source, readOptions: readOptions);
        }

        private async Task<AvroDataTable> GetAvroTable()
        {
            var bytes = await File.ReadAllBytesAsync(_avroPath);
            var source = new InMemoryStream(bytes);
            return await AvroDataTable.FromStreamAsync("avro", source);
        }

        private async Task<JsonDataTable> GetJsonTable()
        {
            var bytes = await File.ReadAllBytesAsync(_jsonPath);
            var source = new InMemoryStream(bytes);
            return await JsonDataTable.FromStreamAsync("json", source);
        }

        private async Task<ParquetDataTable> GetParquetTable()
        {
            var bytes = await File.ReadAllBytesAsync(_parquetPath);
            var source = new InMemoryStream(bytes);
            return await ParquetDataTable.FromStreamAsync("parquet", source);
        }

        #region CSV
        [Fact]
        public async Task Csv_Infers_Schema()
        {
            var csv = await GetCsvTable(new CsvReadOptions {InferMax = 2, HasHeader = false});
            Assert.Equal(4, csv.Schema!.Fields.Count);
        }

        [Fact]
        public async Task Csv_Reads_Data()
        {
            var csv = await GetCsvTable(new CsvReadOptions { InferMax = 2, HasHeader = true });
            var count = 0;

            await foreach (var batch in csv.ReadAsync([1, 3], new QueryContext{BatchSize = 1}))
            {
                Assert.Single(batch);
                count++;
            }

            Assert.Equal(27, count);
        }

        [Fact]
        public async Task Csv_Creates_Execution()
        {
            var csv = await GetCsvTable(new CsvReadOptions { InferMax = 2, HasHeader = true });
            var plan = csv.Scan([1, 3]) as PhysicalFileExecution;

            Assert.NotNull(plan);
        }
        #endregion

        #region Avro
        [Fact]
        public async Task Avro_Infers_Schema()
        {
            var avro = await GetAvroTable();

            Assert.Equal(11, avro.Schema!.Fields.Count);
        }

        [Fact]
        public async Task Avro_Reads_Data()
        {

            var avro = await GetAvroTable();
            var count = 0;

            await foreach (var batch in avro.ReadAsync([1, 3], new QueryContext { BatchSize = 1 }))
            {
                Assert.Single(batch);
                count++;
            }

            Assert.Equal(8, count);
        }

        [Fact]
        public async Task Avro_Creates_Execution()
        {
            var avro = await GetAvroTable();

            var plan = avro.Scan([1, 3]) as PhysicalFileExecution;

            Assert.NotNull(plan);
        }
        #endregion

        #region JSON
        [Fact]
        public async Task Json_Infers_Schema()
        {
            //var bytes = await File.ReadAllBytesAsync(_jsonPath);
            //var source = new InMemoryFileStream(bytes);
            //var json = await JsonDataSourceTable.FromSchemaAsync(source);
            var json = await GetJsonTable();

            Assert.Equal(4, json.Schema!.Fields.Count);
        }

        //[Fact]
        //public async Task Json_Reads_Data()
        //{
        //    var json = await GetJsonTable();
        //    var count = 0;

        //    await foreach (var batch in json.ReadAsync(new List<int> { 1, 3 }, 1))
        //    {
        //        Assert.Single(batch);
        //        count++;
        //    }

        //    Assert.Equal(4, count);
        //}

        [Fact]
        public async Task Json_Creates_Execution()
        {
            //var bytes = await File.ReadAllBytesAsync(_jsonPath);
            //var source = new InMemoryFileStream(bytes);
            //var json = await JsonDataSourceTable.FromSchemaAsync(source);
            var json = await GetJsonTable();

            var plan = json.Scan([1, 3]) as PhysicalFileExecution;

            Assert.NotNull(plan);
        }
        #endregion

        #region Parquet
        [Fact]
        public async Task Parquet_Infers_Schema()
        {
            var parquet = await GetParquetTable();

            Assert.Equal(11, parquet.Schema!.Fields.Count);
        }

        [Fact]
        public async Task Parquet_Reads_Data()
        {
            var parquet = await GetParquetTable();

            var count = 0;

            await foreach (var batch in parquet.ReadAsync([1, 3], new QueryContext { BatchSize = 1 }))
            {
                Assert.Single(batch);
                count++;
            }

            Assert.Equal(8, count);
        }

        [Fact]
        public async Task Parquet_Creates_Execution()
        {
            var parquet = await GetParquetTable();

            var plan = parquet.Scan([1, 3]) as PhysicalFileExecution;

            Assert.NotNull(plan);
        }
        #endregion
    }
}
