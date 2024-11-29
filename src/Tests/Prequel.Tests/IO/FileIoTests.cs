using Moq;
using Prequel.Tests.Fakes;
using Prequel.Engine.Caching;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Source.File;
using Prequel.Engine.Core.Data;
using Prequel.Engine.IO;
using Prequel.Engine.Caching;

namespace Prequel.Tests.IO
{
    public class FileIOTests
    {
        [Fact]
        public async Task LocalDirectory_Filters_Files()
        {
            var path = Directory.GetCurrentDirectory().TrimEnd('\\', '/') + "/Integration";

            var files = await new LocalDirectory(path).GetFilesWithExtensionAsync("csv");

            Assert.Equal(5, files.Count());
        }

        [Fact]
        public async Task LocalFileStream_Opens_Write_Stream()
        {
            var path = Directory.GetCurrentDirectory().TrimEnd('\\', '/') + "/Integration/write.txt";

            var file = new LocalFileStream(path);
            await using var stream = await file.GetWriteStreamAsync();
            stream.Close();
            File.Delete(path);
        }

        [Fact]
        public async Task FileDataReader_Queries_File_Data()
        {
            var table = new FakePhysicalFileDataTable("tablename", new QueryContext());
            var reader = new FileQueryDataSourceReader("name", "select * from tablename", table);

            var rows = new List<object?[]>();

            await foreach (var row in reader.ReadSourceAsync(new QueryContext()))
            {
                rows.Add(row);
            }

            Assert.Equal(2048, rows.Count);
        }

        [Fact]
        public async Task FileDataReader_Infers_Schema()
        {
            var table = new FakePhysicalFileDataTable("tablename", new QueryContext());
            var reader = new FileQueryDataSourceReader("name", "select * from tablename", table);

            var schema = await reader.QuerySchemaAsync();

            Assert.Single(schema.Fields);
        }

        [Fact]
        public async Task FileDataReader_Infers_Schema_Without_Query()
        {
            var table = new FakePhysicalFileDataTable("tablename", new QueryContext());
            var reader = new FileQueryDataSourceReader("name", null, table);

            var schema = await reader.QuerySchemaAsync();

            Assert.Single(schema.Fields);
        }

        [Fact]
        public async Task OutputCacheExecution_Skips_Cache_Without_Options()
        {
            var schema = new Schema([new("name", ColumnDataType.Integer)]);
            var projection = new List<int> { 0 };
            var sourcePlan = new FakeTableExecution(schema, projection);
            var outputCache = new Mock<ICacheProvider>();
            outputCache.Setup(c => c.ExistsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync(false);
            var writer = new Mock<IDataWriter>();

            var cacheExecution = new OutputCacheExecution(schema, new CacheOptions(), sourcePlan);

            await foreach (var _ in cacheExecution.ExecuteAsync(new QueryContext())) { }

            Assert.True(sourcePlan.Executed);
        }

        [Fact]
        public async Task OutputCacheExecution_Skips_Cache_With_Cache_Key()
        {
            var schema = new Schema([new("name", ColumnDataType.Integer)]);
            var projection = new List<int> { 0 };
            var sourcePlan = new FakeTableExecution(schema, projection);
            var outputCache = new Mock<ICacheProvider>();
            outputCache.Setup(c => c.ExistsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync(false);
            var writer = new Mock<IDataWriter>();

            var options = new CacheOptions
            {
                DurableCacheKey = "key"
            };
            var cacheExecution = new OutputCacheExecution(schema, options, sourcePlan);

            await foreach (var _ in cacheExecution.ExecuteAsync(new QueryContext())) { }

            Assert.True(sourcePlan.Executed);
        }
    }
}