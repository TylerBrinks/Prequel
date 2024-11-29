using Prequel.Engine.Caching;
using Prequel.Data;
using Moq;
using Prequel.Execution;
using Prequel.Engine.IO;

namespace Prequel.Tests.Caching
{
    public class OutputCacheExecutionTests
    {
        [Fact]
        public async Task OutputCacheExecution_Disposes_Writer()
        {
            var schema = new Schema([new("field", ColumnDataType.Integer)]);

            var writer = new Mock<IDataWriter>();
            writer.Setup(w => w.DisposeAsync());

            var cacheOptions = new CacheOptions
            {
                CacheProvider = new Mock<ICacheProvider>().Object,
                DurableCacheKey = "key",
                GetDataWriter = _ => writer.Object,
                UseDurableCache = true
            };

            var plan = new Mock<IExecutionPlan>();
            plan.Setup(p => p.ExecuteAsync(It.IsAny<QueryContext>(), It.IsAny<CancellationToken>()))
                .Returns(new List<RecordBatch>().ToAsyncEnumerable);

            var cacheExecution = new OutputCacheExecution(schema, cacheOptions, plan.Object);

            await foreach(var batch in cacheExecution.ExecuteAsync(new QueryContext()))
            {
                Assert.NotNull(batch);
            }

            writer.Verify(w => w.DisposeAsync(), Times.Once);
            Assert.Same(schema, cacheExecution.Schema);
        }

        [Fact]
        public async Task OutputCacheExecution_Writes_Data()
        {
            var schema = new Schema([new("field", ColumnDataType.Integer)]);

            var writer = new Mock<IDataWriter>();
            writer.Setup(w => w.DisposeAsync());

            var cacheProvider = new Mock<ICacheProvider>();
            cacheProvider.Setup(w => w.ExistsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync(false);

            var cacheOptions = new CacheOptions
            {
                CacheProvider = cacheProvider.Object,
                DurableCacheKey = "key",
                GetDataWriter = _ => writer.Object,
                UseDurableCache = true
            };

            var plan = new Mock<IExecutionPlan>();
            plan.Setup(p => p.ExecuteAsync(It.IsAny<QueryContext>(), It.IsAny<CancellationToken>()))
                .Returns(new List<RecordBatch>
                {
                    new (schema),
                    new (schema),
                    new (schema)
                }.ToAsyncEnumerable);

            var cacheExecution = new OutputCacheExecution(schema, cacheOptions, plan.Object);

            await foreach (var batch in cacheExecution.ExecuteAsync(new QueryContext()))
            {
                Assert.NotNull(batch);
            }

            writer.Verify(w => w.DisposeAsync(), Times.Once);
            writer.Verify(w => w.WriteAsync(It.IsAny<RecordBatch>(), It.IsAny<CancellationToken>()), Times.Exactly(3));
        }

        [Fact]
        public async Task OutputCacheExecution_Prevents_Cache_Overwrite()
        {
            var schema = new Schema([new("field", ColumnDataType.Integer)]);

            var writer = new Mock<IDataWriter>();
            writer.Setup(w => w.DisposeAsync());

            var cacheProvider = new Mock<ICacheProvider>();
            cacheProvider.Setup(w => w.ExistsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync(true);

            var cacheOptions = new CacheOptions
            {
                CacheProvider = cacheProvider.Object,
                DurableCacheKey = "key",
                GetDataWriter = _ => writer.Object,
                UseDurableCache = true
            };

            var plan = new Mock<IExecutionPlan>();
            plan.Setup(p => p.ExecuteAsync(It.IsAny<QueryContext>(), It.IsAny<CancellationToken>()))
                .Returns(new List<RecordBatch>
                {
                    new (schema),
                    new (schema),
                    new (schema)
                }.ToAsyncEnumerable);

            var cacheExecution = new OutputCacheExecution(schema, cacheOptions, plan.Object);

            await foreach (var batch in cacheExecution.ExecuteAsync(new QueryContext()))
            {
                Assert.NotNull(batch);
            }

            writer.Verify(w => w.DisposeAsync(), Times.Once);
            writer.Verify(w => w.WriteAsync(It.IsAny<RecordBatch>(), It.IsAny<CancellationToken>()), Times.Never);
        }
    }
}
