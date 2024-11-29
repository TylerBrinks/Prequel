using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Metrics;
using Prequel.Engine.IO;
using Prequel.Engine.Source.Parquet;
using Exec = Prequel.Engine.Core.Execution.ExecutionContext;

namespace Prequel.Tests.Metrics;

public class QueryProfilerTests
{
    [Fact]
    public async Task Profiler_Measures_Child_Timings()
    {
        var profiler = new QueryProfiler("root");
        using (profiler.Step("Level 1"))
        {
            using (profiler.Step("Level 2"))
            {
                using (profiler.Step("Level 3"))
                {
                    await Task.Delay(500);
                }

                await Task.Delay(500);
            }

            await Task.Delay(500);
        }

        profiler.Stop();
        Assert.True((double) profiler.DurationMilliseconds >= TimeSpan.FromSeconds(1.5).TotalMilliseconds);
        Assert.True(profiler.Root.DurationMilliseconds > profiler.Root.Children![0].DurationMilliseconds);
        Assert.True(profiler.Root.Children[0].DurationMilliseconds > profiler.Root.Children![0].Children![0].DurationMilliseconds);
        Assert.True(profiler.Root.Children![0].Children![0].DurationMilliseconds > profiler.Root.Children![0].Children![0].Children![0].DurationMilliseconds);
    }

    [Fact]
    public async Task Profiler_Timings_Isolate_Own_Execution_Time()
    {
        var profiler = new QueryProfiler("root");

        using (profiler.Step("Level 1"))
        {
            using (profiler.Step("Level 2"))
            {
                using (profiler.Step("Level 3"))
                {
                    await Task.Delay(500);
                }

                await Task.Delay(500);
            }

            await Task.Delay(500);
        }

        profiler.Stop();

        Assert.True((double) profiler.Root.DurationIsolatedMilliseconds! < TimeSpan.FromMilliseconds(650).TotalMilliseconds);
        Assert.True((double) profiler.Root.Children![0].DurationIsolatedMilliseconds! < TimeSpan.FromMilliseconds(650).TotalMilliseconds);
        Assert.True((double) profiler.Root.Children![0].Children![0].DurationIsolatedMilliseconds! < TimeSpan.FromMilliseconds(650).TotalMilliseconds);
        Assert.True((double) profiler.Root.Children![0].Children![0].Children![0].DurationIsolatedMilliseconds! < TimeSpan.FromMilliseconds(650).TotalMilliseconds);
    }

    [Fact]
    public async Task Profiler_Logs_Execution_Step_Batches()
    {
        var query = new Exec();
        var root = Directory.GetCurrentDirectory().TrimEnd('\\', '/');
        await query.RegisterParquetFileAsync("parquet", new LocalFileStream($"{root}/Integration/test.parquet"));

        var context = new QueryContext { BatchSize = 2 };

        await foreach (var _ in query.ExecuteQueryAsync("select id from parquet limit 7", context))
        {
        }

        var profiler = context.Profiler;

        var timing = profiler.Root;
        Assert.Equal(0, timing.RowCount);

        // Execution returns 7 results batched in groups of 2
        timing = timing!.Children![0]; // Enumerate physical execution plan
        Assert.Equal(4, timing.BatchCount);
        Assert.Equal(7, timing.BatchRowCount);
        
        timing = timing!.Children![0]; // Limit execution
        Assert.Equal(4, timing.BatchCount);
        Assert.Equal(8, timing.BatchRowCount);

        timing = timing!.Children![0]; // Read physical file data table
        Assert.Equal(4, timing.BatchCount);
        Assert.Equal(8, timing.BatchRowCount);

        timing = timing!.Children![0]; // Data Table reader 
        Assert.Equal(8, timing.RowCount);
        Assert.Equal(0, timing.BatchCount);// No batches; rows pass through
        Assert.Equal(0, timing.BatchRowCount); 

        timing = timing!.Children![0]; // Data Source Reader
        Assert.Equal(8, timing.RowCount);
        Assert.Equal(0, timing.BatchRowCount); // No batches; rows pass through
    }
}