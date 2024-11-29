using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Execution;
using Prequel.Engine.Core.Metrics;
using Prequel.Engine.IO;
using System.Runtime.CompilerServices;

namespace Prequel.Engine.Source.Execution;

/// <summary>
/// Data source reader execution plan executes a plan's
/// query context against a given data source
/// </summary>
public class DataSourceReaderExecution : IExecutionPlan
{
    private readonly List<int> _projection;
    private readonly IDataSourceReader _reader;

    public DataSourceReaderExecution(Schema schema, List<int> projection, IDataSourceReader reader)
    {
        _projection = projection;
        _reader = reader;
        var fields = schema.Fields.Where((_, i) => projection.Contains(i)).ToList();
        Schema = new Schema(fields);
    }

    /// <summary>
    /// Schema of the data contained in the data source
    /// </summary>
    public Schema Schema { get; }

    /// <summary>
    /// Reads the file from the data source and creates record
    /// batch instances using data read from the file read operation.
    /// </summary>
    /// <param name="queryContext">Query context</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Async enumerable record batches</returns>
    public virtual async IAsyncEnumerable<RecordBatch> ExecuteAsync(
        QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        var batch = new RecordBatch(Schema);
        var recordCount = 0;

        using (queryContext.Profiler.Step("Execution Plan, Data source reader, Read source"))
        {
            // Query the source for individual rows
            await foreach (var row in _reader.ReadSourceAsync(queryContext, cancellation))
            {
                var ordinal = 0;

                // Check if the column is included in the incoming query
                for (var i = 0; i < row.Length; i++)
                {
                    if (_projection.Contains(i))
                    {
                        batch.AddResult(ordinal++, row[i]);
                    }
                }

                recordCount++;
                // Results are batched.  Continue processing until the
                // configured batch size is reached
                if (recordCount % queryContext.BatchSize != 0)
                {
                    continue;
                }

                yield return batch;
                // the batch has been processed; reuse the existing
                // batch for the next set of output values.
                batch = new RecordBatch(Schema);
            }
        }

        // Finalize any records that did not meet the batch size threshold.
        if (batch.RowCount > 0)
        {
            yield return batch;
        }
    }
}
