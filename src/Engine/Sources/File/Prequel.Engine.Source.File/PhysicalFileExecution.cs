using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Execution;
using Prequel.Engine.Core.Metrics;
using System.Runtime.CompilerServices;

namespace Prequel.Engine.Source.File;

/// <summary>
/// Physical file execution plan base class for 
/// querying data read from a file stream
/// </summary>
public class PhysicalFileExecution : IExecutionPlan
{
    private readonly List<int> _projection;
    private readonly PhysicalFileDataTable _dataTable;

    public PhysicalFileExecution(Schema schema, List<int> projection, PhysicalFileDataTable dataTable)
    {
        _projection = projection;
        _dataTable = dataTable;
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
    /// <param name="queryContext">Query queryContext</param>
    /// <param name="cancellation">Optional cancellation token</param>
    /// <returns>Async enumerable record batches</returns>
    public virtual async IAsyncEnumerable<RecordBatch> ExecuteAsync(QueryContext queryContext,
        [EnumeratorCancellation] CancellationToken cancellation = default!)
    {
        using var step = queryContext.Profiler.Step("Execution Plan, Physical file, Read data table");

        await foreach (var slice in _dataTable.ReadAsync(_projection, queryContext, cancellation))
        {
            var batch = new RecordBatch(Schema);

            //TODO mod batch size & repartition???
            foreach (var line in slice)
            {
                step.IncrementRowCount();

                for (var i = 0; i < _projection.Count; i++)
                {
                    batch.AddResult(i, line[_projection[i]]);
                }
            }

            step.IncrementBatch(batch.RowCount);

            yield return batch;
        }
    }
}