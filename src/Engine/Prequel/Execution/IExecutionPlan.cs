﻿using Prequel.Data;
using Prequel.Logical;

namespace Prequel.Execution;

/// <summary>
/// Outlines the schema and execution operation for executing
/// a physical query plan step.
/// </summary>
public interface IExecutionPlan
{
    Schema Schema { get; }
    IAsyncEnumerable<RecordBatch> ExecuteAsync(QueryContext queryContext, CancellationToken cancellation = default!);

    string ToStringIndented(Indentation? indentation = null);
}