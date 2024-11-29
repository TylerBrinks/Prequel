﻿using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Values;

namespace Prequel.Engine.Core.Physical.Expressions;

/// <summary>
/// Physical sort expression.  Acts as a placeholder and does not perform sorting.
/// Sorting is handled at the time of execution once all data has been collected.
/// </summary>
/// <param name="Expression">Sort expression</param>
/// <param name="SortSchema">Schema containing sort field definitions</param>
/// <param name="InputSchema">Input plan schema</param>
/// <param name="Ascending">True if sorted in ascending order; otherwise false.</param>
internal record PhysicalSortExpression(
    IPhysicalExpression Expression,
    Schema SortSchema,
    Schema InputSchema,
    bool Ascending) : IPhysicalExpression
{
    public ColumnDataType GetDataType(Schema schema)
    {
        throw new NotImplementedException();
    }

    public ColumnValue Evaluate(RecordBatch batch, int? schemaIndex = null)
    {
        throw new NotImplementedException();
    }
}