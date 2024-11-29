using Prequel.Data;
using Prequel.Values;

namespace Prequel.Physical.Expressions;

/// <summary>
/// Physical column expression
/// </summary>
/// <param name="Name">Column name</param>
/// <param name="Index">Index of the physical column</param>
internal record Column(string Name, int Index) : IPhysicalExpression
{
    /// <summary>
    /// Gets a columns data type from a given schema
    /// </summary>
    /// <param name="schema">Schema containing field data matching the column</param>
    /// <returns>Column's data type</returns>
    public ColumnDataType GetDataType(Schema schema)
    {
        return schema.Fields[Index].DataType;
    }
    /// <summary>
    /// Evaluates a batch creating an array of column values
    /// using values at a given schema column index
    /// </summary>
    /// <param name="batch">Batch containing data to extract</param>
    /// <param name="schemaIndex">Index of the column data</param>
    /// <returns>Array column values</returns>
    public ColumnValue Evaluate(RecordBatch batch, int? schemaIndex = null)
    {
        return new ArrayColumnValue(batch.Results[schemaIndex ?? Index].Values, batch.Schema.Fields[schemaIndex ?? Index].DataType);
    }
}