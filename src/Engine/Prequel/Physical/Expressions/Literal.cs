using Prequel.Data;
using Prequel.Logical.Values;
using Prequel.Values;

namespace Prequel.Physical.Expressions;

/// <summary>
/// Literal physical expression
/// </summary>
/// <param name="Value">Literal scalar value</param>
internal record Literal(ScalarValue Value) : IPhysicalExpression
{
    /// <summary>
    /// Gets the value's data type
    /// </summary>
    /// <param name="schema">Schema containing binary expression field definitions</param>
    /// <returns>Column data type</returns>
    public ColumnDataType GetDataType(Schema schema)
    {
        return Value.DataType;
    }
    /// <summary>
    /// Creates a scalar column value matching the number of
    /// rows in the record batch.
    /// </summary>
    /// <param name="batch">Record batch defining the number of rows to produce using the scalar value</param>
    /// <param name="schemaIndex">Unused</param>
    /// <returns>Scalar column value repeated for each batch row</returns>
    public ColumnValue Evaluate(RecordBatch batch, int? schemaIndex = null)
    {
        return new ScalarColumnValue(Value, batch.RowCount, Value.DataType);
    }
}