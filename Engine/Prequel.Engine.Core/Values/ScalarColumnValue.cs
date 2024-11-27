using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Logical.Values;

namespace Prequel.Engine.Core.Values;

/// <summary>
/// Scalar column value containing a single value
/// </summary>
/// <param name="Value">Contained scalar value </param>
/// <param name="RecordCount">Number of items in a projected array of the scalar value</param>
/// <param name="DataType">Data type of contained values</param>
internal record ScalarColumnValue(ScalarValue Value, int RecordCount, ColumnDataType DataType) : ColumnValue(DataType)
{
    /// <summary>
    /// Size of the value array;
    /// </summary>
    internal override int Size => RecordCount;
    /// <summary>
    /// Gets a value at a given index in the array
    /// </summary>
    /// <param name="index">Unused</param>
    /// <returns>Scalar value</returns>
    internal override object? GetValue(int index)
    {
        return Value.RawValue;
    }
}