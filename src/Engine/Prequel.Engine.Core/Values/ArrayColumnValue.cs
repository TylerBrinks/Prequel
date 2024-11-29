using System.Collections;
using Prequel.Engine.Core.Data;

namespace Prequel.Engine.Core.Values;

/// <summary>
/// Column with a specific data type and an array of values of that data type
/// </summary>
/// <param name="Values">Values in the array</param>
/// <param name="DataType">Data type of contained values</param>
internal record ArrayColumnValue(IList Values, ColumnDataType DataType) : ColumnValue(DataType)
{
    /// <summary>
    /// Size of the value array
    /// </summary>
    internal override int Size => Values.Count;
    /// <summary>
    /// Gets a value at a given index in the array
    /// </summary>
    /// <param name="index">Array value index to return</param>
    /// <returns>Value at the specified index</returns>
    internal override object? GetValue(int index)
    {
        return Values[index];
    }
}