using Prequel.Engine.Core.Data;

namespace Prequel.Engine.Core.Values;

/// <summary>
/// Column value expecting a data of a given column type
/// </summary>
/// <param name="DataType">Column type of data values</param>
internal abstract record ColumnValue(ColumnDataType DataType)
{
    /// <summary>
    /// Size of the value array
    /// </summary>
    internal abstract int Size { get; }
    /// <summary>
    /// Gets a value at a given index in the array
    /// </summary>
    /// <param name="index">Array value index to return</param>
    /// <returns>Value at the specified index</returns>
    internal abstract object? GetValue(int index);
}