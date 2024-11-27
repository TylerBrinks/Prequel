using Prequel.Engine.Core.Data;

namespace Prequel.Engine.Core.Values;

/// <summary>
/// Column with an array of boolean values
/// </summary>
/// <param name="Values">Boolean values in the array</param>
internal record BooleanColumnValue(bool?[] Values) : ColumnValue(ColumnDataType.Boolean)
{
    internal BooleanColumnValue(IEnumerable<bool> values) : this(values.Cast<bool?>().ToArray())
    {
    }
    /// <summary>
    /// Size of the value array
    /// </summary>
    internal override int Size => Values.Length;
    /// <summary>
    /// Gets a value at a given index in the array
    /// </summary>
    /// <param name="index">Array value index to return</param>
    /// <returns>Value at the specified index</returns>
    internal override object? GetValue(int index)
    {
        return Values[index];
    }
    /// <summary>
    /// Creates a similar column with inverted values.
    /// </summary>
    /// <returns>New boolean column value with inverted values</returns>
    public BooleanColumnValue Invert()
    {
        for (var i = 0; i < Values.Length; i++)
        {
            Values[i] = !Values[i];
        }

        return this;
    }
}