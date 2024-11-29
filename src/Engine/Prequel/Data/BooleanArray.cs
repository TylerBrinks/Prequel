using System.Collections;

namespace Prequel.Data;

/// <summary>
/// Array of nullable boolean values used when
/// building record batches
/// </summary>
internal class BooleanArray : TypedRecordArray<bool?>
{
    internal BooleanArray()
    {
    }

    internal BooleanArray(IEnumerable<bool> values)
    {
        foreach (var value in values)
        {
            List.Add(value);
        }
    }
    /// <summary>
    /// Gets the underlying boolean values in list form
    /// </summary>
    public override IList Values => List;
    /// <summary>
    /// Boolean list value indexer
    /// </summary>
    /// <param name="index">Value index</param>
    /// <returns>Boolean value at the specified index</returns>
    internal bool this[int index]
    {
        get => List[index] ?? false;
        set => List[index] = value;
    }
    /// <summary>
    /// Adds a new boolean value or null to the array 
    /// </summary>
    /// <param name="value">Value to add to the array</param>
    internal override bool Add(object? value)
    {
        if (value is bool val)
        {
            List.Add(val);
            return true;
        }

        var parsed = bool.TryParse(value?.ToString(), out var result);

        if (parsed)
        {
            List.Add(result);
        }
        else
        {
            List.Add(null);
        }

        return true;
    }
    /// <summary>
    /// Creates a new, empty array of a specified size
    /// </summary>
    /// <param name="size">Array size</param>
    /// <returns>Boolean record array</returns>
    public override RecordArray NewEmpty(int size)
    {
        return new BooleanArray().FillWithNull(size);
    }
}