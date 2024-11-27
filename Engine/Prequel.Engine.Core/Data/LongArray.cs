using System.Collections;

namespace Prequel.Engine.Core.Data;

/// <summary>
/// Value array containing integer values
/// </summary>
internal class LongArray : TypedRecordArray<long?>, INumericArray
{
    public override IList Values => List;
    /// <summary>
    /// Adds a new integer value to the array
    /// </summary>
    /// <param name="value">Value to add to the array</param>
    internal override bool Add(object? value)
    {
        if (value is long val)
        {
            List.Add(val);
            return true;
        }

        var parsed = long.TryParse(value?.ToString(), out var result);
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
    void INumericArray.AddNumeric(object number)
    {
        List.Add(Convert.ToInt64(number));
    }
    /// <summary>
    /// Creates a new, empty array of a specified size
    /// </summary>
    /// <param name="size">Array size</param>
    /// <returns>Integer record array</returns>
    public override RecordArray NewEmpty(int size)
    {
        return new LongArray().FillWithNull(size);
    }
}