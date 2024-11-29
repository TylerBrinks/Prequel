using System.Collections;

namespace Prequel.Data;

/// <summary>
/// Value array containing double values
/// </summary>
internal class DoubleArray : TypedRecordArray<double?>
{
    public override IList Values => List;
    /// <summary>
    /// Adds a new double value to the array
    /// </summary>
    /// <param name="value">Value to add to the array</param>
    internal override bool Add(object? value)
    {
        if (value is double val)
        {
            List.Add(val);
            return true;
        }

        var parsed = double.TryParse(value?.ToString(), out var result);
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
    /// <param name="size"></param>
    /// <returns>Double record array</returns>
    public override RecordArray NewEmpty(int size)
    {
        return new DoubleArray().FillWithNull(size);
    }
}