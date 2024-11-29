using Prequel.Engine.Core;
using System.Collections;

namespace Prequel.Engine.Core.Data;

/// <summary>
/// Value array containing integer values
/// </summary>
internal class IntegerArray : TypedRecordArray<int?>, IUpcastNumericArray
{
    public override IList Values => List;
    /// <summary>
    /// Adds a new integer value to the array
    /// </summary>
    /// <param name="value">Value to add to the array</param>
    internal override bool Add(object? value)
    {
        if (value is int val)
        {
            List.Add(val);
            return true;
        }

        var stringValue = value?.ToString();
        var parsed = int.TryParse(stringValue, out var result);

        if (parsed)
        {
            List.Add(result);
            return true;
        }

        var isNumeric = stringValue.ParseNumeric().IsNumeric;
        //List.Add(null);
        if (!isNumeric)
        {
            List.Add(null);
        }

        return !isNumeric;
    }
    void INumericArray.AddNumeric(object number)
    {
        List.Add(Convert.ToInt32(number));
    }
    /// <summary>
    /// Creates a new, empty array of a specified size
    /// </summary>
    /// <param name="size">Array size</param>
    /// <returns>Integer record array</returns>
    public override RecordArray NewEmpty(int size)
    {
        return new IntegerArray().FillWithNull(size);
    }
}