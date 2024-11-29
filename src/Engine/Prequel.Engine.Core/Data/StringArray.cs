using System.Collections;

namespace Prequel.Engine.Core.Data;

/// <summary>
/// Array of nullable string values used when
/// building record batches
/// </summary>
internal class StringArray : TypedRecordArray<string?>
{
    /// <summary>
    /// Gets the underlying boolean values in list form
    /// </summary>
    public override IList Values => List;
    /// <summary>
    /// Adds a new string value or null to the array 
    /// </summary>
    /// <param name="value">Value to parse as a boolean</param>
    internal override bool Add(object? value)
    {
        if (value is string val)
        {
            List.Add(val);
            return true;
        }

        if (value != null)
        {
            if (value is string str)
            {
                List.Add(str);
            }
            else
            {
                List.Add(value.ToString());
            }
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
    /// <returns>String record array</returns>
    public override RecordArray NewEmpty(int size)
    {
        return new StringArray().FillWithNull(size);
    }
}