using System.Collections;

namespace Prequel.Engine.Core.Data;

/// <summary>
/// Value array containing DateTime values
/// </summary>
internal class DateTimeArray : TypedRecordArray<DateTime?>
{
    private bool _hasTime;

    public override IList Values => List;
    /// <summary>
    /// Adds a new DateTime value to the array
    /// </summary>
    /// <param name="value">Value to add to the array</param>
    internal override bool Add(object? value)
    {
        if (value != null)
        {
            DateTime dateToAdd;

            if (value is DateTime date)
            {
                dateToAdd = date;
            }
            else
            {
                dateToAdd = DateTime.MinValue;
                try
                {
                    dateToAdd = Convert.ToDateTime(value);
                }
                catch
                {
                    dateToAdd = DateTime.MinValue;
                }
            }

            List.Add(dateToAdd);

            if (!_hasTime && dateToAdd.TimeOfDay.TotalSeconds > 0)
            {
                _hasTime = true;
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
    /// <param name="size"></param>
    /// <returns>DateTime record array</returns>
    public override RecordArray NewEmpty(int size)
    {
        return new TimeStampArray().FillWithNull(size);
    }
    /// <summary>
    /// Format dates using the ISO8601 standard format
    /// </summary>
    /// <param name="ordinal">Index of the field to serialize</param>
    /// <returns>String date format</returns>
    public override string GetStringValue(int ordinal)
    {
        var date = List[ordinal];
        var format = _hasTime ? "s" : "d";
        return date == null ? "" : date.Value.ToString(format);
    }
}