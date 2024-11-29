namespace Prequel.Data;

/// <summary>
/// Value array containing Timestamp values
/// </summary>
internal class TimeStampArray : DateTimeArray
{
    /// <summary>
    /// Format dates using the ISO8601 standard format
    /// </summary>
    /// <param name="ordinal">Index of the field to serialize</param>
    /// <returns>String date format</returns>
    public override string GetStringValue(int ordinal)
    {
        var date = List[ordinal];
        return date == null ? "" : date.Value.ToString("O");
    }
}