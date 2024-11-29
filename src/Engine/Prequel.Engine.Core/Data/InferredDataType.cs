using Prequel.Engine.Core.Data;
using System.Text.RegularExpressions;

namespace Prequel.Core.Data;

/// <summary>
/// Utility for interrogating sampled data to infer a field's data type
/// </summary>
public partial class InferredDataType
{
    public string? Name { get; set; }

    private static readonly List<Regex> TypeExpressions =
    [
        BooleanRegex(),
        IntegerRegex(),
        DoubleRegex(),
        DateRegex(),
        DateTimeRegex(),
        TimestampRegex()
    ];

    /// <summary>
    /// Checks a value against the type expressions.  The flag
    /// assigned to the type is increased if the type exceeds
    /// the previous flag's value.
    /// value 
    /// </summary>
    /// <param name="value">Value to check</param>
    /// <param name="datetimeRegex">Optional date/time expression for date-based values</param>
    public void Update(string? value, Regex? datetimeRegex = null)
    {
        if (value != null && value.StartsWith('"'))
        {
            DataType = ColumnDataType.Utf8;
            return;
        }

        var matched = false;
        for (var i = 0; i < TypeExpressions.Count; i++)
        {
            if (!TypeExpressions[i].IsMatch(value)) { continue; }

            var suggestedType = (1 << i);

            if (suggestedType > (int) DataType)
            {
                DataType = (ColumnDataType)(1 << i);
            }

            matched = true;
        }

        if (matched) { return; }

        if (DateTime.TryParse(value, out var parsedDate))
        {
            var suggestedDateType = parsedDate.TimeOfDay switch
            {
                { Microseconds: > 0 } => ColumnDataType.TimestampNanosecond,
                { Seconds: > 0 } => ColumnDataType.TimestampSecond,

                _ => ColumnDataType.Date32
            };

            if (suggestedDateType > DataType)
            {
                DataType = suggestedDateType;
            }

            return;
        }

        DataType = datetimeRegex != null && datetimeRegex.IsMatch(value)
            ? ColumnDataType.TimestampNanosecond
            : ColumnDataType.Utf8;
    }

    public override string ToString()
    {
        return DataType.ToString();
    }

    public ColumnDataType DataType { get; private set; }

    #region Generated expressions
    [GeneratedRegex("(?i)^(true)$|^(false)$(?-i)", RegexOptions.None, "en-US")] //TODO: en-US constant
    private static partial Regex BooleanRegex();

    [GeneratedRegex("^-?(\\d+)$", RegexOptions.None, "en-US")]
    private static partial Regex IntegerRegex();

    [GeneratedRegex("^-?((\\d*\\.\\d+|\\d+\\.\\d*)([eE]-?\\d+)?|\\d+([eE]-?\\d+))$", RegexOptions.None, "en-US")]
    private static partial Regex DoubleRegex();

    [GeneratedRegex("^\\d{4}-\\d\\d-\\d\\d$", RegexOptions.None, "en-US")]
    private static partial Regex DateRegex();

    [GeneratedRegex("^\\d{4}-\\d\\d-\\d\\d[T ]\\d\\d:\\d\\d:\\d\\d$", RegexOptions.None, "en-US")]
    private static partial Regex DateTimeRegex();

    //[GeneratedRegex("^\\d{4}-\\d\\d-\\d\\d[T ]\\d\\d:\\d\\d:\\d\\d.\\d{1,3}$", RegexOptions.None, "en-US")]
    //private static partial Regex TimestampMillisecond();

    //[GeneratedRegex("^\\d{4}-\\d\\d-\\d\\d[T ]\\d\\d:\\d\\d:\\d\\d.\\d{1,6}$", RegexOptions.None, "en-US")]
    //private static partial Regex TimestampMicrosecond();

    [GeneratedRegex("^\\d{4}-\\d\\d-\\d\\d[T ]\\d\\d:\\d\\d:\\d\\d.\\d{1,9}$", RegexOptions.None, "en-US")]
    private static partial Regex TimestampRegex();
    #endregion
}