using Prequel.Data;

namespace Prequel.Logical.Values;

/// <summary>
/// Scalar value in string (long) form
/// </summary>
/// <param name="Value">Strongly typed string value</param>
internal record StringScalar(string? Value) : ScalarValue(Value, ColumnDataType.Utf8)
{
    public override bool IsEqualTo(object? value)
    {
        if (value == null)
        {
            return Value == null;
        }

        try
        {
            if (value is string s)
            {
                return Value == s;
            }

            return Value == value.ToString();
        }
        catch
        {
            return false;
        }
    }

    public override string ToString()
    {
        return Value ?? "";
    }
}