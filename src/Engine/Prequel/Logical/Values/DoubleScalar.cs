using Prequel.Data;

namespace Prequel.Logical.Values;

/// <summary>
/// Scalar value in double form
/// </summary>
/// <param name="Value">Strongly typed double value</param>
internal record DoubleScalar(double Value) : ScalarValue(Value, ColumnDataType.Double)
{
    public override bool IsEqualTo(object? value)
    {
        if (value == null)
        {
            return false;
        }

        try
        {
            if (value is double d)
            {
                return Value.Equals(d);// Value == d;
            }

            return Value.Equals(Convert.ToDouble(value)); //Value == Convert.ToDouble(value);
        }
        catch
        {
            return false;
        }
    }

    public override string ToString()
    {
        return RawValue == null ? "" : RawValue.ToString()!;
    }
}