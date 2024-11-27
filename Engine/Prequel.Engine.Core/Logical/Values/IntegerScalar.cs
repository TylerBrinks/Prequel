using Prequel.Engine.Core.Data;

namespace Prequel.Engine.Core.Logical.Values;

/// <summary>
/// Scalar value in integer (long) form
/// </summary>
/// <param name="Value">Strongly typed long value</param>
internal record IntegerScalar(long Value) : ScalarValue(Value, ColumnDataType.Integer)
{
    public override bool IsEqualTo(object? value)
    {
        if (value == null)
        {
            return false;
        }

        try
        {
            if (value is int i)
            {
                return Value.Equals(i);// Value == i;
            }

            return Value.Equals(Convert.ToInt64(value));// Value == Convert.ToInt64(value);
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