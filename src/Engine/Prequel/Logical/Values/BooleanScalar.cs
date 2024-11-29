using Prequel.Data;

namespace Prequel.Logical.Values;

/// <summary>
/// Scalar value in boolean form
/// </summary>
/// <param name="Value">Strongly typed boolean value</param>
internal record BooleanScalar(bool Value) : ScalarValue(Value, ColumnDataType.Boolean)
{
    public override bool IsEqualTo(object? value)
    {
        if (value is not bool)
        {
            return false;
        }

        //try
        //{
        return Value == Convert.ToBoolean(value);
        //}
        //catch
        //{
        //    return false;
        //}
    }

    public override string ToString()
    {
        return Value.ToString();
    }
}