using Prequel.Engine.Core.Data;

namespace Prequel.Engine.Core.Logical.Values;

/// <summary>
/// Scalar value holding a raw, nullable value and identified schema data type
/// </summary>
/// <param name="RawValue">Raw scalar value</param>
/// <param name="DataType">Underlying data type</param>
public abstract record ScalarValue(object? RawValue, ColumnDataType DataType)
{
    public abstract bool IsEqualTo(object? value);
}