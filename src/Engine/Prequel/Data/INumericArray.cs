namespace Prequel.Data;

/// <summary>
/// Defines an array that supports numeric data
/// </summary>
internal interface INumericArray
{
    void AddNumeric(object number);
}


/// <summary>
/// Defines an array that supports numeric data and can be up-cast
/// to a new array type when a new value exceed the current
/// array's max or min value.
/// </summary>
internal interface IUpcastNumericArray : INumericArray
{
    /// <summary>
    /// Converts an Integer array to a Long array
    /// </summary>
    /// <returns>Converted record array</returns>
    public RecordArray Upcast(object? newValue)
    {
        if (newValue == null)
        {
            return (RecordArray)this;
        }

        var arrayType = newValue.ToString().ParseNumeric().NumericType;
#pragma warning disable CS8846 // The switch expression does not handle all possible values of its input type (it is not exhaustive).
        var array = arrayType switch
#pragma warning restore CS8846 // The switch expression does not handle all possible values of its input type (it is not exhaustive).
        {
            not null when arrayType == typeof(byte) => Typecast(typeof(byte)),
            not null when arrayType == typeof(short) => FillNumericArray((INumericArray)Typecast(typeof(short))),
            not null when arrayType == typeof(int) => FillNumericArray((INumericArray)Typecast(typeof(int))),
            not null when arrayType == typeof(long) => FillNumericArray((INumericArray)Typecast(typeof(long)))
        };

        array.Add(newValue);

        return array;

        RecordArray FillNumericArray(INumericArray numericArray)
        {
            foreach (var value in ((RecordArray)this).Values)
            {
                numericArray.AddNumeric(value);
            }

            return (RecordArray)numericArray;
        }
    }

    public RecordArray Typecast(Type arrayType)
    {
        return arrayType switch
        {
            not null when arrayType == typeof(byte) || arrayType == typeof(byte?) => new ByteArray(),
            not null when arrayType == typeof(short) || arrayType == typeof(short?) => new ShortArray(),
            not null when arrayType == typeof(int) || arrayType == typeof(int?) => new IntegerArray(),
            not null when arrayType == typeof(long) || arrayType == typeof(long?) => new LongArray(),
            _ => throw new NotImplementedException("Invalid numeric array type cast")
        };
    }
}
