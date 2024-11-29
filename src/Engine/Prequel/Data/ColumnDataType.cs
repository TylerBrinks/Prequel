namespace Prequel.Data;

//TODO: simplify timestamps
[Flags]
public enum ColumnDataType
{
    Null = 0,
    Boolean = 1 << 0,
    Integer = 1 << 1,
    Double = 1 << 2,
    Date32 = 1 << 3,
    TimestampSecond = 1 << 4,
    TimestampNanosecond = 1 << 5,
    Utf8 = 1 << 6,
}

public enum IntegerDataType
{
    Byte,
    Short,
    Integer,
    Long
}