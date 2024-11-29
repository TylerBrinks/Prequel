# Data Types

You’re likely familiar with data types if you’ve ever created a database table, worked with JSON, or even created a CSV file.  Data types are the foundation of a database system including how the system’s query engine operates.

For example, storing a boolean (true/false) value might be as simple as storing a 1 or a 0 (zero) to distinguish between true and false data.  However, numeric fields may also contain the same 1/0 values, but the meaning is entirely different.  And a text-based field may contain data with a "1" or "0." The similarity between the underlying data does not necessarily mean the data is equal. 

In other words, a data type defines how data is stored, the constraints on allowable values, and how data may or may not be converted when it’s under comparison.

## Type System
Databases gather the various data types collectively into a type system.  Most database systems have a robust type system with dozens of types available.  This type diversity helps optimize storage to squeeze as much data into as small a space as possible.  For example, if a system stores 1M rows using a ‘Tinyint’ data type (byte), the system will require 1M bytes for storage.  If the same data were stored using a standard "Integer” data type (4 bytes), which occupies 4 bytes, the same data would require 4M bytes, even if most of it is unused.  

## Schemas
A schema is a collection of metadata for all the data sources within a given database.  A schema will consist of at least 1 field, but may encompass an entire database.  Each "field" in a schema contains metadata about the data type, name (or alias), precision or length constraints, whether the value can be null, and so on.

**Prequel** uses a primitive type system intentionally (for now). Using a simplistic type system makes the system easier to understand, but it also provides a least-common-denominator approach for comparing data from unrelated data sources.  A more advanced type system would be used if data were being stored in binary format and/or the system only operated on one data source.

The rationale behind choosing a simple system is that integrating with schemaless data source types can be challenging.  CSV files are a good example of data that appears structured, but has no formal guarantee.  One could scan 10,000 lines of a CSV finding values in a column that never exceed 255 (i.e. appropriate for an unsigned byte value) only to encounter a value that exceeds the maximum value on the next line. 

The same is true when inferring all values are greater than zero only to (eventually) encounter a negative value.  Choosing a byte vs a long value, or signed vs unsigned numeric value can be challenging.  Prequel uses a best guess approach over a data sample to infer data types.

Other file formats, such as Avro, contain metadata on primitives, but the number of primitives is nowhere near as comprehensive or complex as a traditional database.  

## Field Types
Prequel defines a field as a combination of a Name and a Column Data Type

```c#
public abstract record Field(string Name, ColumnDataType DataType)
```

Likewise, a "Qualified Field" is a field subclass that belongs to a specific, named table.

```c#
public record QualifiedField(string Name, ColumnDataType DataType, TableReference? Qualifier = null) : Field(Name, DataType)
```

## Type Definitions
**Prequel** defines the following data types:
```c#
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
```

Within the Integer data type, the following magnitudes are used:
```c#
public enum IntegerDataType
{
    Byte,
    Short,
    Integer,
    Long
}
```

## Type Inference
Types in Prequel can be inferred for data sources that do not contain field metadata.  In order to infer data type, the reads a range of records (rows) and runs each value through a series of regular expressions and string parsing tests.  The type inference falls back to a UTF8 string type if none of the tests identify a valid data type.  See the [InferredDataType.cs](../Engine/Prequel.Engine/Data/InferredDataType.cs) implementation.

```c#
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
```

Data sources that contain schema metadata, such as Postgres or MS Sql, provide specific field data types that are mapped into the appropriate Prequel data type.

Mapping specific types to Prequel's primitive types makes it possible to run filter logic and join logic across different data sources.  In other words, having a common type
system allows joining a Postgres boolean column to a CSV file with an inferred boolean column.