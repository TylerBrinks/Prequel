using Prequel.Logical;
using Prequel.Logical.Expressions;

namespace Prequel.Data;

/// <summary>
/// Represents a field in a data source's schema
/// </summary>
/// <param name="Name">Field name</param>
/// <param name="DataType">Field data type</param>
public abstract record Field(string Name, ColumnDataType DataType)
{
    public IntegerDataType? NumericType { get; internal set; } = null;
}

/// <summary>
/// Field with a fully qualified name
/// </summary>
/// <param name="Name">Field name</param>
/// <param name="DataType">Field data type</param>
/// <param name="Qualifier">Reference to the field's table if related to a known table</param>
public record QualifiedField(string Name, ColumnDataType DataType, TableReference? Qualifier = null) : Field(Name, DataType)
{
    internal Column QualifiedColumn()
    {
        return new Column(Name, Qualifier);
    }
    /// <summary>
    /// Gets the field's fully qualified name
    /// </summary>
    internal string QualifiedName
    {
        get
        {
            var qualifier = Qualifier != null ? $"{Qualifier.Name}." : string.Empty;
            return $"{qualifier}{Name}";
        }
    }
    /// <summary>
    /// Get the unqualified field without table reference
    /// </summary>
    /// <param name="name"></param>
    /// <param name="dataType"></param>
    /// <returns></returns>
    public static QualifiedField Unqualified(string name, ColumnDataType dataType)
    {
        return new QualifiedField(name, dataType);
    }

    public override string ToString()
    {
        var qualifier = Qualifier != null ? $"{Qualifier}." : "";
        return $"{qualifier}{Name}::{DataType}";
    }
    /// <summary>
    /// Clones a field with a new table qualifier
    /// </summary>
    /// <param name="qualifier">able reference</param>
    /// <returns>Closed qualified field</returns>
    internal QualifiedField FromQualified(TableReference qualifier)
    {
        return new QualifiedField(Name, DataType, qualifier);
    }
}