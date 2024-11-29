using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Logical;

namespace Prequel.Model.Execution;

public class DataTableField
{
    public required string Name { get; set; }
    public required int Index { get; set; }
    public required ColumnDataType DataType { get; set; }
    public string? ReferenceName { get; set; }
    public string? ReferenceAlias { get; set; }

    /// <summary>
    /// Converts a data table field to a schema qualified field
    /// </summary>
    /// <returns>QualifiedField instance</returns>
    public QualifiedField ToQualifiedField()
    {
        TableReference? reference = null;

        if (ReferenceName != null)
        {
            reference = new TableReference(ReferenceName, ReferenceAlias);
        }

        return new QualifiedField(Name, DataType, reference);
    }

    /// <summary>
    /// Converts a qualified field to a data table field model instance
    /// </summary>
    /// <param name="field">Qualified field to convert</param>
    /// <param name="index">Field index</param>
    /// <returns>DataTableField instance</returns>
    public static DataTableField FromQualifiedField(QualifiedField field, int index)
    {
        string? referenceName = null;
        string? referenceAlias = null;

        if (field.Qualifier != null)
        {
            referenceName = field.Qualifier.Name;
            referenceAlias = field.Qualifier.Alias;
        }

        return new DataTableField
        {
            Name = field.Name,
            Index = index,
            DataType = field.DataType,
            ReferenceName = referenceName,
            ReferenceAlias = referenceAlias
        };
    }
}