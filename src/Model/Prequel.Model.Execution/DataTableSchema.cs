using Prequel.Engine.Core.Data;

namespace Prequel.Model.Execution;

/// <summary>
/// Maps a data source's schema in the form of data
/// table fields, typically with qualified names
/// </summary>
public class DataTableSchema
{
    public List<DataTableField> Fields { get; set; } = [];

    public Schema ToSchema()
    {
        return new Schema(Fields.OrderBy(f => f.Index).Select(f => f.ToQualifiedField()).ToList());
    }

    public static DataTableSchema FromSchema(Schema schema)
    {
        var tableSchema = new DataTableSchema();

        tableSchema.Fields.AddRange(schema.Fields.Select(DataTableField.FromQualifiedField));

        return tableSchema;
    }
}