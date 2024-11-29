using Prequel.Logical;
using Prequel.Logical.Expressions;

namespace Prequel.Data;

public class Schema
{
    private readonly bool _fullyQualified;
    /// <summary>
    /// Creates a new data schema with a set of fields defined in the schema
    /// </summary>
    /// <param name="fields"></param>
    public Schema(List<QualifiedField> fields)
    {
        Fields = fields;
        _fullyQualified = fields.Any(f => f.Qualifier != null);
    }
    /// <summary>
    /// A list of all fields contained in the schema
    /// </summary>
    public List<QualifiedField> Fields { get; }

    /// <summary>
    /// Gets a fully qualified fields from the schema
    /// </summary>
    /// <param name="name">Field name to look up in the schema</param>
    /// <returns>Qualified field if one exists; otherwise null.</returns>
    public QualifiedField? GetField(string name)
    {
        return FieldsWithUnqualifiedName(name).FirstOrDefault();
    }
    /// <summary>
    /// Gets a schema field from a column.  Returns a qualified field if a relation
    /// exists and the schema is qualified.
    /// </summary>
    /// <param name="column">Column containing a name and relation</param>
    /// <returns>Qualified field if the schema and column are qualified; otherwise generic field.</returns>
    internal QualifiedField? GetFieldFromColumn(Column column)
    {
        // Fields may not be qualified for a given schema, so
        // method for looking up fields depends on the type
        // of schema being queried.
        return _fullyQualified && column.Relation != null
            ? FieldsWithQualifiedName(column.Relation, column.Name).FirstOrDefault()
            : GetField(column.Name);
    }
    /// <summary>
    /// Gets the index of a column in the schema
    /// </summary>
    /// <param name="column">Column containing a name to query the schema for the field's index</param>
    /// <returns>Index of the field in the schema</returns>
    internal int? IndexOfColumn(Column column)
    {
        var field = GetFieldFromColumn(column);

        if (field == null)
        {
            return null;
        }

        return Fields.IndexOf(field);
    }
    /// <summary>
    /// Compares schemas for equality
    /// </summary>
    /// <param name="obj">Schema instance</param>
    /// <returns>True if equal; otherwise false</returns>
    public override bool Equals(object? obj)
    {
        return Equals(obj as Schema);
    }
    /// <summary>
    /// Compares schemas for equality
    /// </summary>
    /// <param name="other">Schema instance</param>
    /// <returns>True if equal; otherwise false</returns>
    public bool Equals(Schema? other)
    {
        return other != null && Fields.SequenceEqual(other.Fields);
    }
    /// <summary>
    /// Gets the schema instance's hash code
    /// </summary>
    /// <returns></returns>
    public override int GetHashCode()
    {
        HashCode hash = new();

        foreach (var field in Fields)
        {
            hash.Add(field);
        }

        return hash.ToHashCode();
    }
    /// <summary>
    /// Gets fields that match a given table reference and column name.
    /// </summary>
    /// <param name="qualifier">Table reference to filter schema fields</param>
    /// <param name="columnName">Column name fields must match</param>
    /// <returns>List of qualified fields</returns>
    public IEnumerable<QualifiedField> FieldsWithQualifiedName(TableReference qualifier, string columnName)
    {
        return FieldsWithQualified(qualifier).Where(field => field.Name.Equals(columnName, StringComparison.InvariantCultureIgnoreCase));
    }
    /// <summary>
    /// Gets a that matches a given table reference and column name.
    /// </summary>
    /// <param name="name"></param>
    /// <returns>Qualified field</returns>
    /// <exception cref="InvalidOperationException">Thrown if multiple matches exist</exception>
    internal QualifiedField? FieldWithUnqualifiedName(string name)
    {
        var matches = FieldsWithUnqualifiedName(name).ToList();

        return matches.Count switch
        {
            0 => null,

            1 => matches.First(),

            _ => FindField()
        };

        QualifiedField FindField()
        {
            var fieldsWithoutQualifier = matches.Where(f => f.Qualifier == null).ToList();

            if (fieldsWithoutQualifier.Count == 1)
            {
                return fieldsWithoutQualifier[0];
            }

            throw new InvalidOperationException("Unqualified field not found");
        }
    }
    /// <summary>
    /// Gets fields that match a given table reference and column name.
    /// </summary>
    /// <param name="qualifier"></param>
    /// <returns>List of qualified fields</returns>
    public IEnumerable<QualifiedField> FieldsWithQualified(TableReference qualifier)
    {
        return Fields.Where(f => f.Qualifier != null && f.Qualifier.Name.Equals(qualifier.Name, StringComparison.InvariantCultureIgnoreCase));
    }
    /// <summary>
    /// Gets fields that match a given column name.
    /// </summary>
    /// <param name="columnName"></param>
    /// <returns></returns>
    public IEnumerable<QualifiedField> FieldsWithUnqualifiedName(string columnName)
    {
        return Fields.Where(f => f.Name.Equals(columnName, StringComparison.InvariantCultureIgnoreCase));
    }
    /// <summary>
    /// Combines two schemas by making a union of fields from both schemas and
    /// creating a new, single schema instance.
    /// </summary>
    /// <param name="joinSchema">Schema with fields to combine with the current schema</param>
    /// <returns>New schema instance with combined fields</returns>
    public Schema Join(Schema joinSchema)
    {
        var fields = Fields.ToList().Concat(joinSchema.Fields.ToList()).ToList();
        return new Schema(fields);
    }
    /// <summary>
    /// Checks if the schema has a given field
    /// </summary>
    /// <param name="column">Column with a field name to query</param>
    /// <returns>True if the schema contains the field; otherwise false.</returns>
    internal bool HasColumn(Column column)
    {
        return GetFieldFromColumn(column) != null;
    }
    /// <summary>
    /// Gets the index of a table referenced column
    /// </summary>
    /// <param name="qualifier">Table reference to query</param>
    /// <param name="name">Field name to query</param>
    /// <returns>Index of the column in the schema</returns>
    internal int? IndexOfColumnByName(TableReference? qualifier, string name)
    {
        var matches = Fields.Where(field =>
        {
            if (qualifier != null && field.Qualifier != null)
            {
                return field.Name.Equals(name, StringComparison.InvariantCultureIgnoreCase) &&
                       field.Qualifier.Name.Equals(qualifier.Name, StringComparison.InvariantCultureIgnoreCase);
            }

            if (qualifier != null && field.Qualifier == null)
            {
                var column = Column.FromQualifiedName(field.Name);

                if (column.Relation != null && column.Relation == qualifier)
                {
                    return column.Relation.Name.Equals(qualifier.Name, StringComparison.InvariantCultureIgnoreCase)
                           && column.Name.Equals(name, StringComparison.InvariantCultureIgnoreCase);
                }

                return false;
            }

            return field.Name.Equals(name, StringComparison.InvariantCultureIgnoreCase);

        }).ToList();

        if (!matches.Any())
        {
            return null;
        }

        return Fields.IndexOf(matches.First());
    }
    /// <summary>
    /// Gets a schema fields with a given name.  Returns a qualified field if
    /// the column has a relation; otherwise returns an unqualified field.
    /// </summary>
    /// <param name="column">Column containing field name and optional relation.</param>
    /// <returns>Qualified field</returns>
    internal QualifiedField? FieldWithName(Column column)
    {
        return column.Relation != null
            ? FieldsWithQualifiedName(column.Relation, column.Name).FirstOrDefault()
            : FieldWithUnqualifiedName(column.Name);
    }
}