using Prequel.Logical;
using Prequel.Data;

namespace Prequel.Tests.Data;

public class FieldTests
{
    [Fact]
    public void Fields_Creates_Columns()
    {
        var field = new QualifiedField("name", ColumnDataType.Integer);
        var column = field.QualifiedColumn();

        Assert.Equal("name", column.Name);
        Assert.Null(column.Relation);
    }

    [Fact]
    public void Fields_Creates_Qualified_Columns()
    {
        var field = new QualifiedField("name", ColumnDataType.Integer, new TableReference("table"));
        var column = field.QualifiedColumn();

        Assert.Equal("name", column.Name);
        Assert.NotNull(column.Relation);
    }

    [Fact]
    public void Fields_Creates_Unqualified_Columns()
    {
        var column = QualifiedField.Unqualified("name", ColumnDataType.Integer);

        Assert.Equal("name", column.Name);
        Assert.Null(column.Qualifier);
    }

    [Fact]
    public void Fields_Creates_Qualified_Columns_From_Relations()
    {
        var field = new QualifiedField("name", ColumnDataType.Integer);
        var column = field.FromQualified(new TableReference("table"));

        Assert.Equal("name", column.Name);
        Assert.NotNull(column.Qualifier);
    }

    [Fact]
    public void Fields_Overrides_ToString()
    {
        var qualifiedField = new QualifiedField("name", ColumnDataType.Integer, new TableReference("table"));
        Assert.Equal("table.name::Integer", qualifiedField.ToString());

        var field = (Field) qualifiedField;
        Assert.Equal("table.name::Integer", field.ToString());

        qualifiedField = new QualifiedField("name", ColumnDataType.Integer);
        Assert.Equal("name::Integer", qualifiedField.ToString());

        field = qualifiedField;
        Assert.Equal("name::Integer", field.ToString());
    }
}