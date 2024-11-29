using Prequel.Data;
using Prequel.Logical;
using Prequel.Logical.Expressions;

namespace Prequel.Tests.Data;

public class SchemaTests
{
    [Fact]
    public void Schema_Finds_Unqualified_Fields()
    {
        var schema = new Schema([new("test", ColumnDataType.Integer)]);

        var index = schema.IndexOfColumn(new Column("test"));

        Assert.Equal(0, index);
    }

    [Fact]
    public void Schema_Ignores_Qualified_Field_Null_Relations()
    {
        var schema = new Schema([new("test", ColumnDataType.Integer, new TableReference("table"))]);

        var index = schema.IndexOfColumn(new Column("test"));

        Assert.Equal(0, index);
    }

    [Fact]
    public void Schema_Find_Qualified_Fields()
    {
        var table = new TableReference("table");
        var schema = new Schema([new("test", ColumnDataType.Integer, table)]);

        var index = schema.IndexOfColumn(new Column("test", table));

        Assert.Equal(0, index);
    }

    [Fact]
    public void Schema_Rejects_Unmatched_Qualified_Fields()
    {
        var table = new TableReference("table");
        var table2 = new TableReference("other");
        var schema = new Schema([new("test", ColumnDataType.Integer, table)]);

        var index = schema.IndexOfColumn(new Column("test", table2));

        Assert.Null(index);
    }

    [Fact]
    public void Schema_Gets_Fields_By_Name()
    {
        var schema = new Schema([new("test", ColumnDataType.Integer)]);

        Assert.NotNull(schema.GetField("test"));
    }

    [Fact]
    public void Schema_Gets_Qualified_Fields_By_Name()
    {
        var table = new TableReference("table");
        var schema = new Schema([new("test", ColumnDataType.Integer, table)]);

        Assert.NotNull(schema.GetFieldFromColumn(new Column("test", table)));
    }

    [Fact]
    public void Schema_Rejects_Unmatched_Qualified_Fields_By_Name()
    {
        var table = new TableReference("table");
        var table2 = new TableReference("other");
        var schema = new Schema([new("test", ColumnDataType.Integer, table2)]);

        Assert.Null(schema.GetFieldFromColumn(new Column("test", table)));
    }

    [Fact]
    public void Schema_Queries_Qualified_Fields()
    {
        var table1 = new TableReference("table");
        var table2 = new TableReference("other");
        var schema = new Schema([
            new("first", ColumnDataType.Integer, table1),
            new("first", ColumnDataType.Integer, table2),
            new("third", ColumnDataType.Integer)
        ]);

        var qualified = schema.FieldsWithQualifiedName(table1, "first");

        Assert.Single(qualified);
    }

    [Fact]
    public void Schema_Queries_Unqualified_Fields()
    {
        var table1 = new TableReference("table");
        var table2 = new TableReference("other");
        var schema = new Schema([
            new("first", ColumnDataType.Integer, table1),
            new("first", ColumnDataType.Integer, table2),
            new("second", ColumnDataType.Integer)
        ]);

        var qualified = schema.FieldsWithUnqualifiedName("first");

        Assert.Equal(2, qualified.Count());
    }

    [Fact]
    public void Schema_Joins_Fields()
    {
        var schema1 = new Schema([
            new("first", ColumnDataType.Integer),
            new("second", ColumnDataType.Integer)
        ]);

        var schema2 = new Schema([
            new("first", ColumnDataType.Integer),
            new("third", ColumnDataType.Integer)
        ]);

        var joined = schema1.Join(schema2);

        Assert.Equal(4, joined.Fields.Count);
        // Joining does not force unique names
        Assert.Equal("first", joined.Fields[0].Name);
        Assert.Equal("second", joined.Fields[1].Name);
        Assert.Equal("first", joined.Fields[2].Name);
        Assert.Equal("third", joined.Fields[3].Name);
    }

    [Fact]
    public void Schema_Equality_Compares_Field()
    {
        var schema1 = new Schema([
            new("first", ColumnDataType.Integer)
        ]);

        var schema2 = new Schema([
            new("first", ColumnDataType.Integer)
        ]);


        Assert.Equal(schema1, schema2);

        schema2 = new Schema([
            new("first", ColumnDataType.Utf8)
        ]);

        Assert.NotEqual(schema1, schema2);

        schema2 = new Schema([
            new("second", ColumnDataType.Integer)
        ]);

        Assert.NotEqual(schema1, schema2);
    }

    [Fact]
    public void Schemas_Compare_Hash_Codes()
    {
        var schema1 = new Schema([
            new("first", ColumnDataType.Integer),
            new("second", ColumnDataType.Integer)
        ]);

        var schema2 = new Schema([
            new("first", ColumnDataType.Integer),
            new("second", ColumnDataType.Integer)
        ]);

        Assert.Equal(schema1.GetHashCode(), schema2.GetHashCode());
    }

    [Fact]
    public void Schema_Finds_Field_By_Name()
    {
        var schema = new Schema([
            new("first", ColumnDataType.Integer, new TableReference("table")),
            new("second", ColumnDataType.Integer, new TableReference("table"))
        ]);


        Assert.Equal(1, schema.IndexOfColumnByName(null, "second")!.Value);
        Assert.Equal(1, schema.IndexOfColumnByName(new TableReference("table"), "second")!.Value);
    }

    [Fact]
    public void Schema_Returns_Null_Index_For_Not_Found_Field()
    {
        var schema = new Schema([
            new("first", ColumnDataType.Integer, new TableReference("table")),
            new("second", ColumnDataType.Integer, new TableReference("table"))
        ]);


        Assert.Null(schema.IndexOfColumnByName(null, "third"));
    }

    [Fact]
    public void Schema_Finds_Fields_With_Unqualified_Name()
    {
        var schema = new Schema([new("col", ColumnDataType.Utf8, new TableReference("table"))]);

        Assert.Null(schema.FieldWithUnqualifiedName("missing"));
        Assert.NotNull(schema.FieldWithUnqualifiedName("col"));

        var list = new List<QualifiedField>
        {
            new("col", ColumnDataType.Utf8, new TableReference("table1")),
            new("col", ColumnDataType.Utf8)
        };
        schema = new Schema(list);

        Assert.NotNull(schema.FieldWithUnqualifiedName("col"));

        var fields = new List<QualifiedField>
        {
            new("col", ColumnDataType.Utf8, new TableReference("table1")),
            new("col", ColumnDataType.Utf8, new TableReference("table2"))
        };
        schema = new Schema(fields);

        Assert.Throws<InvalidOperationException>(() => schema.FieldWithUnqualifiedName("col"));
    }

    [Fact]
    public void RecordBatch_Integer_Types_Override_Byte_Array_Default()
    {
        var schema = new Schema([new("Field", ColumnDataType.Integer)]);
        var batch = new RecordBatch(schema);
        Assert.IsType<ByteArray>(batch.Results[0]);

        schema = new Schema([new("Field", ColumnDataType.Integer) { NumericType = IntegerDataType.Short }]);
        batch = new RecordBatch(schema);
        Assert.IsType<ShortArray>(batch.Results[0]);

        schema = new Schema([new("Field", ColumnDataType.Integer) { NumericType = IntegerDataType.Integer }]);
        batch = new RecordBatch(schema);
        Assert.IsType<IntegerArray>(batch.Results[0]);

        schema = new Schema([new("Field", ColumnDataType.Integer) { NumericType = IntegerDataType.Long }]);
        batch = new RecordBatch(schema);
        Assert.IsType<LongArray>(batch.Results[0]);
    }
}