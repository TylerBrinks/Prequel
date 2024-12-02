using SqlParser.Ast;
using Prequel.Logical.Expressions;
using Prequel.Logical.Plans;
using Prequel.Logical.Values;
using Prequel.Physical.Expressions;
using Prequel.Data;
using Prequel.Logical;
using Aggregate = Prequel.Logical.Plans.Aggregate;
using Binary = Prequel.Logical.Expressions.Binary;
using Column = Prequel.Logical.Expressions.Column;
using Literal = Prequel.Logical.Expressions.Literal;
using Case = Prequel.Logical.Expressions.Case;

namespace Prequel.Tests.Data;

public class ModelTests
{
    [Fact]
    public void Alias_Overrides_ToString()
    {
       Assert.Equal("name AS alias", new Alias(new Column("name"), "alias").ToString());
    }

    [Fact]
    public void Binary_Overrides_ToString()
    {
        var binary = new Binary(new Column("left"), BinaryOperator.Eq, new Column("right"));
        Assert.Equal("left = right", binary.ToString());
    }

    [Fact]
    public void Column_Overrides_ToString()
    {
        var column = new Column("left");
        Assert.Equal("left", column.ToString());

        column = new Column("column", new TableReference("table"));
        Assert.Equal("table.column", column.ToString());
    }

    [Fact]
    public void ScalarVariable_Overrides_ToString()
    {
        var column = new ScalarVariable(["one", "two", "three"]);
        Assert.Equal("one.two.three", column.ToString());
    }

    [Fact]
    public void Wildcard_Overrides_ToString()
    {
        Assert.Equal("*", new Wildcard().ToString());
    }

    [Fact]
    public void Between_Overrides_ToString()
    {
        Assert.Equal("name Between low And high", new Between(new Column("name"), false, new Column("low"), new Column("high")).ToString());
        Assert.Equal("name Not Between low And high", new Between(new Column("name"), true, new Column("low"), new Column("high")).ToString());
    }

    [Fact]
    public void Union_Overrides_ToString()
    {
        Assert.Equal($"UNION {Environment.NewLine}  Empty Relation", new Union([new EmptyRelation()], new Schema(new List<QualifiedField>())).ToStringIndented());
    }

    [Fact]
    public void Indent_Increases_Spacing()
    {
        var plan = new TestPlan {InnerPlan = new TestPlan()};
        Assert.Equal($"Test Plan{Environment.NewLine}  Test Plan", plan.ToStringIndented());
    }

    [Fact]
    public void Empty_Relation_Builds_Schema()
    {
        var relation = new EmptyRelation();
        Assert.NotNull(relation.Schema);
        Assert.Empty(relation.Schema.Fields);
        Assert.Equal(relation.ToString(), relation.ToStringIndented());
    }

    [Fact]
    public void Distinct_Overrides_ToString()
    {
        var schema = new Schema([new("name", ColumnDataType.Integer)]);
        var distinct = new Distinct(new TestPlan {Schema = schema});
        Assert.Same(schema, distinct.Schema);
        Assert.Equal($"Distinct: {Environment.NewLine}  Test Plan", distinct.ToStringIndented());
    }

    [Fact]
    public void Literal_Overrides_ToString_With_Value()
    {
        var literal = new Literal(new StringScalar("abc"));
        Assert.Equal("'abc'", literal.ToString());
    }

    [Fact]
    public void BooleanScalar_Overrides_ToString_With_Value()
    {
        var scalar = new BooleanScalar(true);
        Assert.Equal("True", scalar.ToString());
    }

    [Fact]
    public void StringScalar_Overrides_ToString_With_Value()
    {
        var scalar = new StringScalar("abc");
        Assert.Equal("abc", scalar.ToString());
    }

    [Fact]
    public void IntegerScalar_Overrides_ToString_With_Value()
    {
        var scalar = new IntegerScalar(123);
        Assert.Equal("123", scalar.ToString());
    }

    [Fact]
    public void DoubleScalar_Overrides_ToString_With_Value()
    {
        var scalar = new DoubleScalar(123);
        Assert.Equal("123", scalar.ToString());
    }

    [Fact]
    public void Aggregate_Overrides_ToString()
    {
        var agg = new Aggregate(new TestPlan(),
            [new Column("group_col")],
            [new Column("agg_col")],
            new Schema([
                new("group_col", ColumnDataType.Integer),
                new("agg_col", ColumnDataType.Integer)
            ]));

        Assert.Equal($"Aggregate: groupBy=[group_col], aggregate=[agg_col]{Environment.NewLine}  Test Plan", agg.ToStringIndented());
    }

    [Fact]
    public void Case_Overrides_ToString()
    {
        var join = new Case(new Column("column"), null!, null);

        Assert.Equal("Case/When", join.ToString());
    }

    [Fact]
    public void Cast_Overrides_ToString()
    {
        var cast = new Cast(new Column("column"), ColumnDataType.Utf8);

        Assert.Equal("Cast(Utf8)", cast.ToString());
    }

    [Fact]
    public void TableScan_Overrides_ToString()
    {
        var schema = new Schema([new("name", ColumnDataType.Utf8)]);
        var scan = new TableScan("table", 
            schema, 
            new EmptyDataTable("", schema, []), 
            []);
          
        Assert.Equal("Table Scan: table projection=()", scan.ToStringIndented());
        Assert.Equal("Table Scan: table projection=()", scan.ToString());
    }

    [Fact]
    public void Filter_Overrides_ToString()
    {
        var filter = new Filter(new TestPlan(), new Column("test"));

        Assert.Equal($"Filter: test{Environment.NewLine}  Test Plan", filter.ToStringIndented());
        Assert.Equal("Filter: test", filter.ToString());
    }

    [Fact]
    public void Limit_Overrides_ToString()
    {
        var limit = new Limit(new TestPlan(), 1, 2);

        Assert.Equal($"Limit: Skip 1, Limit 2{Environment.NewLine}  Test Plan", limit.ToStringIndented());
        Assert.Equal("Limit: Skip 1, Limit 2", limit.ToString());
    }

    [Fact]
    public void SubqueryAlias_Overrides_ToString()
    {
        var schema = new Schema([new("name", ColumnDataType.Utf8)]);
        var alias = new SubqueryAlias(new TestPlan(), schema, "alias");

        Assert.Equal($"Subquery Alias: alias{Environment.NewLine}  Test Plan", alias.ToStringIndented());
        Assert.Equal("Subquery Alias: alias", alias.ToString());
    }

    [Fact]
    public void Join_Overrides_ToString()
    {
        var join = new Prequel.Logical.Plans.Join(
            new TestPlan(), 
            new TestPlan(),
            [],
            null, 
            JoinType.Inner, 
            new Schema([])
            //new JoinConstraint.On(new Expression.LiteralValue(new Value.Number("1")))
            );

        Assert.Equal($"Inner Join:  {Environment.NewLine}  Test Plan{Environment.NewLine}  Test Plan", join.ToStringIndented());
        Assert.Equal("Inner Join: ", join.ToString());
    }

    [Fact]
    public void CrossJoin_Overrides_ToString()
    {
        var join = new CrossJoin(new TestPlan(), new TestPlan());

        Assert.Equal($"Cross Join: {Environment.NewLine}  Test Plan{Environment.NewLine}  Test Plan", join.ToStringIndented());
        Assert.Equal("Cross Join", join.ToString());
    }

    [Fact]
    public void Projection_Overrides_ToString()
    {
        var schema = new Schema([new("name", ColumnDataType.Utf8)]);
        var projection = new Projection(new TestPlan(), [new Column("column")], schema);

        Assert.Equal($"Projection: column {Environment.NewLine}  Test Plan", projection.ToStringIndented());
    }

    [Fact]
    public void PhysicalSortExpression_Proxies_Type_Metadata()
    {
        var sort = new PhysicalSortExpression(null!, null!, null!, false);
        Assert.Throws<NotImplementedException>(() => sort.GetDataType(null!));
        Assert.Throws<NotImplementedException>(() => sort.Evaluate(null!));
    }

    [Fact]
    public void Indentation_Repeats_Indentation()
    {
        var indent = new Indentation();
        var string1 = indent.Repeat(new TestPlan());
        var string2 = indent.Repeat(new TestPlan());
        var string3 = indent.Repeat(new TestPlan());

        Assert.Equal($"{Environment.NewLine}Test Plan", string1);
        Assert.Equal($"{Environment.NewLine}Test Plan", string2);
        Assert.Equal($"{Environment.NewLine}Test Plan", string3);
    }

    [Fact]
    public void Clr_Types_Convert_To_Column_Types()
    {
        Assert.Equal(ColumnDataType.Integer, typeof(sbyte).GetColumnType());
        Assert.Equal(ColumnDataType.Integer, typeof(byte).GetColumnType());
        Assert.Equal(ColumnDataType.Integer, typeof(short).GetColumnType());
        Assert.Equal(ColumnDataType.Integer, typeof(int).GetColumnType());
        Assert.Equal(ColumnDataType.Integer, typeof(long).GetColumnType());
        Assert.Equal(ColumnDataType.Integer, typeof(int).GetColumnType());
        Assert.Equal(ColumnDataType.Integer, typeof(ushort).GetColumnType());
        Assert.Equal(ColumnDataType.Integer, typeof(uint).GetColumnType());
        Assert.Equal(ColumnDataType.Integer, typeof(ulong).GetColumnType());
        
        Assert.Equal(ColumnDataType.Double, typeof(decimal).GetColumnType());
        Assert.Equal(ColumnDataType.Double, typeof(double).GetColumnType());
        Assert.Equal(ColumnDataType.Double, typeof(float).GetColumnType());

        Assert.Equal(ColumnDataType.Boolean, typeof(bool).GetColumnType());
        Assert.Equal(ColumnDataType.TimestampNanosecond, typeof(DateTime).GetColumnType());

        Assert.Equal(ColumnDataType.Utf8, typeof(string).GetColumnType());
    }

    [Fact]
    public void Column_Types_Convert_To_Clr_Types()
    {
        Assert.Equal(typeof(long), ColumnDataType.Integer.GetPrimitiveType());
        Assert.Equal(typeof(double), ColumnDataType.Double.GetPrimitiveType());
        Assert.Equal(typeof(bool), ColumnDataType.Boolean.GetPrimitiveType());
        Assert.Equal(typeof(DateTime), ColumnDataType.Date32.GetPrimitiveType());
        Assert.Equal(typeof(DateTime), ColumnDataType.TimestampSecond.GetPrimitiveType());
        Assert.Equal(typeof(DateTime), ColumnDataType.TimestampNanosecond.GetPrimitiveType());
        Assert.Equal(typeof(DateTime), ColumnDataType.TimestampSecond.GetPrimitiveType());
        Assert.Equal(typeof(string), ColumnDataType.Utf8.GetPrimitiveType());
        Assert.Equal(typeof(string), ColumnDataType.Null.GetPrimitiveType());
    }

    [Fact]
    public void Numbers_Are_Parsed_By_Magnitude()
    {
        string? value = null;
        Assert.False(value.ParseNumeric().IsNumeric);
        Assert.False("".ParseNumeric().IsNumeric);
        Assert.True("0".ParseNumeric().IsNumeric);
        Assert.True("1".ParseNumeric().IsNumeric);
        Assert.True("255".ParseNumeric().IsNumeric); // byte max
        Assert.True("32767".ParseNumeric().IsNumeric); // short max
        Assert.True("2147483647".ParseNumeric().IsNumeric); // int max
        Assert.True("9223372036854775807".ParseNumeric().IsNumeric); // long max

        Assert.Null(value.ParseNumeric().NumericType);
        Assert.Null("".ParseNumeric().NumericType);
        Assert.Equal(typeof(byte), "0".ParseNumeric().NumericType);
        Assert.Equal(typeof(byte), "1".ParseNumeric().NumericType);
        Assert.Equal(typeof(byte), "255".ParseNumeric().NumericType);
        Assert.Equal(typeof(short), "32767".ParseNumeric().NumericType);
        Assert.Equal(typeof(int), "2147483647".ParseNumeric().NumericType);
        Assert.Equal(typeof(long), "9223372036854775807".ParseNumeric().NumericType);
    }
    
    [Fact]
    public void Objects_Compare_Equality()
    {
        object? left = null;

        Assert.True(left.CompareValueEquality(null));
        Assert.False(left.CompareValueEquality((byte)0));

        left = (byte?) 1;
        Assert.False(left.CompareValueEquality(null));
        Assert.True(left.CompareValueEquality((byte)1));
        Assert.True(left.CompareValueEquality((short)1));
        Assert.True(left.CompareValueEquality(1));
        Assert.True(left.CompareValueEquality((long)1));

        left = (short?)1;
        Assert.True(left.CompareValueEquality((byte)1));
        Assert.True(left.CompareValueEquality((short)1));
        Assert.True(left.CompareValueEquality(1));
        Assert.True(left.CompareValueEquality((long)1));

        left = (int?)1;
        Assert.True(left.CompareValueEquality((byte)1));
        Assert.True(left.CompareValueEquality((short)1));
        Assert.True(left.CompareValueEquality(1));
        Assert.True(left.CompareValueEquality((long)1));

        left = (long?)1;
        Assert.True(left.CompareValueEquality((byte)1));
        Assert.True(left.CompareValueEquality((short)1));
        Assert.True(left.CompareValueEquality(1));
        Assert.True(left.CompareValueEquality((long)1));

        left = false;
        Assert.False(left.CompareValueEquality(null));
        Assert.False(left.CompareValueEquality(true));
        Assert.True(left.CompareValueEquality(false));

        left = "test";
        Assert.False(left.CompareValueEquality(null));
        Assert.False(left.CompareValueEquality("abc"));
        Assert.True(left.CompareValueEquality("test"));
    }

    [Fact]
    public void Column_Compare_Equality()
    {
        var column1 = new Column("Name");

        Assert.False(column1.Equals(null));

        var column2 = new Column("Name");

        Assert.True(column1.Equals(column2));

        column1 = new Column("Name", new TableReference("Ref"));

        Assert.False(column1.Equals(column2));

        column2 = new Column("Name", new TableReference("Ref"));

        Assert.True(column1.Equals(column2));
        
        Assert.Equal(column1.GetHashCode(), column2.GetHashCode());
    }

    [Fact]
    public void TableReference_Compare_Equality()
    {
        var table1 = new TableReference("Name");

        Assert.False(table1.Equals(null));

        var table2 = new TableReference("Name");
        
        Assert.True(table1.Equals(table2));

        table1 = new TableReference("Name", "Alias");

        Assert.False(table1.Equals(table2));

        table2 = new TableReference("Name", "Alias");

        Assert.True(table1.Equals(table2));

        Assert.Equal(table1.GetHashCode(), table2.GetHashCode());
    }
}

internal class TestPlan : ILogicalPlan
{
    public Schema? Schema { get; init; }

    public ILogicalPlan? InnerPlan { get; init; }

    public override string ToString()
    {
        return ToStringIndented();
    }

    public string ToStringIndented(Indentation? indentation = null)
    {
        var indent = indentation ?? new Indentation();
        return "Test Plan" + (InnerPlan != null? indent.Next(InnerPlan!) : "");
    }
}
