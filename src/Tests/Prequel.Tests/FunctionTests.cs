using Prequel.Data;
using Prequel.Logical.Values;
using Prequel.Physical.Expressions;
using Prequel.Physical.Functions;
using Prequel.Physical.Aggregation;

namespace Prequel.Tests;

public class FunctionTests
{
    [Fact]
    public void Count_Function_Defaults()
    {
        var expression = new Literal(new StringScalar("abc"));
        var fn = new CountFunction(expression, "test", ColumnDataType.Utf8);
        Assert.Single(fn.StateFields);
        Assert.Equal("test", fn.StateFields.First().Name);
        Assert.Equal(ColumnDataType.Utf8, fn.StateFields.First().DataType);
        Assert.Equal(QualifiedField.Unqualified("test", ColumnDataType.Utf8), fn.NamedQualifiedField);
        Assert.Equal(QualifiedField.Unqualified("test", ColumnDataType.Utf8), fn.NamedQualifiedField);
        Assert.Equal(expression, fn.Expressions.First());
    }

    [Fact]
    public void Count_Builds_Accumulator()
    {
        var expression = new Literal(new StringScalar("abc"));
        var fn = new CountFunction(expression, "test", ColumnDataType.Utf8);
        Assert.IsType<CountAccumulator>(fn.CreateAccumulator());
    }

    [Fact]
    public void Max_Function_Defaults()
    {
        var expression = new Literal(new StringScalar("abc"));
        var fn = new MaxFunction(expression, "test", ColumnDataType.Utf8);
        Assert.Single(fn.StateFields);
        Assert.Equal("test", fn.StateFields.First().Name);
        Assert.Equal(ColumnDataType.Utf8, fn.StateFields.First().DataType);
        Assert.Equal(QualifiedField.Unqualified("test", ColumnDataType.Utf8), fn.NamedQualifiedField);
        Assert.Equal(QualifiedField.Unqualified("test", ColumnDataType.Utf8), fn.NamedQualifiedField);
        Assert.Equal(expression, fn.Expressions.First());
    }

    [Fact]
    public void Max_Builds_Accumulator()
    {
        var expression = new Literal(new StringScalar("abc"));
        var fn = new MaxFunction(expression, "test", ColumnDataType.Utf8);
        Assert.IsType<MaxAccumulator>(fn.CreateAccumulator());
    }

    [Fact]
    public void Min_Function_Defaults()
    {
        var expression = new Literal(new StringScalar("abc"));
        var fn = new MinFunction(expression, "test", ColumnDataType.Utf8);
        Assert.Single(fn.StateFields);
        Assert.Equal("test", fn.StateFields.First().Name);
        Assert.Equal(ColumnDataType.Utf8, fn.StateFields.First().DataType);
        Assert.Equal(QualifiedField.Unqualified("test", ColumnDataType.Utf8), fn.NamedQualifiedField);
        Assert.Equal(QualifiedField.Unqualified("test", ColumnDataType.Utf8), fn.NamedQualifiedField);
        Assert.Equal(expression, fn.Expressions.First());
    }

    [Fact]
    public void Aggregates_Require_Overridden_Column_Types()
    {
        var expression = new Literal(new StringScalar("abc"));
        var fn = new MinFunction(expression, "test", ColumnDataType.Utf8);
        Assert.Throws<InvalidOperationException>(() => fn.GetDataType(new Schema([])));
    }

    [Fact]
    public void Min_Builds_Accumulator()
    {
        var expression = new Literal(new StringScalar("abc"));
        var fn = new MinFunction(expression, "test", ColumnDataType.Utf8);
        Assert.IsType<MinAccumulator>(fn.CreateAccumulator());
    }

    [Fact]
    public void Sum_Function_Defaults()
    {
        var expression = new Literal(new StringScalar("abc"));
        var fn = new SumFunction(expression, "test", ColumnDataType.Utf8);
        Assert.Single(fn.StateFields);
        Assert.Equal("test", fn.StateFields.First().Name);
        Assert.Equal(ColumnDataType.Utf8, fn.StateFields.First().DataType);
        Assert.Equal(QualifiedField.Unqualified("test", ColumnDataType.Utf8), fn.NamedQualifiedField);
        Assert.Equal(QualifiedField.Unqualified("test", ColumnDataType.Utf8), fn.NamedQualifiedField);
        Assert.Equal(expression, fn.Expressions.First());
    }

    [Fact]
    public void Sum_Builds_Accumulator()
    {
        var expression = new Literal(new StringScalar("abc"));
        var fn = new SumFunction(expression, "test", ColumnDataType.Utf8);
        Assert.IsType<SumAccumulator>(fn.CreateAccumulator());
    }

    [Fact]
    public void Average_Function_Defaults()
    {
        var expression = new Literal(new StringScalar("abc"));
        var fn = new AverageFunction(expression, "test", ColumnDataType.Utf8);
        Assert.Single(fn.StateFields);
        Assert.Equal("test", fn.StateFields.First().Name);
        Assert.Equal(ColumnDataType.Utf8, fn.StateFields.First().DataType);
        Assert.Equal(QualifiedField.Unqualified("test", ColumnDataType.Utf8), fn.NamedQualifiedField);
        Assert.Equal(QualifiedField.Unqualified("test", ColumnDataType.Utf8), fn.NamedQualifiedField);
        Assert.Equal(expression, fn.Expressions.First());
    }

    [Fact]
    public void Average_Builds_Accumulator()
    {
        var expression = new Literal(new StringScalar("abc"));
        var fn = new AverageFunction(expression, "test", ColumnDataType.Utf8);
        Assert.IsType<AverageAccumulator>(fn.CreateAccumulator());
    }
}