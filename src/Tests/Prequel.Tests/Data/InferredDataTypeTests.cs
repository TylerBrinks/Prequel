using Prequel.Core.Data;
using Prequel.Data;

namespace Prequel.Tests.Data;

public class InferredDataTypeTests
{
    [Fact]
    public void InferredDataType_Defaults_Quoted_UTF8()
    {
        var inference = new InferredDataType();
        inference.Update("\"test\"");

        Assert.Equal(ColumnDataType.Utf8, inference.DataType);
    }

    [Fact]
    public void InferredDataType_Defaults_Unknown_UTF8()
    {
        var inference = new InferredDataType();
        inference.Update("unknown");

        Assert.Equal(ColumnDataType.Utf8, inference.DataType);
    }

    [Fact]
    public void InferredDataType_Identifies_Boolean_Values()
    {
        var inference = new InferredDataType();
        inference.Update("true");

        Assert.Equal(ColumnDataType.Boolean, inference.DataType);
    }

    [Fact]
    public void InferredDataType_Identifies_Integer_Values()
    {
        var inference = new InferredDataType();
        inference.Update("1");

        Assert.Equal(ColumnDataType.Integer, inference.DataType);
    }

    [Fact]
    public void InferredDataType_Identifies_Double_Values()
    {
        var inference = new InferredDataType();
        inference.Update("1.2");

        Assert.Equal(ColumnDataType.Double, inference.DataType);
    }

    [Fact]
    public void InferredDataType_Identifies_Date_Values()
    {
        var inference = new InferredDataType();
        inference.Update("2001-02-03");

        Assert.Equal(ColumnDataType.Date32, inference.DataType);
    }

    [Fact]
    public void InferredDataType_Identifies_Second_Values()
    {
        var inference = new InferredDataType();
        inference.Update("2001-02-03T11:22:33");

        Assert.Equal(ColumnDataType.TimestampSecond, inference.DataType);
    }

    [Fact]
    public void InferredDataType_Identifies_Millisecond_Values()
    {
        var inference = new InferredDataType();
        inference.Update("2001-02-03T11:22:33.123");

        Assert.Equal(ColumnDataType.TimestampNanosecond, inference.DataType);
    }

    [Fact]
    public void InferredDataType_Identifies_Microsecond_Values()
    {
        var inference = new InferredDataType();
        inference.Update("2001-02-03T11:22:33.123456");

        Assert.Equal(ColumnDataType.TimestampNanosecond, inference.DataType);
    }

    [Fact]
    public void InferredDataType_Identifies_Nanosecond_Values()
    {
        var inference = new InferredDataType();
        inference.Update("2001-02-03T11:22:33.123456789");

        Assert.Equal(ColumnDataType.TimestampNanosecond, inference.DataType);
    }

    [Fact]
    public void InferredDataType_Overrides_ToString()
    {
        var inference = new InferredDataType();
        inference.Update("123");
        Assert.Equal("Integer", inference.ToString());
    }

    [Fact]
    public void InferredDataType_Does_Not_Regress_For_Dates()
    {
        var inference = new InferredDataType();
        inference.Update("2001-02-03T11:22:33.123456789");
        inference.Update("2001-02-03T11:22:33");

        Assert.Equal(ColumnDataType.TimestampNanosecond, inference.DataType);
    }

    [Fact]
    public void InferredDataType_Does_Not_Regress_For_Date_Strings()
    {
        var inference = new InferredDataType();
        inference.Update("Wed, 1 Jan 2020 06:21:15 GMT");
        inference.Update("Wed, 1 Jan 2020");

        Assert.Equal(ColumnDataType.TimestampSecond, inference.DataType);
    }
}