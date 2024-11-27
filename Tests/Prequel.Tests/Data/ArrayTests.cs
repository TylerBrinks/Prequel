using Prequel.Core.Data;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Data;

namespace Prequel.Tests.Data;

public class ArrayTests
{
    [Fact]
    public void StringArray_Adds_Values()
    {
        var array = new StringArray();
        array.Add(true);
        array.Add("true");
        array.Add(1);
        array.Add(false);
        array.Add("false");
        array.Add(0);
        array.Add("abc");
        array.Add(123);
        array.Add(null);

        Assert.Equal(9, array.Values.Count);
        Assert.Equal(8, array.Values.Cast<string?>().Count(_ => !string.IsNullOrWhiteSpace(_)));
        Assert.Equal(1, array.Values.Cast<string?>().Count(string.IsNullOrWhiteSpace));
    }

    [Fact]
    public void StringArray_Fills_Null_Values()
    {
        var array = new StringArray().FillWithNull(5);

        Assert.Equal(5, array.Values.Cast<string?>().Count(_ => _ == null));
    }

    [Fact]
    public void StringArray_Creates_Empty_Arrays()
    {
        var array = new StringArray().NewEmpty(5);

        Assert.Equal(5, array.Values.Cast<string?>().Count(_ => _ == null));
    }

    [Fact]
    public void BooleanArray_Adds_Values()
    {
        var array = new BooleanArray();
        array.Add(true);
        array.Add("true");
        array.Add(1);
        array.Add(false);
        array.Add("false");
        array.Add(0);
        array.Add("abc");
        array.Add(123);
        array.Add(null);

        Assert.Equal(9, array.Values.Count);
        Assert.Equal(2, array.Values.Cast<bool?>().Count(_ => _ != null && _.Value));
        Assert.Equal(2, array.Values.Cast<bool?>().Count(_ => _ != null && !_.Value));
        Assert.Equal(5, array.Values.Cast<bool?>().Count(_ => _ == null));
    }

    [Fact]
    public void BooleanArray_Fills_Null_Values()
    {
        var array = new BooleanArray().FillWithNull(5);

        Assert.Equal(5, array.Values.Cast<bool?>().Count(_ => _ == null));
    }

    [Fact]
    public void BooleanArray_Creates_Empty_Arrays()
    {
        var array = new BooleanArray().NewEmpty(5);

        Assert.Equal(5, array.Values.Cast<bool?>().Count(_ => _ == null));
    }

    [Fact]
    public void ByteArray_Adds_Values()
    {
        var array = new ByteArray();
        array.Add((byte)1);
        array.Add("1");
        array.Add("abc");
        array.Add(123.45);
        array.Add(null);

        Assert.Equal(5, array.Values.Count);
        Assert.Equal(2, array.Values.Cast<byte?>().Count(_ => _ != null));
        Assert.Equal(3, array.Values.Cast<byte?>().Count(_ => _ == null));
    }

    [Fact]
    public void ByteArray_Fills_Null_Values()
    {
        var array = new ByteArray().FillWithNull(5);

        Assert.Equal(5, array.Values.Cast<byte?>().Count(_ => _ == null));
    }

    [Fact]
    public void ByteArray_Creates_Empty_Arrays()
    {
        var array = new ByteArray().NewEmpty(5);

        Assert.Equal(5, array.Values.Cast<byte?>().Count(_ => _ == null));
    }

    [Fact]
    public void ByteArray_Adds_Numeric_Values()
    {
        var array = new ByteArray();
        (array as INumericArray).AddNumeric(1);

        Assert.Single(array.Values);
    }

    [Fact]
    public void ShortArray_Adds_Values()
    {
        var array = new ShortArray();
        array.Add((byte)1);
        array.Add("1");
        array.Add("abc");
        array.Add(123.45);
        array.Add(null);

        Assert.Equal(5, array.Values.Count);
        Assert.Equal(2, array.Values.Cast<short?>().Count(_ => _ != null));
        Assert.Equal(3, array.Values.Cast<short?>().Count(_ => _ == null));
    }

    [Fact]
    public void ShortArray_Fills_Null_Values()
    {
        var array = new ShortArray().FillWithNull(5);

        Assert.Equal(5, array.Values.Cast<short?>().Count(_ => _ == null));
    }

    [Fact]
    public void ShortArray_Creates_Empty_Arrays()
    {
        var array = new ShortArray().NewEmpty(5);

        Assert.Equal(5, array.Values.Cast<byte?>().Count(_ => _ == null));
    }

    [Fact]
    public void ShortArray_Adds_Numeric_Values()
    {
        var array = new ShortArray();
        (array as INumericArray).AddNumeric(1);

        Assert.Single(array.Values);
    }

    [Fact]
    public void IntegerArray_Adds_Values()
    {
        var array = new IntegerArray();
        array.Add(1);
        array.Add("1");
        array.Add("abc");
        array.Add(123.45);
        array.Add(null);

        Assert.Equal(5, array.Values.Count);
        Assert.Equal(2, array.Values.Cast<int?>().Count(_ => _ != null));
        Assert.Equal(3, array.Values.Cast<int?>().Count(_ => _ == null));
    }

    [Fact]
    public void IntegerArray_Fills_Null_Values()
    {
        var array = new IntegerArray().FillWithNull(5);

        Assert.Equal(5, array.Values.Cast<long?>().Count(_ => _ == null));
    }

    [Fact]
    public void IntegerArray_Creates_Empty_Arrays()
    {
        var array = new IntegerArray().NewEmpty(5);

        Assert.Equal(5, array.Values.Cast<long?>().Count(_ => _ == null));
    }

    [Fact]
    public void IntegerArray_Adds_Numeric_Values()
    {
        var array = new IntegerArray();
        (array as INumericArray).AddNumeric(1);

        Assert.Single(array.Values);
    }

    [Fact]
    public void LongArray_Adds_Values()
    {
        var array = new LongArray();
        array.Add((byte)1);
        array.Add("1");
        array.Add("abc");
        array.Add(123.45);
        array.Add(null);

        Assert.Equal(5, array.Values.Count);
        Assert.Equal(2, array.Values.Cast<long?>().Count(_ => _ != null));
        Assert.Equal(3, array.Values.Cast<long?>().Count(_ => _ == null));
    }

    [Fact]
    public void LongArray_Fills_Null_Values()
    {
        var array = new LongArray().FillWithNull(5);

        Assert.Equal(5, array.Values.Cast<long?>().Count(_ => _ == null));
    }

    [Fact]
    public void LongArray_Creates_Empty_Arrays()
    {
        var array = new LongArray().NewEmpty(5);

        Assert.Equal(5, array.Values.Cast<long?>().Count(_ => _ == null));
    }

    [Fact]
    public void LongArray_Adds_Numeric_Values()
    {
        var array = new LongArray();
        (array as INumericArray).AddNumeric(1);

        Assert.Single(array.Values);
    }

    [Fact]
    public void DoubleArray_Adds_Values()
    {
        var array = new DoubleArray();
        array.Add(1);
        array.Add(1.23);
        array.Add("2.34");
        array.Add("abc");
        array.Add(null);

        Assert.Equal(5, array.Values.Count);
        Assert.Equal(3, array.Values.Cast<double?>().Count(_ => _ != null));
        Assert.Equal(2, array.Values.Cast<double?>().Count(_ => _ == null));
    }

    [Fact]
    public void DoubleArray_Fills_Null_Values()
    {
        var array = new DoubleArray().FillWithNull(5);

        Assert.Equal(5, array.Values.Cast<double?>().Count(_ => _ == null));
    }

    [Fact]
    public void DoubleArray_Creates_Empty_Arrays()
    {
        var array = new DoubleArray().NewEmpty(5);

        Assert.Equal(5, array.Values.Cast<double?>().Count(_ => _ == null));
    }

    [Fact]
    public void DateTimeArray_Adds_Values()
    {
        var array = new DateTimeArray();
        array.Add("2020-01-01");
        array.Add(DateTime.Now);
        array.Add(null);

        Assert.Equal(3, array.Values.Count);
        Assert.Equal(1, array.Values.Cast<DateTime?>().Count(_ => _ == null));
    }

    [Fact]
    public void DateTimeArray_Fills_Null_Values()
    {
        var array = new DateTimeArray().FillWithNull(5);

        Assert.Equal(5, array.Values.Cast<bool?>().Count(_ => _ == null));
    }

    [Fact]
    public void DateTimeArray_Creates_Empty_Arrays()
    {
        var array = new DateTimeArray().NewEmpty(5);

        Assert.Equal(5, array.Values.Cast<bool?>().Count(_ => _ == null));
    }

    [Fact]
    public void DateTimeArray_Formats_Times()
    {
        var array = new DateTimeArray();
        var now = DateTime.Now;
        array.Add(now);

        Assert.Equal(now.ToString("s"), array.GetStringValue(0));
    }

    [Fact]
    public void DateTimeArray_Formats_Dates()
    {
        var array = new DateTimeArray();
        var now = DateTime.Parse("2020-04-01");
        array.Add(now);

        Assert.Equal(now.ToString("d"), array.GetStringValue(0));
    }

    [Fact]
    public void TimeStampArray_Adds_Values()
    {
        var array = new TimeStampArray();
        array.Add("2020-01-01");
        array.Add(DateTime.Now);
        array.Add(null);

        Assert.Equal(3, array.Values.Count);
        Assert.Equal(1, array.Values.Cast<DateTime?>().Count(_ => _ == null));
    }

    [Fact]
    public void TimestampArray_Formats_Times()
    {
        var array = new TimeStampArray();
        var now = DateTime.Now;
        array.Add(now);

        Assert.Equal(now.ToString("O"), array.GetStringValue(0));
    }

    [Fact]
    public void TimeStampArray_Fills_Null_Values()
    {
        var array = new TimeStampArray().FillWithNull(5);

        Assert.Equal(5, array.Values.Cast<bool?>().Count(_ => _ == null));
    }

    [Fact]
    public void TimeStampArray_Creates_Empty_Arrays()
    {
        var array = new TimeStampArray().NewEmpty(5);

        Assert.Equal(5, array.Values.Cast<bool?>().Count(_ => _ == null));
    }

    [Fact]
    public void TypedArray_Gets_Sorted_Indices()
    {
        var array = new StringArray();
        array.Add("c");
        array.Add("b");
        array.Add("d");
        array.Add("e");
        array.Add("a");
        var indices = array.GetSortIndices(true);
        Assert.True(new List<int> { 2, 1, 3, 4, 0 }.SequenceEqual(indices));
    }

    [Fact]
    public void TypedArray_Gets_Sorted_Indices_Descending()
    {
        var array = new StringArray();
        array.Add("c");
        array.Add("b");
        array.Add("d");
        array.Add("e");
        array.Add("a");
        var indices = array.GetSortIndices(false);
        Assert.True(new List<int> { 2, 3, 1, 0, 4 }.SequenceEqual(indices));
    }

    [Fact]
    public void TypedArray_Reorders_Lists()
    {
        var array = new StringArray();
        array.Add("a");
        array.Add("b");
        array.Add("c");
        array.Add("d");
        array.Add("e");

        array.Reorder([4, 3, 2, 1, 0]);

        Assert.True(new List<string> { "e", "d", "c", "b", "a"  }.SequenceEqual(array.Values.Cast<string>()));
    }

    [Fact]
    public void TypedArray_Slices_Values()
    {
        var array = new StringArray();
        array.Add("a");
        array.Add("b");
        array.Add("c");
        array.Add("d");
        array.Add("e");

        array.Slice(1, 3);

        Assert.True(new List<string> { "b", "c", "d" }.SequenceEqual(array.Values.Cast<string>()));
    }
}