using System.Collections;
using Prequel.Core.Data;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Data;

namespace Prequel.Tests.Data;

public class RecordBatchTests
{
    [Fact]
    public void RecordBatch_Gets_Array_Types()
    {
        Assert.IsType<StringArray>(RecordBatch.GetArrayType(new QualifiedField("", ColumnDataType.Utf8)));
        Assert.IsType<ByteArray>(RecordBatch.GetArrayType(new QualifiedField("", ColumnDataType.Integer)));
        Assert.IsType<DoubleArray>(RecordBatch.GetArrayType(new QualifiedField("", ColumnDataType.Double)));
        Assert.IsType<BooleanArray>(RecordBatch.GetArrayType(new QualifiedField("", ColumnDataType.Boolean)));
        Assert.IsType<DateTimeArray>(RecordBatch.GetArrayType(new QualifiedField("", ColumnDataType.Date32)));
        Assert.IsType<DateTimeArray>(RecordBatch.GetArrayType(new QualifiedField("", ColumnDataType.TimestampSecond)));
        Assert.IsType<TimeStampArray>(RecordBatch.GetArrayType(new QualifiedField("", ColumnDataType.TimestampNanosecond)));
        Assert.IsType<StringArray>(RecordBatch.GetArrayType(null));
    }

    [Fact]
    public void RecordBatch_Sets_Array_Types()
    {
        var batch = new RecordBatch(new Schema([
            new("", ColumnDataType.Utf8),
            new("", ColumnDataType.Integer),
            new("", ColumnDataType.Double),
            new("", ColumnDataType.Boolean),
            new("", ColumnDataType.Date32)
        ]));

        Assert.IsType<StringArray>(batch.Results[0]);
        Assert.IsType<ByteArray>(batch.Results[1]);
        Assert.IsType<DoubleArray>(batch.Results[2]);
        Assert.IsType<BooleanArray>(batch.Results[3]);
        Assert.IsType<DateTimeArray>(batch.Results[4]);
    }

    [Fact]
    public void RecordBatch_Counts_Rows()
    {
        var batch = new RecordBatch(new Schema([
            new("", ColumnDataType.Utf8)
        ]));

        batch.Results[0].Values.Add("");
        batch.Results[0].Values.Add("");
        batch.Results[0].Values.Add("");

        Assert.Equal(3, batch.RowCount);
    }

    [Fact]
    public void RecordBatch_Reorders_Columns()
    {
        var batch = new RecordBatch(new Schema([
            new("ordered", ColumnDataType.Utf8),
            new("unordered", ColumnDataType.Utf8)
        ]));

        batch.Results[0].Values.Add("c");
        batch.Results[0].Values.Add("b");
        batch.Results[0].Values.Add("a");

        batch.Results[1].Values.Add("c");
        batch.Results[1].Values.Add("b");
        batch.Results[1].Values.Add("a");

        batch.Reorder([2, 1, 0], [1]);

        Assert.Equal("a", batch.Results[0].Values[0]);
        Assert.Equal("b", batch.Results[0].Values[1]);
        Assert.Equal("c", batch.Results[0].Values[2]);
        Assert.Equal("c", batch.Results[1].Values[0]);
        Assert.Equal("b", batch.Results[1].Values[1]);
        Assert.Equal("a", batch.Results[1].Values[2]);
    }

    [Fact]
    public void RecordBatch_Slices_Values()
    {
        var batch = new RecordBatch(new Schema([
            new("", ColumnDataType.Integer),
            new("", ColumnDataType.Integer)
        ]));

        foreach (var array in batch.Results)
        {
            for (var i = 1; i < 6; i++)
            {
                array.Add(i);
            }
        }

        batch.Slice(1, 3);

        Assert.Equal((byte)2, batch.Results[0].Values[0]);
        Assert.Equal((byte)3, batch.Results[0].Values[1]);
        Assert.Equal((byte)4, batch.Results[0].Values[2]);
        Assert.Equal((byte)2, batch.Results[1].Values[0]);
        Assert.Equal((byte)3, batch.Results[1].Values[1]);
        Assert.Equal((byte)4, batch.Results[1].Values[2]);
    }

    [Fact]
    public void RecordBatch_Repartitions()
    {
        var batch = new RecordBatch(new Schema([
            new("", ColumnDataType.Integer),
            new("", ColumnDataType.Integer)
        ]));

        foreach (var array in batch.Results)
        {
            for (var i = 0; i < 10; i++)
            {
                array.Add(i);
            }
        }

        var partitions = batch.Repartition(3).ToList();

        Assert.Equal(3, partitions[0].RowCount);
        Assert.Equal(3, partitions[1].RowCount);
        Assert.Equal(3, partitions[2].RowCount);
        Assert.Equal(1, partitions[3].RowCount);
    }

    [Fact]
    public void RecordBatch_Creates_Instances_With_Data()
    {
        var schema = new Schema([
            new("first", ColumnDataType.Integer),
            new("second", ColumnDataType.Integer)
        ]);

        var columns = new List<object> { 1, 2 };

        var batch = RecordBatch.TryNew(schema, columns!);

        Assert.Equal(2, batch.Results.Count);
        Assert.Equal(1, batch.RowCount);
        Assert.Equal((byte)1, batch.Results[0].Values[0]);
        Assert.Equal((byte)2, batch.Results[1].Values[0]);
    }

    [Fact]
    public void RecordBatch_Creates_Instances_With_List_Data()
    {
        var schema = new Schema([
            new("first", ColumnDataType.Integer),
            new("second", ColumnDataType.Integer)
        ]);

        var lists = new List<IList>
        {
            new List<int> {1, 2, 3},
            new List<int> {4, 5, 6},
        };

        var batch = RecordBatch.TryNewWithLists(schema, lists);

        Assert.Equal(2, batch.Results.Count);
        Assert.Equal(3, batch.RowCount);
        Assert.Equal((byte)1, batch.Results[0].Values[0]);
        Assert.Equal((byte)2, batch.Results[0].Values[1]);
        Assert.Equal((byte)3, batch.Results[0].Values[2]);
        Assert.Equal((byte)4, batch.Results[1].Values[0]);
        Assert.Equal((byte)5, batch.Results[1].Values[1]);
        Assert.Equal((byte)6, batch.Results[1].Values[2]);
    }

    [Fact]
    public void RecordBatch_Concatenates_Batches()
    {
        var schema = new Schema([
            new("first", ColumnDataType.Integer),
            new("second", ColumnDataType.Integer)
        ]);

        var columns1 = new List<object> { 1, 2 };
        var columns2 = new List<object> { 3, 4 };

        var batch1 = RecordBatch.TryNew(schema, columns1!);
        var batch2 = RecordBatch.TryNew(schema, columns2!);
        batch1.Concat(batch2);

        Assert.Equal(2, batch1.Results.Count);
        Assert.Equal(2, batch1.RowCount);
        Assert.Equal((byte)1, batch1.Results[0].Values[0]);
        Assert.Equal((byte)3, batch1.Results[0].Values[1]);
        Assert.Equal((byte)2, batch1.Results[1].Values[0]);
        Assert.Equal((byte)4, batch1.Results[1].Values[1]);
    }

    [Fact]
    public void RecordBatch_Counts_Zero_Rows()
    {
        var schema = new Schema([..new List<QualifiedField> { new("", ColumnDataType.Integer) }]);
        Assert.Equal(0, new RecordBatch(schema).RowCount);
    }

    [Fact]
    public void RecordBath_Prevents_Schema_Mismatch()
    {
        var schema = new Schema([new("name", ColumnDataType.Integer)]);
        Assert.Throws<InvalidOperationException>(() => RecordBatch.TryNew(schema, new List<object?>()));
    }

    [Fact]
    public void RecordBath_Counts_Rows()
    {
        var schema = new Schema([new("name", ColumnDataType.Integer)]);
        var batch = new RecordBatch(schema);

        Assert.Equal(0, batch.RowCount);

        batch.Results[0].Add("");
       
        Assert.Equal(1, batch.RowCount);
    }

    [Fact]
    public void RecordBatch_Projects_Columns_And_Schemas()
    {
        var schema = new Schema(new List<QualifiedField>
        {
            new("0", ColumnDataType.Integer),
            new("1", ColumnDataType.Boolean),
            new("2", ColumnDataType.Boolean),
            new("3", ColumnDataType.Boolean),
            new("4", ColumnDataType.Utf8)
        });
        var batch = new RecordBatch(schema);

        batch.Project([1, 2, 3]);

        Assert.Equal(3, batch.Results.Count);
        Assert.Equal(3, batch.Schema.Fields.Count);

        foreach (var result in batch.Results)
        {
            Assert.IsType<BooleanArray>(result);
        }

        for (var i = 0; i < batch.Schema.Fields.Count; i++)
        {
            var field = batch.Schema.Fields[i];
            Assert.Equal($"{i+1}", field.Name);
            Assert.Equal(ColumnDataType.Boolean, field.DataType);
        }
    }

    [Fact]
    public void RecordBatch_Arrays_Upcast()
    {
        var batch = new RecordBatch(new Schema([
            new("name", ColumnDataType.Integer)
        ]));

        batch.Results[0].Add(1);
    }

    [Fact]
    public void RecordBatch_Upcasts_Numeric_Arrays()
    {
        var schema = new Schema([new("number", ColumnDataType.Integer)]);

        var batch = new RecordBatch(schema);

        batch.AddResult(0, byte.MaxValue);
        Assert.IsType<ByteArray>(batch.Results[0]);

        batch.AddResult(0, short.MaxValue);
        Assert.IsType<ShortArray>(batch.Results[0]);

        batch.AddResult(0, int.MaxValue);
        Assert.IsType<IntegerArray>(batch.Results[0]);

        batch.AddResult(0, long.MaxValue);
        Assert.IsType<LongArray>(batch.Results[0]);
    }

    [Fact]
    public void RecordBatch_Clones_Itself_Without_Schema_Loss()
    {
        var schema = new Schema([
            new("1", ColumnDataType.Integer),
            new("2", ColumnDataType.Integer),
            new("3", ColumnDataType.Integer),
            new("4", ColumnDataType.Integer),
            new("5", ColumnDataType.Integer)
        ]);
       
        var source = new RecordBatch(schema);
        var copy = source.CloneBatch();

        Assert.Equal(source.Schema, copy.Schema);

        copy.Project([1, 3]);

        Assert.NotEqual(source.Schema, copy.Schema);
    }
}