using Prequel.Tests.Fakes;
using SqlParser.Ast;
using Schema = Prequel.Engine.Core.Data.Schema;
using Exec = Prequel.Engine.Core.Execution.ExecutionContext;
using Prequel.Engine.Core.Logical.Values;
using Prequel.Engine.Core.Physical.Expressions;
using Prequel.Engine.Core.Execution;
using Prequel.Engine.Core.Data;

namespace Prequel.Tests.Physical;

public class ExecutionTests
{
    private readonly Exec _context;

    public ExecutionTests()
    {
        _context = new Exec();

        var schema = new Schema(
        [
            new("a", ColumnDataType.Integer),
            new("b", ColumnDataType.Utf8),
            new("c", ColumnDataType.Double)
        ]);


        var memDb = new FakeDataTable("db", schema, [0, 1, 2]);
        _context.RegisterDataTable(memDb);

        var joinSchema = new Schema(
        [
            new("c", ColumnDataType.Integer),
            new("d", ColumnDataType.Utf8),
            new("e", ColumnDataType.Double)
        ]);

        var joinDb = new FakeDataTable("join", joinSchema, [0, 1, 2]);
        _context.RegisterDataTable(joinDb);
    }

    [Fact]
    public void Anti_Indices_Are_Inverse_Indices()
    {
        var anti = new long[] {0, 3}.GetAntiIndices(4);

        Assert.Equal(1, anti[0]);
        Assert.Equal(2, anti[1]);
    }

    [Fact]
    public void Semi_Indices_Are_Not_Inverse_Indices()
    {
        var anti = new long[] { 0, 3 }.GetSemiIndices(4);

        Assert.Equal(0, anti[0]);
        Assert.Equal(3, anti[1]);
    }

    [Fact]
    public void Filter_Execution_Requires_Boolean_Types()
    {
        var schema = new Schema([new("", ColumnDataType.Integer)]);

        Assert.Throws<InvalidOperationException>(() =>
            FilterExecution.TryNew(new Literal(new IntegerScalar(1)), new FakeTableExecution(schema, null!)));
    }

    [Fact]
    public void Binary_Adds_Values()
    {
        var binary = new Binary(new Literal(new IntegerScalar(2)), BinaryOperator.Plus, new Literal(new IntegerScalar(3)));
        var batch = new RecordBatch(new Schema([new ("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        var value = binary.Evaluate(batch);

        Assert.Equal(5L, value.GetValue(0));
    }

    [Fact]
    public void Binary_Subtracts_Values()
    {
        var binary = new Binary(new Literal(new IntegerScalar(7)), BinaryOperator.Minus, new Literal(new IntegerScalar(3)));
        var batch = new RecordBatch(new Schema([new("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        var value = binary.Evaluate(batch);

        Assert.Equal(4L, value.GetValue(0));
    }

    [Fact]
    public void Binary_Multiplies_Values()
    {
        var binary = new Binary(new Literal(new IntegerScalar(3)), BinaryOperator.Multiply, new Literal(new IntegerScalar(4)));
        var batch = new RecordBatch(new Schema([new("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        var value = binary.Evaluate(batch);

        Assert.Equal(12L, value.GetValue(0));
    }

    [Fact]
    public void Binary_Divides_Values()
    {
        var binary = new Binary(new Literal(new IntegerScalar(10)), BinaryOperator.Divide, new Literal(new IntegerScalar(5)));
        var batch = new RecordBatch(new Schema([new("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        var value = binary.Evaluate(batch);

        Assert.Equal(2L, value.GetValue(0));
    }

    [Fact]
    public void Binary_Calculates_Modulo()
    {
        var binary = new Binary(new Literal(new IntegerScalar(10)), BinaryOperator.Modulo, new Literal(new IntegerScalar(6)));
        var batch = new RecordBatch(new Schema([new("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        var value = binary.Evaluate(batch);

        Assert.Equal(4L, value.GetValue(0));
    }

    [Fact]
    public void Binary_Compares_Greater_Than()
    {
        var binary = new Binary(new Literal(new IntegerScalar(10)), BinaryOperator.Gt, new Literal(new IntegerScalar(5)));
        var batch = new RecordBatch(new Schema([new("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        var value = binary.Evaluate(batch);

        Assert.True((bool)value.GetValue(0)!);
        
        binary = new Binary(new Literal(new IntegerScalar(5)), BinaryOperator.Gt, new Literal(new IntegerScalar(10)));
        batch = new RecordBatch(new Schema([new("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        value = binary.Evaluate(batch);

        Assert.False((bool)value.GetValue(0)!);
    }

    [Fact]
    public void Binary_Compares_Greater_ThanEqual()
    {
        var binary = new Binary(new Literal(new IntegerScalar(10)), BinaryOperator.GtEq, new Literal(new IntegerScalar(5)));
        var batch = new RecordBatch(new Schema([new("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        var value = binary.Evaluate(batch);

        Assert.True((bool)value.GetValue(0)!);

        binary = new Binary(new Literal(new IntegerScalar(10)), BinaryOperator.GtEq, new Literal(new IntegerScalar(10)));
        batch = new RecordBatch(new Schema([new("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        value = binary.Evaluate(batch);

        Assert.True((bool)value.GetValue(0)!);

        binary = new Binary(new Literal(new IntegerScalar(5)), BinaryOperator.GtEq, new Literal(new IntegerScalar(10)));
        batch = new RecordBatch(new Schema([new("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        value = binary.Evaluate(batch);

        Assert.False((bool)value.GetValue(0)!);
    }

    [Fact]
    public void Binary_Compares_Less_Than()
    {
        var binary = new Binary(new Literal(new IntegerScalar(5)), BinaryOperator.Lt, new Literal(new IntegerScalar(10)));
        var batch = new RecordBatch(new Schema([new("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        var value = binary.Evaluate(batch);

        Assert.True((bool)value.GetValue(0)!);

        binary = new Binary(new Literal(new IntegerScalar(10)), BinaryOperator.Lt, new Literal(new IntegerScalar(5)));
        batch = new RecordBatch(new Schema([new("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        value = binary.Evaluate(batch);

        Assert.False((bool)value.GetValue(0)!);
    }

    [Fact]
    public void Binary_Compares_Less_ThanEqual()
    {
        var binary = new Binary(new Literal(new IntegerScalar(5)), BinaryOperator.LtEq, new Literal(new IntegerScalar(10)));
        var batch = new RecordBatch(new Schema([new("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        var value = binary.Evaluate(batch);

        Assert.True((bool)value.GetValue(0)!);

        binary = new Binary(new Literal(new IntegerScalar(5)), BinaryOperator.LtEq, new Literal(new IntegerScalar(5)));
        batch = new RecordBatch(new Schema([new("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        value = binary.Evaluate(batch);

        Assert.True((bool)value.GetValue(0)!);

        binary = new Binary(new Literal(new IntegerScalar(10)), BinaryOperator.LtEq, new Literal(new IntegerScalar(5)));
        batch = new RecordBatch(new Schema([new("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        value = binary.Evaluate(batch);

        Assert.False((bool)value.GetValue(0)!);
    }

    [Fact]
    public void Binary_Compares_Not_Equal()
    {
        var binary = new Binary(new Literal(new IntegerScalar(10)), BinaryOperator.NotEq, new Literal(new IntegerScalar(5)));
        var batch = new RecordBatch(new Schema([new("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        var value = binary.Evaluate(batch);

        Assert.True((bool)value.GetValue(0)!);

        binary = new Binary(new Literal(new IntegerScalar(5)), BinaryOperator.NotEq, new Literal(new IntegerScalar(5)));
        batch = new RecordBatch(new Schema([new("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        value = binary.Evaluate(batch);

        Assert.False((bool)value.GetValue(0)!);
    }

    [Fact]
    public void Binary_Compares_And()
    {
        var binary = new Binary(new Literal(new BooleanScalar(true)), BinaryOperator.And, new Literal(new BooleanScalar(true)));
        var batch = new RecordBatch(new Schema([new("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        var value = binary.Evaluate(batch);

        Assert.True((bool)value.GetValue(0)!);

        binary = new Binary(new Literal(new BooleanScalar(true)), BinaryOperator.And, new Literal(new BooleanScalar(false)));
        batch = new RecordBatch(new Schema([new("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        value = binary.Evaluate(batch);

        Assert.False((bool)value.GetValue(0)!);

        binary = new Binary(new Literal(new BooleanScalar(false)), BinaryOperator.And, new Literal(new BooleanScalar(true)));
        batch = new RecordBatch(new Schema([new("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        value = binary.Evaluate(batch);

        Assert.False((bool)value.GetValue(0)!);
    }

    [Fact]
    public void Binary_Compares_Or()
    {
        var binary = new Binary(new Literal(new BooleanScalar(true)), BinaryOperator.Or, new Literal(new BooleanScalar(true)));
        var batch = new RecordBatch(new Schema([new("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        var value = binary.Evaluate(batch);

        Assert.True((bool)value.GetValue(0)!);

        binary = new Binary(new Literal(new BooleanScalar(true)), BinaryOperator.Or, new Literal(new BooleanScalar(false)));
        batch = new RecordBatch(new Schema([new("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        value = binary.Evaluate(batch);

        Assert.True((bool)value.GetValue(0)!);

        binary = new Binary(new Literal(new BooleanScalar(false)), BinaryOperator.Or, new Literal(new BooleanScalar(true)));
        batch = new RecordBatch(new Schema([new("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        value = binary.Evaluate(batch);

        Assert.True((bool)value.GetValue(0)!);

        binary = new Binary(new Literal(new BooleanScalar(false)), BinaryOperator.Or, new Literal(new BooleanScalar(false)));
        batch = new RecordBatch(new Schema([new("value", ColumnDataType.Integer)]));
        batch.Results[0].Add(0);

        value = binary.Evaluate(batch);

        Assert.False((bool)value.GetValue(0)!);
    }
}