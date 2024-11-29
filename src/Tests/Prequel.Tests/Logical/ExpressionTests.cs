using Prequel.Logical;
using Prequel.Logical.Expressions;
using Prequel.Logical.Values;
using Prequel.Data;
using SqlParser.Ast;

namespace Prequel.Tests.Logical;

public class ExpressionTests
{
    [Fact]
    public void Column_Flattens_Name()
    {
        var column = new Column("column", new TableReference("table"));

        Assert.Equal("column", column.Name);
        Assert.Equal("table.column", column.FlatName);
    }

    [Fact]
    public void AggregateFunction_Gets_Function_From_Keywords()
    {
        Assert.Equal(AggregateFunctionType.Min, AggregateFunction.GetFunctionType("min"));
        Assert.Equal(AggregateFunctionType.Max, AggregateFunction.GetFunctionType("max"));
        Assert.Equal(AggregateFunctionType.Count, AggregateFunction.GetFunctionType("count"));
        Assert.Equal(AggregateFunctionType.Avg, AggregateFunction.GetFunctionType("avg"));
        Assert.Equal(AggregateFunctionType.Avg, AggregateFunction.GetFunctionType("mean"));
        Assert.Equal(AggregateFunctionType.Sum, AggregateFunction.GetFunctionType("sum"));
        Assert.Equal(AggregateFunctionType.Median, AggregateFunction.GetFunctionType("median"));
        Assert.Equal(AggregateFunctionType.Variance, AggregateFunction.GetFunctionType("var"));
        Assert.Equal(AggregateFunctionType.Variance, AggregateFunction.GetFunctionType("var_samp"));
        Assert.Equal(AggregateFunctionType.VariancePop, AggregateFunction.GetFunctionType("var_pop"));
        Assert.Equal(AggregateFunctionType.StdDev, AggregateFunction.GetFunctionType("stddev"));
        Assert.Equal(AggregateFunctionType.StdDev, AggregateFunction.GetFunctionType("stddev_samp"));
        Assert.Equal(AggregateFunctionType.StdDevPop, AggregateFunction.GetFunctionType("stddev_pop"));
        Assert.Equal(AggregateFunctionType.Covariance, AggregateFunction.GetFunctionType("covar"));
        Assert.Equal(AggregateFunctionType.Covariance, AggregateFunction.GetFunctionType("covar_samp"));
        Assert.Equal(AggregateFunctionType.CovariancePop, AggregateFunction.GetFunctionType("covar_pop"));
    }

    [Fact]
    public void AggregateFunction_Equality_Compares_Properties()
    {
        var fn1 = new AggregateFunction(AggregateFunctionType.Avg, new (){new Column("name")}, false, new Column("abc"));
        Assert.NotNull(fn1);

        var fn2 = new AggregateFunction(AggregateFunctionType.Min, new (){new Column("nope")}, true, new Literal(new StringScalar("xyz")));

        Assert.NotEqual(fn1, fn2);

        fn2 = new AggregateFunction(AggregateFunctionType.Min, new() { new Column("nope") }, true, new Column("abc"));
        Assert.NotEqual(fn1, fn2);

        fn2 = new AggregateFunction(AggregateFunctionType.Min, new() { new Column("nope") }, false, new Column("abc"));
        Assert.NotEqual(fn1, fn2);

        fn2 = new AggregateFunction(AggregateFunctionType.Min, new() { new Column("name") }, false, new Column("abc"));
        Assert.NotEqual(fn1, fn2);

        fn2 = new AggregateFunction(AggregateFunctionType.Avg, new() { new Column("name") }, false, null);
        Assert.NotEqual(fn1, fn2);
       
        fn2 = new AggregateFunction(AggregateFunctionType.Avg, new() { new Column("name") }, false, new Column("abc"));
        Assert.Equal(fn1, fn2);
    }

    [Fact]
    public void OrderBy_Overrides_ToString()
    {
        var order = new Prequel.Logical.Expressions.OrderBy(new Column("column"), false);

        Assert.Equal("Order By column Desc", order.ToString());

        order = new Prequel.Logical.Expressions.OrderBy(new Column("column"),true);

        Assert.Equal("Order By column Asc", order.ToString());
    }

    [Fact]
    public void OuterReference_Overrides_ToString()
    {
        var reference = new OuterReferenceColumn(ColumnDataType.Integer, new Column("name"));
        Assert.Equal("outer_ref(name)", reference.ToString());
    }
    
    [Fact]
    public void ScalarSubquery_Overrides_ToString()
    {
        var reference = new ScalarSubquery(new Data.TestPlan(), [new Column("name")]);
        Assert.Equal("Subquery(Test Plan)", reference.ToString());
    }

    [Fact]
    public void InList_Overrides_ToString()
    {
        var @in = new InList(new Column("name"), [], false);
        Assert.Equal("In List", @in.ToString());

        @in = new InList(new Column("name"), [], true);
        Assert.Equal("Not In List", @in.ToString());
    }

    [Fact]
    public void Like_Overrides_ToString()
    {
        var like = new Like(false, new Column("name"), new Column("name"), '_', false);
        Assert.Equal("Like", like.ToString());

        like = new Like(true, new Column("name"), new Column("name"), '_', false);
        Assert.Equal("Not Like", like.ToString());

        like = new Like(false, new Column("name"), new Column("name"), '_', true);
        Assert.Equal("ILike", like.ToString());

        like = new Like(true, new Column("name"), new Column("name"), '_', true);
        Assert.Equal("Not ILike", like.ToString());
    }

    [Fact]
    public void Expressions_Maps_Children()
    {
        var column = new Column("column");
        ILogicalExpression alias = new Alias(column, "alias");

        ILogicalExpression Transformation(ILogicalExpression node) => column;

        var mapped = alias.Transform(alias, Transformation);

        Assert.Same(column, mapped);
    }

    [Fact]
    public void Operators_Display_Friendly_Text()
    {
        Assert.Equal("+",  BinaryOperator.Plus.GetDisplayText());
        Assert.Equal("-",  BinaryOperator.Minus.GetDisplayText());
        Assert.Equal("*",  BinaryOperator.Multiply.GetDisplayText());
        Assert.Equal("/",  BinaryOperator.Divide.GetDisplayText());
        Assert.Equal("%",  BinaryOperator.Modulo.GetDisplayText());
        Assert.Equal(">",  BinaryOperator.Gt.GetDisplayText());
        Assert.Equal("<",  BinaryOperator.Lt.GetDisplayText());
        Assert.Equal(">=",  BinaryOperator.GtEq.GetDisplayText());
        Assert.Equal("<=",  BinaryOperator.LtEq.GetDisplayText());
        Assert.Equal("<=>",  BinaryOperator.Spaceship.GetDisplayText());
        Assert.Equal("=",  BinaryOperator.Eq.GetDisplayText());
        Assert.Equal("!=",  BinaryOperator.NotEq.GetDisplayText());
        Assert.Equal("AND",  BinaryOperator.And.GetDisplayText());
        Assert.Equal("OR",  BinaryOperator.Or.GetDisplayText());
        Assert.Equal("XOR",  BinaryOperator.Xor.GetDisplayText());
        Assert.Equal("|",  BinaryOperator.BitwiseOr.GetDisplayText());
        Assert.Equal("&",  BinaryOperator.BitwiseAnd.GetDisplayText());
        Assert.Equal("^", BinaryOperator.BitwiseXor.GetDisplayText());
        Assert.Equal("PGREGEXNOTMATCH", BinaryOperator.PGRegexNotMatch.GetDisplayText());
    }
}
