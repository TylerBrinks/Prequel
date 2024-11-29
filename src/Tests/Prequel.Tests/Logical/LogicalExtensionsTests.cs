using SqlParser.Ast;
using SqlParser;
using static SqlParser.Ast.Expression;
using Wildcard = Prequel.Logical.Expressions.Wildcard;
using Prequel.Logical.Expressions;
using Prequel.Logical.Values;
using Prequel.Data;
using Prequel.Logical;
// ReSharper disable IdentifierTypo

namespace Prequel.Tests.Logical;

public class LogicalExtensionsTests
{
    [Fact]
    public void LogicalExpressions_Create_Names()
    {
        var column = new Column("column", new TableReference("table"));
        var alias = new Alias(column, "alias");
        var binary = new Binary(column, BinaryOperator.Eq, new Column("right"));
        var literal = new Literal(new StringScalar("value"));
        var function = new AggregateFunction(AggregateFunctionType.Min, [column], false);
        var functionDistinct = new AggregateFunction(AggregateFunctionType.Min, [column], true);
        var wildcard = new Wildcard();

        Assert.Equal("table.column", column.CreateLogicalName());
        Assert.Equal("alias", alias.CreateLogicalName());
        Assert.Equal("table.column = right", binary.CreateLogicalName());
        Assert.Equal("value", literal.CreateLogicalName());
        Assert.Equal("MIN(table.column)", function.CreateLogicalName());
        Assert.Equal("MIN(DISTINCT table.column)", functionDistinct.CreateLogicalName());
        Assert.Equal("*", wildcard.CreateLogicalName());
    }

    [Fact]
    public void Expressions_Covert_To_Columns()
    {
        var expressions = new List<ILogicalExpression>
            {
                new Column("column"),
                new ScalarVariable(["scalar"]),
                new Literal(new IntegerScalar(1))
            };

        var columns = new HashSet<Column>();
        expressions.ExpressionListToColumns(columns);

        Assert.Equal(2, columns.Count);
        Assert.Equal("column", columns.First().FlatName);
        Assert.Equal("scalar", columns.Last().FlatName);
    }

    [Fact]
    public void ExpressionsList_Converts_To_Fields()
    {
        var expressions = new List<ILogicalExpression>
            {
                new Column("column", new TableReference("table")),
                new Alias(new Column("other"), "alias"),
            };

        var schema = new Schema([
            new("column", ColumnDataType.Utf8),
            new("other", ColumnDataType.Utf8)
        ]);
        var fields = expressions.ExpressionListToFields(new TestPlan(schema));

        Assert.NotNull(fields[0].Qualifier);
        Assert.Null(fields[1].Qualifier);
    }

    [Fact]
    public void Expression_Gets_DataType()
    {
        var schema = new Schema([
            new("column", ColumnDataType.Integer),
            new("other", ColumnDataType.Integer)
        ]);

        var expression = new Column("column");
        var alias = new Alias(new Column("other"), "alias");
        var fn = new AggregateFunction(AggregateFunctionType.Min, [new Column("column")], false);
        var fnDouble = new AggregateFunction(AggregateFunctionType.Avg, [new Column("column")], false);

        Assert.Equal(ColumnDataType.Integer, expression.GetDataType(schema));
        Assert.Equal(ColumnDataType.Integer, alias.GetDataType(schema));
        Assert.Equal(ColumnDataType.Integer, fn.GetDataType(schema));
        Assert.Equal(ColumnDataType.Double, fnDouble.GetDataType(schema));
    }

    [Fact]
    public void Unalias_Reverts_To_Wrapped_Expression()
    {
        var wildcard = new Wildcard();
        var alias = new Alias(wildcard, "wildcard");
        var unaliased = alias.Unalias();

        Assert.Equal(wildcard, unaliased);
        Assert.Same(wildcard, unaliased);
    }

    [Fact]
    public void Clone_Resolves_Callback_Expression()
    {
        var column = new Column("column");
        var alias = (Alias)column.CloneWithReplacement(e => new Alias(e, "alias"));

        Assert.IsType<Alias>(alias);
        Assert.Same(alias.Expression, column);
    }

    [Fact]
    public void Clone_Resolves_Null_Expression()
    {
        var column = new Column("column");
        var literal = new Literal(new IntegerScalar(1));
        var alias = new Alias(column, "alias");
        var fn = new AggregateFunction(AggregateFunctionType.Min, [column, alias], false);
        var orderBy = new Prequel.Logical.Expressions.OrderBy(column, false);

        var columnClone = column.CloneWithReplacement(_ => null);
        Assert.IsType<Column>(columnClone);
        Assert.Same(columnClone, column);

        var literalClone = literal.CloneWithReplacement(_ => null);
        Assert.IsType<Literal>(literalClone);
        Assert.Same(literalClone, literal);

        var aliasClone = alias.CloneWithReplacement(_ => null);
        Assert.IsType<Alias>(aliasClone);
        Assert.Equal(aliasClone, alias);
        Assert.Equal(alias.Expression, column);

        var fnClone = fn.CloneWithReplacement(_ => null);
        Assert.IsType<AggregateFunction>(fnClone);
        Assert.Equal(fnClone, fn);

        Assert.Throws<NotImplementedException>(() => orderBy.CloneWithReplacement(_ => null));
    }

    [Fact]
    public void Sql_Converts_To_Logical_Expressions()
    {
        var schema = new Schema([
            new("ident", ColumnDataType.Integer),
            new("fn", ColumnDataType.Integer),
            new("first", ColumnDataType.Integer)
        ]);

        var literal = new LiteralValue(new Value.Boolean(true));
        var ident = new Identifier("ident");
        var fn = new Function(new ObjectName("min"));
        var compound = new CompoundIdentifier(new Sequence<Ident>(new List<Ident> { "first" }));
        var context = new PlannerContext(null!, null!);

        var literalExpr = (Literal)literal.SqlToExpression(null!, context);
        Assert.True((bool)literalExpr.Value.RawValue!);

        var identExpr = (Column)ident.SqlToExpression(schema, context);
        Assert.Equal("ident", identExpr.Name);

        var fnExpr = (AggregateFunction)fn.SqlToExpression(schema, context);
        Assert.Equal(AggregateFunctionType.Min, fnExpr.FunctionType);

        var compoundExpr = (Column)compound.SqlToExpression(schema, context);
        Assert.Equal("first", compoundExpr.Name);
    }

    [Fact]
    public void Sql_Converts_To_Binary_Logical_Expressions()
    {
        var schema = new Schema([
            new("left", ColumnDataType.Integer),
            new("right", ColumnDataType.Integer),
            new("first", ColumnDataType.Integer)
        ]);

        var binary = new BinaryOp(
            new LiteralValue(new Value.Number("1")),
            BinaryOperator.Eq,
            new LiteralValue(new Value.Number("1")));

        var binaryExpr = (Binary)binary.SqlToExpression(schema, new PlannerContext(null!, null!));
        Assert.IsType<Literal>(binaryExpr.Left);
        Assert.IsType<Literal>(binaryExpr.Right);
        Assert.Equal(BinaryOperator.Eq, binaryExpr.Op);
    }

    [Fact]
    public void Expressions_Rebase_From_Collection()
    {
        var column = new Column("column");
        var alias = new Alias(column, "alias");
        var schema = new Schema([
            new("column", ColumnDataType.Integer),
            new("alias", ColumnDataType.Integer)
        ]);

        var rebased = (Column)alias.RebaseExpression([alias], schema);
        Assert.Equal("alias", rebased.Name);
    }

    [Fact]
    public void Column_Expressions_Rebase_As_Self()
    {
        var column = new Column("column");
        var schema = new Schema([
            new("column", ColumnDataType.Integer)
        ]);

        var rebased = (Column)column.RebaseExpression([column], schema);
        Assert.Equal("column", rebased.Name);
        Assert.NotSame(column, rebased);
    }

    [Fact]
    public void Expressions_Rebase_As_Self_Without_Schema_Column()
    {
        var column = new Column("column");
        var schema = new Schema([
            new("column", ColumnDataType.Integer)
        ]);

        var rebased = (Column)column.RebaseExpression([new Column("other")], schema);
        Assert.Equal("column", rebased.Name);
        Assert.Same(column, rebased);
    }

    [Fact]
    public void Identifiers_Convert_To_Columns()
    {
        var ident = new Identifier(new Ident("name"));
        var schema = new Schema([new("name", ColumnDataType.Integer)]);
        var column = ident.SqlIdentifierToExpression(schema, new PlannerContext(null!, null!));
        Assert.Equal("name", column.Name);
        Assert.Null(column.Relation);
    }

    [Fact]
    public void Literal_Values_Parse_To_Expressions()
    {
        var number = new LiteralValue(new Value.Number("1"));
        var str = new LiteralValue(new Value.SingleQuotedString("abc"));
        var boolean = new LiteralValue(new Value.Boolean(true));

        var numberLiteral = (Literal)number.ParseValue();
        var stringLiteral = (Literal)str.ParseValue();
        var booleanLiteral = (Literal)boolean.ParseValue();

        Assert.IsType<IntegerScalar>(numberLiteral.Value);
        Assert.IsType<StringScalar>(stringLiteral.Value);
        Assert.IsType<BooleanScalar>(booleanLiteral.Value);
    }

    [Fact]
    public void Sql_Numbers_Parse_To_Literal_Expression()
    {
        Assert.IsType<IntegerScalar>(((Literal)new Value.Number("1").ParseSqlNumber()).Value);

        Assert.IsType<DoubleScalar>(((Literal)new Value.Number("1.23").ParseSqlNumber()).Value);

        Assert.IsType<StringScalar>(((Literal)new Value.Number("abc").ParseSqlNumber()).Value);
    }

    [Fact]
    public void Idents_Convert_To_Search_Terms()
    {
        var terms = new List<string> { "one", "two" };
        var search = terms.GenerateSearchTerms();

        Assert.Equal("one", search[0].Table!.Name);
        Assert.Equal("two", search[0].ColumnName);
        Assert.Empty(search[0].NestedNames);

        Assert.Null(search[1].Table);
        Assert.Equal("one", search[1].ColumnName);
        Assert.Equal("two", search[1].NestedNames[0]);
    }

    [Fact]
    public void Idents_Have_Length_Constraints()
    {
        var terms = new List<string> { "one", "two", "three" };
        Assert.Throws<InvalidOperationException>(() => terms.GenerateSearchTerms());
    }

    [Fact]
    public void BitwiseCoercion_Finds_Common_Data_Type()
    {
        Assert.Equal(ColumnDataType.Utf8, LogicalExtensions.GetBitwiseCoercion(ColumnDataType.Date32, ColumnDataType.Double));
        Assert.Equal(ColumnDataType.Integer, LogicalExtensions.GetBitwiseCoercion(ColumnDataType.Integer, ColumnDataType.Integer));
        Assert.Equal(ColumnDataType.Double, LogicalExtensions.GetBitwiseCoercion(ColumnDataType.Double, ColumnDataType.Integer));
        Assert.Equal(ColumnDataType.Double, LogicalExtensions.GetBitwiseCoercion(ColumnDataType.Integer, ColumnDataType.Double));
        Assert.Equal(ColumnDataType.Double, LogicalExtensions.GetBitwiseCoercion(ColumnDataType.Double, ColumnDataType.Double));
    }

    [Fact]
    public void ComparisonCoercion_Finds_Common_Data_Type()
    {
        Assert.Equal(ColumnDataType.Boolean, LogicalExtensions.GetComparisonCoercion(ColumnDataType.Boolean, ColumnDataType.Boolean));
        Assert.Equal(ColumnDataType.Integer, LogicalExtensions.GetComparisonCoercion(ColumnDataType.Integer, ColumnDataType.Boolean));
        Assert.Equal(ColumnDataType.Double, LogicalExtensions.GetComparisonCoercion(ColumnDataType.Boolean, ColumnDataType.Double));
        Assert.Equal(ColumnDataType.Utf8, LogicalExtensions.GetComparisonCoercion(ColumnDataType.Utf8, ColumnDataType.Utf8));
    }

    [Fact]
    public void MathCoercion_Finds_Common_Data_Type()
    {
        Assert.Equal(ColumnDataType.Utf8, LogicalExtensions.GetMathNumericalCoercion(ColumnDataType.Date32, ColumnDataType.Double));
        Assert.Equal(ColumnDataType.Integer, LogicalExtensions.GetMathNumericalCoercion(ColumnDataType.Integer, ColumnDataType.Integer));
        Assert.Equal(ColumnDataType.Double, LogicalExtensions.GetMathNumericalCoercion(ColumnDataType.Double, ColumnDataType.Integer));
        Assert.Equal(ColumnDataType.Double, LogicalExtensions.GetMathNumericalCoercion(ColumnDataType.Integer, ColumnDataType.Double));
        Assert.Equal(ColumnDataType.Double, LogicalExtensions.GetMathNumericalCoercion(ColumnDataType.Double, ColumnDataType.Double));
    }
}

internal record TestPlan(Schema Schema) : ILogicalPlan
{
    public string ToStringIndented(Indentation? indentation)
    {
        throw new NotImplementedException();
    }
}