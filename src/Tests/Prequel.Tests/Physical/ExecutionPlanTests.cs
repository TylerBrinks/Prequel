using SqlParser.Ast;
using Prequel.Tests.Data;
using Prequel.Tests.Fakes;
using Prequel.Logical.Plans;
using Prequel.Physical;
using Prequel.Physical.Expressions;
using Prequel.Execution;
using Prequel.Data;
using SqlParser;
using Schema = Prequel.Data.Schema;
using Exec = Prequel.Execution.ExecutionContext;

namespace Prequel.Tests.Physical;

public class PlanTests
{
    private readonly Exec _context;

    public PlanTests()
    {
        _context = new Exec();

        var schema = new Schema([
            new("a", ColumnDataType.Integer),
            new("b", ColumnDataType.Utf8),
            new("c", ColumnDataType.Double)
        ]);
        var memDb = new FakeDataTable("db", schema, [0, 1, 2]);
        _context.RegisterDataTable(memDb);

        var joinSchema = new Schema([
            new("c", ColumnDataType.Integer),
            new("d", ColumnDataType.Utf8),
            new("e", ColumnDataType.Double)
        ]);
        var joinDb = new FakeDataTable("joinDb", joinSchema, [0, 1, 2]);
        _context.RegisterDataTable(joinDb);
    }

    [Fact]
    public void Context_Supports_One_Query()
    {
        const string sql = "select * from db; select * from db";

        Assert.Throws<InvalidOperationException>(() => _context.BuildLogicalPlan(sql));
    }

    [Fact]
    public void Context_Optimizes_Wildcard_Projections()
    {
        const string sql = "select * from db";

        var logicalPlan = _context.BuildLogicalPlan(sql);
        var physicalPlan = Exec.BuildPhysicalPlan(logicalPlan);

        Assert.IsType<FakeTableExecution>(physicalPlan);
        Assert.Equal(3, physicalPlan.Schema.Fields.Count);
    }

    [Fact]
    public void Context_Optimizes_Select_Projections()
    {
        const string sql = "select a,b,c from db";
        
        var logicalPlan = _context.BuildLogicalPlan(sql);
        var physicalPlan = Exec.BuildPhysicalPlan(logicalPlan);

        Assert.IsType<FakeTableExecution>(physicalPlan);
        Assert.Equal(3, physicalPlan.Schema.Fields.Count);
    }

    [Fact]
    public void Context_Optimizes_Literal_Filtered_Selects()
    {
        const string sql = "select a,b,c from db where a = '1'";

        var logicalPlan = _context.BuildLogicalPlan(sql);
        var filterExec = (FilterExecution)Exec.BuildPhysicalPlan(logicalPlan);
        var binary = (Binary)filterExec.Predicate;

        Assert.Equal(3, filterExec.Schema.Fields.Count);
        Assert.IsType<Column>(binary.Left);
        Assert.IsType<Literal>(binary.Right);
        Assert.Equal(BinaryOperator.Eq, binary.Op);
        Assert.IsType<FakeTableExecution>(filterExec.Plan);
        Assert.Equal(3, filterExec.Plan.Schema.Fields.Count);
    }

    [Fact]
    public void Context_Optimizes_Column_Filtered_Selects()
    {
        const string sql = "select a,b,c from db where a > b";

        var logicalPlan = _context.BuildLogicalPlan(sql);
        var filterExec = (FilterExecution)Exec.BuildPhysicalPlan(logicalPlan);
        var binary = (Binary)filterExec.Predicate;

        Assert.Equal(3, filterExec.Schema.Fields.Count);
        Assert.IsType<Column>(binary.Left);
        Assert.IsType<Column>(binary.Right);
        Assert.Equal(BinaryOperator.Gt, binary.Op);
        Assert.IsType<FakeTableExecution>(filterExec.Plan);
        Assert.Equal(3, filterExec.Plan.Schema.Fields.Count);
    }

    [Fact]
    public void Context_Uses_Multiple_Aggregation_Steps()
    {
        const string sql = "SELECT avg(a) FROM db group by a";
        var logicalPlan = _context.BuildLogicalPlan(sql);
        var projectionExec = (ProjectionExecution)Exec.BuildPhysicalPlan(logicalPlan);

        Assert.Single(projectionExec.Schema.Fields);
        Assert.Equal("Avg(db.a)", projectionExec.Expressions[0].Name);
        Assert.Single(projectionExec.Schema.Fields);

        var finalAggregateExec = (AggregateExecution)projectionExec.Plan;
        Assert.Equal(2, finalAggregateExec.Schema.Fields.Count);
        Assert.Equal(AggregationMode.Final, finalAggregateExec.Mode);
        Assert.Equal("a", finalAggregateExec.GroupBy.Expression[0].Name);
        Assert.Single(finalAggregateExec.AggregateExpressions);
        Assert.Equal("a", ((Column)finalAggregateExec.AggregateExpressions[0].Expression).Name);

        var partialAggregateExec = (AggregateExecution)finalAggregateExec.Plan;
        Assert.Equal(2, partialAggregateExec.Schema.Fields.Count);
        Assert.Equal(AggregationMode.Partial, partialAggregateExec.Mode);
        Assert.Equal("a", partialAggregateExec.GroupBy.Expression[0].Name);
        Assert.Single(partialAggregateExec.AggregateExpressions);
        Assert.Equal("a", ((Column)partialAggregateExec.AggregateExpressions[0].Expression).Name);
       
        var scanExec = (FakeTableExecution)partialAggregateExec.Plan;
        Assert.Equal(3, scanExec.Schema.Fields.Count);
    }

    [Fact]
    public void Context_Converts_Distinct_To_Aggregates()
    {
        const string sql = "SELECT distinct a FROM db";
        var logicalPlan = _context.BuildLogicalPlan(sql);
        var finalAggregateExec = (AggregateExecution)Exec.BuildPhysicalPlan(logicalPlan);

        Assert.Single(finalAggregateExec.Schema.Fields);
        Assert.Equal(AggregationMode.Final, finalAggregateExec.Mode);
        Assert.Equal("a", finalAggregateExec.GroupBy.Expression[0].Name);
        Assert.Empty(finalAggregateExec.AggregateExpressions);

        var partialAggregateExec = (AggregateExecution) finalAggregateExec.Plan;
        Assert.Single(partialAggregateExec.Schema.Fields);
        Assert.Equal(AggregationMode.Partial, partialAggregateExec.Mode);
        Assert.Equal("a", partialAggregateExec.GroupBy.Expression[0].Name);
        Assert.Empty(partialAggregateExec.AggregateExpressions);

        var scanExec = (FakeTableExecution) partialAggregateExec.Plan;
        Assert.Equal(3, scanExec.Schema.Fields.Count);
    }

    [Fact]
    public void Context_Orders_Projected_Column()
    {
        const string sql = "SELECT a FROM db order by a";
        var logicalPlan = _context.BuildLogicalPlan(sql);
        var sortExec = (SortExecution)Exec.BuildPhysicalPlan(logicalPlan);

        Assert.Equal(3, sortExec.Schema.Fields.Count);
        Assert.Equal("a", ((Column)sortExec.SortExpressions[0].Expression).Name);

        var scanExec = (FakeTableExecution)sortExec.Plan;
        Assert.Equal(3, scanExec.Schema.Fields.Count);
    }

    [Fact]
    public void Context_Orders_Unused_Column()
    {
        const string sql = "SELECT a FROM db order by b";
        var logicalPlan = _context.BuildLogicalPlan(sql);
        var projectionExec = (ProjectionExecution)Exec.BuildPhysicalPlan(logicalPlan);

        Assert.Single(projectionExec.Schema.Fields);
        Assert.Equal("a", ((Column)projectionExec.Expressions[0].Expression).Name);

        var sortExec = (SortExecution) projectionExec.Plan;
        Assert.Equal(3, sortExec.Schema.Fields.Count);
        Assert.Single(sortExec.SortExpressions);
        Assert.Equal("b", ((Column)sortExec.SortExpressions[0].Expression).Name);

        var scanExec = (FakeTableExecution)sortExec.Plan;
        Assert.Equal(3, scanExec.Schema.Fields.Count);
    }

    [Fact]
    public void Context_Groups_Columns()
    {
        const string sql = "SELECT max(a), b FROM db group by b";
        var logicalPlan = _context.BuildLogicalPlan(sql);
        var projectionExec = (ProjectionExecution)Exec.BuildPhysicalPlan(logicalPlan);
        var finalAggregateExec = (AggregateExecution)projectionExec.Plan;
        var partialAggregateExec = (AggregateExecution)finalAggregateExec.Plan;
        var scanExec = (FakeTableExecution)partialAggregateExec.Plan;

        Assert.Equal(2, projectionExec.Schema.Fields.Count);
        Assert.Equal("MAX(db.a)", ((Column)projectionExec.Expressions[0].Expression).Name);
        Assert.Equal("b", ((Column)projectionExec.Expressions[1].Expression).Name);

        Assert.Equal(2, finalAggregateExec.Schema.Fields.Count);
        Assert.Single(finalAggregateExec.AggregateExpressions);
        Assert.Single(finalAggregateExec.GroupBy.Expression);
        Assert.Equal("b", ((Column)finalAggregateExec.GroupBy.Expression[0].Expression).Name);
        Assert.Equal("a", ((Column)finalAggregateExec.AggregateExpressions[0].Expression).Name);

        Assert.Equal(2, partialAggregateExec.Schema.Fields.Count);
        Assert.Single(partialAggregateExec.AggregateExpressions);
        Assert.Single(partialAggregateExec.GroupBy.Expression);
        Assert.Equal("b", ((Column)partialAggregateExec.GroupBy.Expression[0].Expression).Name);
        Assert.Equal("a", ((Column)partialAggregateExec.AggregateExpressions[0].Expression).Name);

        Assert.Equal(3, scanExec.Schema.Fields.Count);
    }

    [Fact]
    public void Context_Replaces_Having_Step_With_Filter()
    {
        const string sql = "SELECT max(a), b FROM db group by b having b = 1";
        var logicalPlan = _context.BuildLogicalPlan(sql);
        var projectionExec = (ProjectionExecution)Exec.BuildPhysicalPlan(logicalPlan);
        var filterExec = (FilterExecution) projectionExec.Plan;
        var finalAggregateExec = (AggregateExecution)filterExec.Plan;
        var partialAggregateExec = (AggregateExecution)finalAggregateExec.Plan;
        var scanExec = (FakeTableExecution)partialAggregateExec.Plan;

        Assert.Equal(2, projectionExec.Schema.Fields.Count);
        Assert.Equal("MAX(db.a)", ((Column)projectionExec.Expressions[0].Expression).Name);
        Assert.Equal("b", ((Column)projectionExec.Expressions[1].Expression).Name);

        Assert.Equal(2, filterExec.Schema.Fields.Count);
        Assert.IsType<Binary>(filterExec.Predicate);

        Assert.Equal(2, finalAggregateExec.Schema.Fields.Count);
        Assert.Single(finalAggregateExec.AggregateExpressions);
        Assert.Single(finalAggregateExec.GroupBy.Expression);
        Assert.Equal("b", ((Column)finalAggregateExec.GroupBy.Expression[0].Expression).Name);
        Assert.Equal("a", ((Column)finalAggregateExec.AggregateExpressions[0].Expression).Name);

        Assert.Equal(2, partialAggregateExec.Schema.Fields.Count);
        Assert.Single(partialAggregateExec.AggregateExpressions);
        Assert.Single(partialAggregateExec.GroupBy.Expression);
        Assert.Equal("b", ((Column)partialAggregateExec.GroupBy.Expression[0].Expression).Name);
        Assert.Equal("a", ((Column)partialAggregateExec.AggregateExpressions[0].Expression).Name);

        Assert.Equal(3, scanExec.Schema.Fields.Count);
    }

    [Fact]
    public void Context_Filters_With_Where_Clause()
    {
        const string sql = "SELECT max(a) as a, b FROM db where b = 'x' group by b";
        var logicalPlan = _context.BuildLogicalPlan(sql);
        var projectionExec = (ProjectionExecution)Exec.BuildPhysicalPlan(logicalPlan);
        var finalAggregateExec = (AggregateExecution)projectionExec.Plan;
        var partialAggregateExec = (AggregateExecution)finalAggregateExec.Plan;
        var filterExec = (FilterExecution)partialAggregateExec.Plan;
        var scanExec = (FakeTableExecution)filterExec.Plan;

        Assert.Equal(2, projectionExec.Schema.Fields.Count);
        Assert.Equal("MAX(db.a)", ((Column)projectionExec.Expressions[0].Expression).Name);
        Assert.Equal("b", ((Column)projectionExec.Expressions[1].Expression).Name);

        Assert.Equal(2, finalAggregateExec.Schema.Fields.Count);
        Assert.Single(finalAggregateExec.AggregateExpressions);
        Assert.Single(finalAggregateExec.GroupBy.Expression);
        Assert.Equal("b", ((Column)finalAggregateExec.GroupBy.Expression[0].Expression).Name);
        Assert.Equal("a", ((Column)finalAggregateExec.AggregateExpressions[0].Expression).Name);

        Assert.Equal(2, partialAggregateExec.Schema.Fields.Count);
        Assert.Single(partialAggregateExec.AggregateExpressions);
        Assert.Single(partialAggregateExec.GroupBy.Expression);
        Assert.Equal("b", ((Column)partialAggregateExec.GroupBy.Expression[0].Expression).Name);
        Assert.Equal("a", ((Column)partialAggregateExec.AggregateExpressions[0].Expression).Name);

        Assert.Equal(3, filterExec.Schema.Fields.Count);
        Assert.IsType<Binary>(filterExec.Predicate);

        Assert.Equal(3, scanExec.Schema.Fields.Count);
    }

    [Fact]
    public void Context_Orders_And_Limits_Results()
    {
        const string sql = "SELECT a, b FROM db order by a desc  offset 5 limit 10";
        var logicalPlan = _context.BuildLogicalPlan(sql);
        var limitExec = (LimitExecution)Exec.BuildPhysicalPlan(logicalPlan);
        var sortExec = (SortExecution) limitExec.Plan;
        var scanExec = (FakeTableExecution)sortExec.Plan;

        Assert.Equal(2, logicalPlan.Schema.Fields.Count);
        Assert.Equal(10, limitExec.Fetch);
        Assert.Equal(5, limitExec.Skip);

        Assert.Equal(3, sortExec.Schema.Fields.Count);
        Assert.IsType<PhysicalSortExpression>(sortExec.SortExpressions[0]);

        Assert.Equal(3, scanExec.Schema.Fields.Count);
    }

    [Fact]
    public void Context_Joins_Tables_Results()
    {
        const string sql = "SELECT db.a, joinDb.d FROM db join joinDb on db.c = joinDb.e";
        var logicalPlan = _context.BuildLogicalPlan(sql);
        var projectionExec = (ProjectionExecution)Exec.BuildPhysicalPlan(logicalPlan);
        var joinExec = (HashJoinExecution)projectionExec.Plan;
        var scanExecLeft = (FakeTableExecution)joinExec.Left;
        var scanExecRight = (FakeTableExecution)joinExec.Right;

        Assert.Equal(2, logicalPlan.Schema.Fields.Count);
        Assert.Equal(2, projectionExec.Expressions.Count);
        
        Assert.Equal(5, joinExec.Schema.Fields.Count);
        Assert.Equal(JoinType.Inner, joinExec.JoinType);

        var join = joinExec.On.First();
        Assert.Equal("c", join.Left.Name);
        Assert.Equal("e", join.Right.Name);

        Assert.Equal(3, scanExecLeft.Schema.Fields.Count);
        Assert.Equal(3, scanExecRight.Schema.Fields.Count);
    }

    [Fact]
    public void Planner_Rejects_Distinct_Plan()
    {
        Assert.Throws<InvalidOperationException>(() => new PhysicalPlanner().CreateInitialPlan(new Distinct(new TestPlan())));
    }

    [Fact]
    public async Task Context_Explains_Query_Plan()
    {
        const string sql = "EXPLAIN SELECT a, b FROM db order by a desc offset 5 limit 10";
        var explainPlan = _context.BuildLogicalPlan(sql);
        var explain = (ExplainExecution)Exec.BuildPhysicalPlan(explainPlan);

        var batch = await explain.ExecuteAsync(new QueryContext()).FirstAsync();

        var expectedSchema = new Schema([new QualifiedField("plan", ColumnDataType.Utf8)]);
        Assert.Equal(expectedSchema, batch.Schema);

        Assert.Equal(4, batch.Results[0].Values.Count);
        Assert.Equal("Limit: Skip 5, Limit 10", batch.Results[0].Values[0]);
        Assert.Equal("  Sort:  db.a Desc", batch.Results[0].Values[1]);
        Assert.Equal("    Projection: db.a, db.b ", batch.Results[0].Values[2]);
        Assert.Equal("      Table Scan: db projection=(db.a, db.b, db.c)", batch.Results[0].Values[3]);
    }

    [Fact]
    public void Nested_Explain_Should_Fail()
    {
        const string sql = "EXPLAIN EXPLAIN SELECT 1";
        Assert.Throws<ParserException>(() => _context.BuildLogicalPlan(sql));
    }
}