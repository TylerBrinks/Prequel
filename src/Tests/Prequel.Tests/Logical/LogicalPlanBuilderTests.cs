using Prequel.Logical.Expressions;
using Prequel.Data;
using Prequel.Logical;
using Prequel.Logical.Plans;

namespace Prequel.Tests.Logical
{
    public class LogicalPlanBuilderTests
    {
        [Fact]
        public void LogicalPlanBuilder_Builds_Projections()
        {
            var schema = new Schema([
                new("one", ColumnDataType.Integer),
                new("two", ColumnDataType.Integer)
            ]);

            var projection = (Projection)new TestPlan(schema).Project([
                new Column("one"),
                new Column("two")
            ]);

            Assert.Equal(schema, projection.Schema);
            Assert.Equal(2, projection.Expression.Count);
            Assert.IsType<TestPlan>(projection.Plan);
        }

        [Fact]
        public void LogicalPlanBuilder_Builds_Aggregate()
        {
            var schema = new Schema([
                new("one", ColumnDataType.Integer),
                new("two", ColumnDataType.Integer)
            ]);

            var groupExpr = new List<ILogicalExpression> { new Column("one") };
            var aggExpr = new List<ILogicalExpression> { new Column("two") };


            var aggregate = (Aggregate)new TestPlan(schema).Aggregate(groupExpr, aggExpr);
            Assert.Equal(schema, aggregate.Schema);
            Assert.Single(aggregate.AggregateExpressions);
            Assert.Single(aggregate.GroupExpressions);
            Assert.IsType<TestPlan>(aggregate.Plan);
        }

        [Fact]
        public void LogicalPlanBuilder_Builds_Filter()
        {
            var schema = new Schema([
                new("one", ColumnDataType.Integer),
                new("two", ColumnDataType.Integer)
            ]);

            var predicate = new Column("one");
            var filter = (Filter)new TestPlan(schema).Filter(predicate);
            Assert.Equal(schema, filter.Schema);
            Assert.Equal(predicate, filter.Predicate);
            Assert.IsType<TestPlan>(filter.Plan);
        }

        [Fact]
        public void LogicalPlanBuilder_Builds_Alias()
        {
            var schema = new Schema([
                new("one", ColumnDataType.Integer, new TableReference("alias")),
                new("two", ColumnDataType.Integer, new TableReference("alias"))
            ]);

            var alias = (SubqueryAlias)new TestPlan(schema).SubqueryAlias("alias");
            Assert.Equal(schema, alias.Schema);
            Assert.Equal("alias", alias.Alias);
            Assert.IsType<TestPlan>(alias.Plan);
        }
    }
}
