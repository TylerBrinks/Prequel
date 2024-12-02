using Prequel.Data;
using Prequel.Execution;
using Prequel.Physical.Joins;
using Aggregate = Prequel.Logical.Plans.Aggregate;
using Column = Prequel.Logical.Expressions.Column;
using Prequel.Logical.Plans;
using Prequel.Logical.Expressions;
using Prequel.Logical;

namespace Prequel.Physical;

internal class PhysicalPlanner
{
    /// <summary>
    /// Converts a logical execution plan into a physical execution plan
    /// </summary>
    /// <param name="plan">Logical plan to convert</param>
    /// <returns>Physical execution plan</returns>
    public IExecutionPlan CreateInitialPlan(ILogicalPlan plan)
    {
        return plan switch
        {
            TableScan table => table.Table.Scan(table.Projection!),
            Aggregate aggregate => CreateAggregatePlan(aggregate),
            Projection projection => CreateProjectionPlan(projection),
            Filter filter => CreateFilterPlan(filter),
            Sort sort => CreateSortPlan(sort),
            Limit limit => CreateLimitPlan(limit),
            SubqueryAlias alias => CreateInitialPlan(alias.Plan),
            Join join => CreateJoinPlan(join),
            CrossJoin cross => new CrossJoinExecution(CreateInitialPlan(cross.Plan), CreateInitialPlan(cross.Right)),
            EmptyRelation empty => new EmptyExecution(empty.Schema),
            Union union => CreateUnionPlan(union),

            // Distinct should have been replaced by an 
            // aggregate plan by this point.
            Distinct => throw new InvalidOperationException("Distinct plans must be replaced with aggregations"),
            Explain explain => new ExplainExecution(explain),

            _ => throw new NotImplementedException("The physical plan type has not been implemented")
        };
    }
    /// <summary>
    /// Converts a logical projection into a physical projection
    /// </summary>
    /// <param name="projection">Projection to convert</param>
    /// <returns>Projection execution plan</returns>
    private IExecutionPlan CreateProjectionPlan(Projection projection)
    {
        var inputExec = CreateInitialPlan(projection.Plan);
        var inputSchema = projection.Plan.Schema;

        var physicalExpressions = projection.Expression.Select(e =>
        {
            string physicalName;

            if (e is Column col)
            {
                var index = inputSchema.IndexOfColumn(col);
                physicalName = index != null
                    ? inputExec.Schema.Fields[index.Value].Name
                    : e.CreatePhysicalName(true);
            }
            else
            {
                physicalName = e.CreatePhysicalName(true);
            }

            return (Expression: e.CreatePhysicalExpression(inputSchema, inputExec.Schema), Name: physicalName);
        }).ToList();

        return ProjectionExecution.TryNew(physicalExpressions, inputExec);
    }
    /// <summary>
    /// Converts a logical aggregate into a physical aggregate
    /// </summary>
    /// <param name="aggregate">Aggregate to convert</param>
    /// <returns>Aggregate execution plan</returns>
    private IExecutionPlan CreateAggregatePlan(Aggregate aggregate)
    {
        var inputExec = CreateInitialPlan(aggregate.Plan);
        var physicalSchema = inputExec.Schema;
        var logicalSchema = aggregate.Plan.Schema;

        var groups = aggregate.GroupExpressions.CreateGroupingPhysicalExpression(logicalSchema, physicalSchema);

        var aggregates = aggregate.AggregateExpressions
            .Select(e => e.CreateAggregateExpression(logicalSchema, physicalSchema))
            .ToList();

        var initialAggregate = AggregateExecution.TryNew(AggregationMode.Partial, groups, aggregates, inputExec, physicalSchema);

        var finalGroup = initialAggregate.OutputGroupExpression();

        var finalGroupingSet = GroupBy.NewSingle(finalGroup.Select((e, i) => (e, groups.Expression[i].Name)).ToList());

        return AggregateExecution.TryNew(AggregationMode.Final, finalGroupingSet, aggregates, initialAggregate, physicalSchema);
    }
    /// <summary>
    /// Converts a logical filter into a physical projection
    /// </summary>
    /// <param name="filter">Filter to convert</param>
    /// <returns>Filter execution plan</returns>
    private IExecutionPlan CreateFilterPlan(Filter filter)
    {
        var physicalInput = CreateInitialPlan(filter.Plan);
        var inputSchema = physicalInput.Schema;
        var inputDfSchema = filter.Plan.Schema;
        var runtimeExpr = filter.Predicate.CreatePhysicalExpression(inputDfSchema, inputSchema);

        return FilterExecution.TryNew(runtimeExpr, physicalInput);
    }
    /// <summary>
    /// Converts a logical sort into a physical projection
    /// </summary>
    /// <param name="sort">Sort to convert</param>
    /// <returns>Sort execution plan</returns>
    private IExecutionPlan CreateSortPlan(Sort sort)
    {
        var physicalInput = CreateInitialPlan(sort.Plan);
        var inputSchema = physicalInput.Schema;
        var sortSchema = sort.Plan.Schema;

        var sortExpressions = sort.OrderByExpressions
            .Select(e =>
            {
                if (e is OrderBy order)
                {
                    return order.Expression.CreatePhysicalSortExpression(sortSchema, inputSchema, order.Ascending);
                }

                throw new InvalidOperationException("Sort only accepts sort expressions");
            }).ToList();

        return new SortExecution(sortExpressions, physicalInput);
    }
    /// <summary>
    /// Converts a logical limit into a physical projection
    /// </summary>
    /// <param name="limit">Limit to convert</param>
    /// <returns>Limit execution plan</returns>
    private IExecutionPlan CreateLimitPlan(Limit limit)
    {
        var physicalInput = CreateInitialPlan(limit.Plan);

        var skip = limit.Skip ?? 0;
        var fetch = limit.Fetch ?? int.MaxValue;

        return new LimitExecution(physicalInput, skip, fetch);
    }
    /// <summary>
    /// Converts a logical join into a physical projection
    /// </summary>
    /// <param name="join">Join to convert</param>
    /// <returns>Join execution plan</returns>
    private IExecutionPlan CreateJoinPlan(Join join)
    {
        var leftSchema = join.Plan.Schema;
        var leftPlan = CreateInitialPlan(join.Plan);

        var rightSchema = join.Right.Schema;
        var rightPlan = CreateInitialPlan(join.Right);

        var joinSchema = rightSchema.Equals(rightPlan.Schema) ? rightPlan.Schema : rightSchema;

        var joinOn = join.On.Select(k =>
        {
            var leftColumn = (Column)k.Left;
            var rightColumn = (Column)k.Right;

            return new JoinOn(
                new Expressions.Column(leftColumn.Name, leftSchema.IndexOfColumn(leftColumn)!.Value),
                new Expressions.Column(rightColumn.Name, rightSchema.IndexOfColumn(rightColumn)!.Value)
            );
        }).ToList();

        var joinFilter = CreateJoinFilter();

        var (schema, columnIndices) = LogicalExtensions.BuildJoinSchema(leftPlan.Schema, joinSchema/*rightPlan.Schema*/, join.JoinType);

        if (!joinOn.Any())
        {
            return new NestedLoopJoinExecution(leftPlan, rightPlan, joinFilter, join.JoinType, columnIndices, schema);
        }

        return new HashJoinExecution(leftPlan, rightPlan, joinOn, joinFilter,
            join.JoinType,/* PartitionMode.CollectLeft,*/ columnIndices, false, schema);

        JoinFilter? CreateJoinFilter()
        {
            if (join.Filter == null)
            {
                return null;
            }

            var columns = new HashSet<Column>();
            join.Filter.ExpressionToColumns(columns);

            // Collect left & right field indices, the field indices are sorted in ascending order
            var leftFieldIndices = columns.Select(leftSchema.IndexOfColumn)
                .Where(i => i != null)
                .Select(i => i!.Value)
                .OrderBy(i => i)
                .ToList();

            var rightFieldIndices = columns.Select(rightSchema.IndexOfColumn)
                .Where(i => i != null)
                .Select(i => i!.Value)
                .OrderBy(i => i)
                .ToList();

            var leftFilterFields = leftFieldIndices
                .Select(i => (leftSchema.Fields[i], leftPlan.Schema.Fields[i]));

            var rightFilterFields = rightFieldIndices
                .Select(i => (rightSchema.Fields[i], rightPlan.Schema.Fields[i]));

            var filterFields = leftFilterFields.Concat(rightFilterFields).ToList();

            // Construct intermediate schemas used for filtering data and
            // convert logical expression to physical according to filter schema
            var filterDfSchema = new Schema(filterFields.Select(f => f.Item1).ToList());
            var filterSchema = new Schema(filterFields.Select(f => f.Item2).ToList());

            var filterExpression = join.Filter!.CreatePhysicalExpression(filterDfSchema, filterSchema);

            var leftIndices = leftFieldIndices.Select(i => new JoinColumnIndex(i, JoinSide.Left));
            var rightIndices = rightFieldIndices.Select(i => new JoinColumnIndex(i, JoinSide.Right));

            var allIndices = leftIndices.Concat(rightIndices).ToList();

            return new JoinFilter(filterExpression, allIndices, filterSchema);
        }
    }
    /// <summary>
    /// Converts a logical union into a physical projection
    /// </summary>
    /// <param name="union">Union to convert</param>
    /// <returns>Union execution plan</returns>
    private IExecutionPlan CreateUnionPlan(Union union)
    {
        var plans = union.Inputs.Select(CreateInitialPlan).ToList();

        if (union.Schema.Fields.Count < plans[0].Schema.Fields.Count)
        {
            //TODO handle field count differences
            throw new InvalidOperationException("Field count mismatch");
        }

        return new UnionExecution(plans, union.Schema);
    }
}