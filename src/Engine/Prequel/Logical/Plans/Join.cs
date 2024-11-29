using Prequel.Data;
using Prequel.Logical.Expressions;

namespace Prequel.Logical.Plans;

/// <summary>
/// Join logical plan
/// </summary>
/// <param name="Plan">Left side join plan</param>
/// <param name="Right">Right side join plan</param>
/// <param name="On">On conditions in the form of left and right expressions</param>
/// <param name="Filter">Optional join filter expression</param>
/// <param name="JoinType">Type of join to perform</param>
internal record Join(
    ILogicalPlan Plan,
    ILogicalPlan Right,
    List<(ILogicalExpression Left, ILogicalExpression Right)> On,
    ILogicalExpression? Filter,
    JoinType JoinType,
    Schema Schema
    //JoinConstraint JoinConstraint
    ) : ILogicalPlanParent
{
    /// <summary>
    /// Creates a new instance of a join plan
    /// </summary>
    /// <param name="left">Left side join plan</param>
    /// <param name="right">Right side join plan</param>
    /// <param name="joinType">Join type</param>
    /// <param name="joinKeys">Join keys mapping relations between plans</param>
    /// <param name="expressionFilter">Optional filter expressions</param>
    /// <returns>Join logical plan</returns>
    internal static Join TryNew(
        ILogicalPlan left,
        ILogicalPlan right,
        JoinType joinType,
        JoinKey joinKeys,
        //JoinConstraint joinConstraint,
        ILogicalExpression? expressionFilter)
    {
        ILogicalExpression? filter = null;
        if (expressionFilter != null)
        {
            var schemas = new List<List<Schema>> { new() { left.Schema, right.Schema } };
            filter = expressionFilter.NormalizeColumnWithSchemas(schemas, []);
        }

        var (leftKeys, rightKeys) = GetJoinKeys(joinKeys);

        var on = leftKeys.Zip(rightKeys)
            .Select(key => ((ILogicalExpression)key.First, (ILogicalExpression)key.Second))
            .ToList();

        var joinSchema = LogicalExtensions.BuildJoinSchema(left.Schema, right.Schema, joinType);
        return new Join(left, right, on, filter, joinType, joinSchema.Schema);

        JoinKey GetJoinKeys(JoinKey joinKeyValues)
        {
            var keys = joinKeyValues.Left.Zip(joinKeyValues.Right)
                .Select(k =>
                {
                    var leftColumn = k.First;
                    var rightColumn = k.Second;

                    var leftPlan = ((ILogicalPlanParent)left).Plan;
                    var rightPlan = ((ILogicalPlanParent)right).Plan;

                    return (leftColumn.Relation, rightColumn.Relation) switch
                    {
                        ({ } lr, { } rr) => GetDualKeys(lr, rr),
                        ({ } l, null) => GetLeftKeys(l),
                        (null, { } r) => GetRightKeys(r),
                        (null, null) => GetSwapKeys(),
                    };

                    (Column Left, Column Right) GetDualKeys(TableReference leftReference, TableReference rightReference)
                    {
                        var lIsLeft = leftPlan.Schema.FieldsWithQualifiedName(leftReference, leftColumn.Name);
                        var lIsRight = rightPlan.Schema.FieldsWithQualifiedName(leftReference, leftColumn.Name);
                        var rIsLeft = leftPlan.Schema.FieldsWithQualifiedName(rightReference, rightColumn.Name);
                        var rIsRight = rightPlan.Schema.FieldsWithQualifiedName(rightReference, rightColumn.Name);

                        return (lIsLeft, lIsRight, rIsLeft, rIsRight) switch
                        {
                            (_, { }, { }, _) => (rightColumn, leftColumn),
                            ({ }, _, _, { }) => (leftColumn, rightColumn),
                            _ => (Normalize(left, leftColumn), Normalize(right, rightColumn))
                        };
                    }

                    (Column Left, Column Right) GetLeftKeys(TableReference leftReference)
                    {
                        var lIsLeft = leftPlan.Schema.FieldsWithQualifiedName(leftReference, leftColumn.Name);
                        var lIsRight = rightPlan.Schema.FieldsWithQualifiedName(leftReference, leftColumn.Name);

                        return (lIsLeft, lIsRight) switch
                        {
                            ({ }, _) => (leftColumn, Normalize(right, rightColumn)),
                            (_, { }) => (Normalize(left, rightColumn), leftColumn),
                            _ => (Normalize(left, leftColumn), Normalize(right, rightColumn))
                        };
                    }

                    (Column Left, Column Right) GetRightKeys(TableReference rightReference)
                    {
                        var rIsLeft = leftPlan.Schema.FieldsWithQualifiedName(rightReference, leftColumn.Name);
                        var rIsRight = rightPlan.Schema.FieldsWithQualifiedName(rightReference, leftColumn.Name);

                        return (rIsLeft, rIsRight) switch
                        {
                            ({ }, _) => (rightColumn, Normalize(right, leftColumn)),
                            (_, { }) => (Normalize(left, leftColumn), rightColumn),
                            _ => (Normalize(left, leftColumn), Normalize(right, rightColumn))
                        };
                    }

                    (Column Left, Column Right) GetSwapKeys()
                    {
                        var swap = false;

                        Column leftKey;

                        try
                        {
                            leftKey = Normalize(left, leftColumn);
                        }
                        catch
                        {
                            swap = true;
                            leftKey = Normalize(right, leftColumn);
                        }

                        return swap ? (Normalize(left, rightColumn), leftKey) : (leftKey, Normalize(right, rightColumn));
                    }
                }).ToList();

            return new(keys.Select(k => k.Left).ToList(), keys.Select(k => k.Right).ToList());
        }

        Column Normalize(ILogicalPlan leftPlan, Column leftColumn)
        {
            return LogicalExtensions.NormalizeJoin(leftPlan, leftColumn);
        }
    }

    public override string ToString()
    {
        var on = On.Select(q => $"{q.Left} = {q.Right}").ToList();
        var filter = Filter != null ? $" Filter: {Filter}" : "";
        return $"{JoinType} Join: {string.Join(",", on)}{filter}";
    }

    public string ToStringIndented(Indentation? indentation = null)
    {
        var indent = indentation ?? new Indentation();
        return $"{this} {indent.Next(Plan)}{indent.Repeat(Right)}";
    }
}

/// <summary>
/// Join key with left and right key column lists
/// </summary>
/// <param name="Left">Join key left column list</param>
/// <param name="Right">Join key right column list</param>
internal record JoinKey(List<Column> Left, List<Column> Right);