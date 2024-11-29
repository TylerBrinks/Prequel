namespace Prequel.Engine.Core.Logical.Expressions;

/// <summary>
/// Scalar subquery expression referring to an outer expression value
/// </summary>
/// <param name="Plan">Subquery logical plan</param>
/// <param name="OutRefColumns">List of references to the outer query column</param>
internal record ScalarSubquery(ILogicalPlan Plan, List<ILogicalExpression> OutRefColumns) : ILogicalExpression
{
    public override string ToString()
    {
        return $"Subquery({Plan})";
    }
}
