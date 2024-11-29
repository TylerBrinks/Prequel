namespace Prequel.Logical.Expressions;

/// <summary>
/// Logical expression definition
/// </summary>
public interface ILogicalExpression : INode
{
    /// <summary>
    /// Applies a lambda function to any logical expression node by first querying
    /// all child expressions and then applying the action to each node until
    /// the action is flagged to stop or all actions have been applied.
    /// </summary>
    /// <param name="action">Action to run on each child expression</param>
    /// <returns>VisitRecursion flag</returns>
    VisitRecursion INode.ApplyChildren(Func<INode, VisitRecursion> action)
    {
        var children = GetChildExpressions(this);

        foreach (var result in children.Select(child => action(child)))
        {
            // ReSharper disable once SwitchStatementMissingSomeEnumCasesNoDefault
            switch (result)
            {
                case VisitRecursion.Skip:
                    return VisitRecursion.Continue;

                case VisitRecursion.Stop:
                    return result;
            }
        }

        return VisitRecursion.Continue;
    }
    /// <summary>
    /// Gets all child expressions for a given logical expression
    /// </summary>
    /// <param name="expression">Expression to query for children</param>
    /// <returns>Expression's child nodes</returns>
    internal List<ILogicalExpression> GetChildExpressions(ILogicalExpression expression)
    {
        return expression switch
        {
            Column
                or ScalarVariable
                or Literal
                or OuterReferenceColumn
                or ScalarSubquery
                => [],
            AggregateFunction fn => GetAggregateChildren(fn),
            Alias alias => [alias.Expression],
            OrderBy orderBy => [orderBy.Expression],
            InList inList => [inList.Expression, .. inList.List],
            Binary binary => [binary.Left, binary.Right],
            Between between => [between.Expression, between.Low, between.High],
            Like like => [like.Expression, like.Pattern],
            Case @case => GetCaseChildren(@case),

            _ => []
        };

        static List<ILogicalExpression> GetAggregateChildren(AggregateFunction fn)
        {
            var args = fn.Args.ToList();
            if (fn.Filter != null)
            {
                args.Add(fn.Filter);
            }

            return args;
        }

        static List<ILogicalExpression> GetCaseChildren(Case @case)
        {
            var caseExpressions = new List<ILogicalExpression>();

            if (@case.Expression != null)
            {
                caseExpressions.Add(@case.Expression);
            }

            foreach (var (when, then) in @case.WhenThenExpression)
            {
                caseExpressions.Add(when);
                caseExpressions.Add(then);
            }

            if (@case.ElseExpression != null)
            {
                caseExpressions.Add(@case.ElseExpression);
            }

            return caseExpressions;
        }
    }
    /// <summary>
    /// Maps a logical expression's children into replacement types
    /// or instances of the node's children.
    /// </summary>
    /// <typeparam name="T">Type of expression</typeparam>
    /// <param name="instance">ILogicalExpression node instance</param>
    /// <param name="transformation">Action used to transform child expressions</param>
    /// <returns>Expression with transformed children</returns>
    T MapChildren<T>(T instance, Func<T, T> transformation) where T : class, ILogicalExpression
    {
        var transform = (Func<ILogicalExpression, ILogicalExpression>)transformation;

        return (this switch
        {
            Alias alias => alias with { Expression = transform(alias.Expression) } as T,
            Binary binary => new Binary(transform(binary.Left), binary.Op, transform(binary.Right)) as T,
            AggregateFunction fn => fn with { Args = TransformList(fn.Args, transform) } as T,
            OrderBy orderBy => orderBy with { Expression = transform(orderBy.Expression) } as T,

            _ => (T)this,
        })!;


        List<ILogicalExpression> TransformList(IEnumerable<ILogicalExpression> list, Func<ILogicalExpression, ILogicalExpression> func)
        {
            return list.Select(l => l.Transform(l, func)).ToList();
        }
    }
    /// <summary>
    /// Transforms a logical expression into another type or instance
    /// including all children of the expression
    /// </summary>
    /// <typeparam name="T">Type of expression</typeparam>
    /// <param name="instance">ILogicalExpression node instance</param>
    /// <param name="transformation"></param>
    /// <returns>Transformed expression</returns>
    T Transform<T>(T instance, Func<T, T>? transformation)
    {
        var transformFunc = transformation as Func<ILogicalExpression, ILogicalExpression>;
        var afterOpChildren = MapChildren(this, node => node.Transform(node, transformFunc!));

        return transformation!((T)afterOpChildren);
    }
}