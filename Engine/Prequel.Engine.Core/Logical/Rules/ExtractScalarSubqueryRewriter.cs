using Prequel.Engine.Core.Logical;
using Prequel.Engine.Core.Logical.Expressions;

namespace Prequel.Engine.Core.Logical.Rules;

/// <summary>
/// Tree node rewriter rule to extract subqueries that yield a scalar result
/// </summary>
internal class ExtractScalarSubqueryRewriter : ITreeNodeRewriter<ILogicalExpression>
{
    private readonly IAliasGenerator _aliasGenerator;

    public ExtractScalarSubqueryRewriter(IAliasGenerator aliasGenerator)
    {
        _aliasGenerator = aliasGenerator;
    }

    /// <summary>
    /// List of scalar subquery info and string aliases
    /// </summary>
    internal List<(ScalarSubquery, string)> SubqueryInfo { get; } = new();

    /// <summary>
    /// Checks if the visited node is a ScalarSubquery type
    /// </summary>
    /// <param name="node">Expression node to check</param>
    /// <returns>Mutate if the node is a ScalarSubquery; otherwise Continue</returns>
    public RewriterRecursion PreVisit(ILogicalExpression node)
    {
        return node switch
        {
            ScalarSubquery => RewriterRecursion.Mutate,
            _ => RewriterRecursion.Continue
        };
    }
    /// <summary>
    /// Rewrites the scalar expression as a column with a unique subquery alias name
    /// </summary>
    /// <param name="expression">Expression to rewrite</param>
    /// <returns>Rewritten column expression with an alias as the table reference</returns>
    public ILogicalExpression Mutate(ILogicalExpression expression)
    {
        return expression switch
        {
            ScalarSubquery s => RewriteSubquery(s),
            _ => expression
        };

        ILogicalExpression RewriteSubquery(ScalarSubquery subquery)
        {
            var subqueryAlias = $"__scalar_sq_{_aliasGenerator.Next()}";
            SubqueryInfo.Add((subquery, subqueryAlias));

            return new Column("__value", new TableReference(subqueryAlias));
        }
    }
}