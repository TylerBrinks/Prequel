namespace Prequel.Logical.Rules;

public enum RewriterRecursion
{
    /// Continue rewrite this node tree.
    Continue,
    /// Call 'op' immediately and return.
    Mutate,
    /// Do not rewrite the children of this node.
    Stop,
    /// Keep recursive but skip apply op on this node
    Skip
}
/// <summary>
/// Defines a pre-visit rewriter
/// </summary>
/// <typeparam name="T">Type of node being rewritten</typeparam>
internal interface ITreeNodeRewriter<T> where T : INode
{
    RewriterRecursion PreVisit(T node);
    T Mutate(T node);
}