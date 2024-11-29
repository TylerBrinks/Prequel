namespace Prequel.Logical;

public interface INode
{
    VisitRecursion Apply(Func<INode, VisitRecursion> action)
    {
        var result = action(this);

        return result switch
        {
            VisitRecursion.Skip => VisitRecursion.Continue,
            VisitRecursion.Stop => VisitRecursion.Stop,
            _ => ApplyChildren(node => node.Apply(action))
        };
    }

    internal VisitRecursion ApplyChildren(Func<INode, VisitRecursion> action);
}