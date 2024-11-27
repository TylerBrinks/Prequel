using SqlParser.Ast;

namespace Prequel.Engine.Core.Logical;

internal class RelationVisitor : Visitor
{
    public override ControlFlow PostVisitTableFactor(TableFactor relation)
    {
        if (relation is not TableFactor.Table table) { return ControlFlow.Continue; }

        string? alias = null;

        if (table.Alias != null)
        {
            alias = table.Alias.Name;
        }

        var reference = new TableReference(table.Name, alias);
        if (!TableReferences.Contains(reference))
        {
            TableReferences.Add(reference);
        }

        return ControlFlow.Continue;
    }

    internal List<TableReference> TableReferences { get; } = new();
}