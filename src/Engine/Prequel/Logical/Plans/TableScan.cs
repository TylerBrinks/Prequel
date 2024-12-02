using Prequel.Data;

namespace Prequel.Logical.Plans;

/// <summary>
/// Creates a table scan logical plan responsible for reading data from
/// the underlying table data source
/// </summary>
/// <param name="Name">Data source name</param>
/// <param name="Schema">Table schema</param>
/// <param name="Table">Data table source</param>
/// <param name="Projection">Optional projection containing fields to read from the source</param>
internal record TableScan(string Name, Schema Schema, DataTable Table, List<int>? Projection = null) : ILogicalPlan
{
    public string ToStringIndented(Indentation? indentation = null)
    {
        return ToString();
    }

    public override string ToString()
    {
        var fields = string.Join(", ", Projection != null 
            ? Projection.Select(i => Table.Schema!.Fields[i].Name) 
            : Schema.Fields.Select(f => f.QualifiedName));

        return $"Table Scan: {Name} projection=({fields})";
    }
}