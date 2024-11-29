using Prequel.Data;
using Prequel.Values;

namespace Prequel.Physical.Expressions;

/// <summary>
/// Aggregate physical expression
/// </summary>
/// <param name="Expression"></param>
internal abstract record Aggregate(IPhysicalExpression Expression) : IPhysicalExpression
{
    public IPhysicalExpression Expression { get; set; } = Expression;

    internal abstract List<QualifiedField> StateFields { get; }

    internal abstract QualifiedField NamedQualifiedField { get; }

    internal abstract List<IPhysicalExpression> Expressions { get; }

    public virtual ColumnDataType GetDataType(Schema schema)
    {
        throw new InvalidOperationException("Aggregates must implement GetDataType");
    }

    public ColumnValue Evaluate(RecordBatch batch, int? schemaIndex = null)
    {
        throw new NotSupportedException();
    }
}