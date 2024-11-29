using Prequel.Data;
using Prequel.Values;

namespace Prequel.Physical.Expressions;

internal interface IPhysicalExpression
{
    internal ColumnDataType GetDataType(Schema schema);

    ColumnValue Evaluate(RecordBatch batch, int? schemaIndex = null);
}