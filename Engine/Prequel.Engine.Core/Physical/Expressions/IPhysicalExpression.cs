using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Values;

namespace Prequel.Engine.Core.Physical.Expressions;

internal interface IPhysicalExpression
{
    internal ColumnDataType GetDataType(Schema schema);

    ColumnValue Evaluate(RecordBatch batch, int? schemaIndex = null);
}