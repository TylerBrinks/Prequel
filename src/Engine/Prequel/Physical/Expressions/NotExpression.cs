using Prequel.Data;
using Prequel.Values;

namespace Prequel.Physical.Expressions;

/// <summary>
/// NOT physical expression used to negate another physical expression
/// </summary>
/// <param name="Expression"></param>
internal record NotExpression(IPhysicalExpression Expression) : IPhysicalExpression
{
    /// <summary>
    /// Gets the evaluation data type.
    /// </summary>
    /// <param name="schema">Schema containing binary expression field definitions</param>
    /// <returns>Binary column data type</returns>
    public ColumnDataType GetDataType(Schema schema)
    {
        return Expression.GetDataType(schema);
    }
    /// <summary>
    /// Evaluates an expression and inverts the boolean result.
    /// </summary>
    /// <param name="batch">Batch to run the binary expression against</param>
    /// <param name="schemaIndex">Unused</param>
    /// <returns>Evaluated column value</returns>
    public ColumnValue Evaluate(RecordBatch batch, int? schemaIndex = null)
    {
        var evaluated = Expression.Evaluate(batch);

        if (evaluated is BooleanColumnValue array)
        {
            if (array.DataType != ColumnDataType.Boolean)
            {
                throw new InvalidOperationException("NOT or Bitwise Not can't be evaluated.  The expression must be a boolean or integer.");
            }

            return array.Invert();
        }

        throw new InvalidOperationException("NOT or Bitwise Not can't be evaluated.  The expression must be a boolean or integer.");
    }
}