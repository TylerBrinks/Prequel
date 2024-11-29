using Prequel.Data;
using Prequel.Values;

namespace Prequel.Physical.Expressions;

/// <summary>
/// Physical IN expression
/// </summary>
/// <param name="Expression"></param>
/// <param name="List"></param>
/// <param name="Negated"></param>
/// <param name="Filter"></param>
internal record InList(IPhysicalExpression Expression, List<IPhysicalExpression> List, bool Negated, List<Literal> Filter) : IPhysicalExpression
{
    /// <summary>
    /// IN operations are always boolean
    /// </summary>
    /// <param name="schema">Unused</param>
    /// <returns>Boolean data type</returns>
    public ColumnDataType GetDataType(Schema schema)
    {
        return ColumnDataType.Boolean;
    }
    /// <summary>
    /// Checks each batch value against a list of values.  Values
    /// contained in the IN expression are filtered and returned
    /// </summary>
    /// <param name="batch">Batch containing data to filter</param>
    /// <param name="schemaIndex">Unused</param>
    /// <returns>Boolean array mapping which fields exist in the IN expression</returns>
    public ColumnValue Evaluate(RecordBatch batch, int? schemaIndex = null)
    {
        var value = Expression.Evaluate(batch);
        BooleanColumnValue array;

        if (Filter.Any())
        {
            var map = Contains(value);
            array = new BooleanColumnValue(map);
        }
        else
        {
            //todo??
            throw new NotImplementedException("InList: eval, not yet implemented");
        }

        return array;
    }
    /// <summary>
    /// Creates a bit mask of the values that are contained
    /// within the IN expression
    /// </summary>
    /// <param name="values">Values to interrogate</param>
    /// <returns>Bit mask flagging which values are contained in the IN expression</returns>
    public bool[] Contains(ColumnValue values)
    {
        var results = new bool[values.Size];

        for (var i = 0; i < values.Size; i++)
        {
            var value = values.GetValue(i);

            var contained = Filter.Any(f => f.Value.IsEqualTo(value));

            if (Negated)
            {
                contained = !contained;
            }

            results[i] = contained;
        }

        return results;
    }
}