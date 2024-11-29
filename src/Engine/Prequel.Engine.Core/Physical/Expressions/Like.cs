using Prequel.Engine.Core;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Values;

namespace Prequel.Engine.Core.Physical.Expressions;

/// <summary>
/// Physical LIKE expression
/// </summary>
/// <param name="Negated">True if the expression is negated; otherwise false.</param>
/// <param name="CaseSensitive">True if the comparison is case-sensitive; otherwise false.</param>
/// <param name="Expression">LIKE Expression definition</param>
/// <param name="Pattern">LIKE expression pattern</param>
internal record Like(bool Negated, bool CaseSensitive, IPhysicalExpression Expression, IPhysicalExpression Pattern) : IPhysicalExpression
{
    /// <summary>
    /// LIKE operations are always boolean
    /// </summary>
    /// <param name="schema">Unused</param>
    /// <returns>Boolean data type</returns>
    public ColumnDataType GetDataType(Schema schema)
    {
        return ColumnDataType.Boolean;
    }
    /// <summary>
    /// Checks each batch value against a pattern.  Values
    /// matching the LIKE expression pattern are filtered and returned
    /// </summary>
    /// <param name="batch">Batch containing data to filter</param>
    /// <param name="schemaIndex">Unused</param>
    /// <returns>Boolean array mapping which fields exist in the IN expression</returns>
    public ColumnValue Evaluate(RecordBatch batch, int? schemaIndex = null)
    {
        var expressionValue = Expression.Evaluate(batch);
        var patternValue = Pattern.Evaluate(batch);
        var expressionDataType = expressionValue.DataType;
        var patternDataType = patternValue.DataType;

        if (expressionDataType != patternDataType)
        {
            throw new NotImplementedException("Cannot evaluate Like or ILike expressions with different types.");
        }

        var scalar = (ScalarColumnValue)patternValue;

        if (expressionValue is ArrayColumnValue array)
        {
            return EvaluateArrayScalar(array, scalar);
        }
        var rows = batch.RowCount;
        var arrayValues = new List<string>(rows);

        for (var i = 0; i < rows; i++)
        {
            arrayValues.Add(expressionValue.GetValue(i)?.ToString() ?? ""); // todo should this be null? likely.
        }

        var arrayValue = new ArrayColumnValue(arrayValues, expressionDataType);

        return EvaluateArrayScalar(arrayValue, scalar);
    }
    /// <summary>
    /// Matches values against a pattern checking for matches.
    /// </summary>
    /// <param name="array">Array with values to interrogate</param>
    /// <param name="scalar">LIKE pattern in string format</param>
    /// <returns>Bit mask flagging which values are contained in the IN expression</returns>
    private BooleanColumnValue EvaluateArrayScalar(ArrayColumnValue array, ScalarColumnValue scalar)
    {
        var values = new bool[array.Size];
        var pattern = (string)scalar.Value.RawValue!;

        for (var i = 0; i < array.Values.Count; i++)
        {
            var val = array.Values[i];
            string? compareValue;

            if (val is string s)
            {
                compareValue = s;
            }
            else
            {
                compareValue = val?.ToString() ?? null;
            }

            if (compareValue == null)
            {
                values[i] = false;
                continue;
            }

            values[i] = compareValue.SqlLike(pattern, CaseSensitive);
        }

        var map = new BooleanColumnValue(values);

        return Negated
            ? map.Invert()
            : new BooleanColumnValue(values);
    }
}