using Prequel.Engine.Core;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Values;

namespace Prequel.Engine.Core.Physical.Expressions;

/// <summary>
/// Case physical expression
/// </summary>
/// <param name="Expression">Case expression</param>
/// <param name="WhenThenExpressions">When expression</param>
/// <param name="ElseExpression">Optional else expression</param>
internal record Case(
    IPhysicalExpression? Expression,
    List<(IPhysicalExpression When, IPhysicalExpression Then)> WhenThenExpressions,
    IPhysicalExpression? ElseExpression) : IPhysicalExpression
{
    /// <summary>
    /// Gets the data type of the CASE expression
    /// </summary>
    /// <param name="schema">Schema containing data type definitions</param>
    /// <returns>Case expression data type</returns>
    public ColumnDataType GetDataType(Schema schema)
    {
        var dataType = ColumnDataType.Null;

        foreach (var expr in WhenThenExpressions)
        {
            dataType = expr.Then.GetDataType(schema);

            if (dataType != ColumnDataType.Null)
            {
                break;
            }
        }

        if (dataType == ColumnDataType.Null && ElseExpression != null)
        {
            dataType = ElseExpression.GetDataType(schema);
        }

        return dataType;
    }
    /// <summary>
    /// Evaluates a CASE statement
    /// </summary>
    /// <param name="batch">Batch containing data to evaluate</param>
    /// <param name="schemaIndex">Unused</param>
    /// <returns>Column value result from the case comparison</returns>
    public ColumnValue Evaluate(RecordBatch batch, int? schemaIndex = null)
    {
        if (Expression != null)
        {
            return CaseWhenWithExpression(batch);
        }

        return CaseWhenNoExpression(batch);
    }
    /// <summary>
    /// Case evaluation when no expression exists
    /// </summary>
    /// <param name="batch">Batch containing data to evaluate</param>
    /// <returns>Column value result from the case comparison</returns>
    private ColumnValue CaseWhenNoExpression(RecordBatch batch)
    {
        var rowCount = batch.RowCount;

        var dataType = GetDataType(batch.Schema);
        var currentValue = RecordBatch.CreateRecordArray(dataType).FillWithNull(batch.RowCount);

        var remainder = new BooleanArray(Enumerable.Range(0, batch.RowCount).Select(_ => true));

        foreach (var expr in WhenThenExpressions)
        {
            var (when, then) = expr;

            var whenValue = EvaluateSelection(when, batch, remainder);
            var whenArray = (BooleanColumnValue)IntoArray(whenValue, rowCount);
            var whenMask = PrepNullMaskFilter(whenArray);

            var thenValue = EvaluateSelection(then, batch, whenMask);
            var thenArray = IntoArray(thenValue, rowCount);

            AssignCaseValues(thenArray, currentValue, whenMask);

            for (var i = 0; i < whenArray.Size; i++)
            {
                remainder[i] &= !whenMask[i];
            }
        }

        if (ElseExpression != null)
        {
            var elseValue = EvaluateSelection(ElseExpression, batch, remainder);
            var elseArray = IntoArray(elseValue, rowCount);

            AssignCaseValues(elseArray, currentValue, remainder);
        }

        return new ArrayColumnValue(currentValue.Values, dataType);
    }
    /// <summary>
    /// Case evaluation when an expression is provided
    /// </summary>
    /// <param name="batch">Batch containing data to evaluate</param>
    /// <returns>Column value result from the case comparison</returns>
    private ColumnValue CaseWhenWithExpression(RecordBatch batch)
    {
        var rowCount = batch.RowCount;
        var dataType = GetDataType(batch.Schema);

        var baseValue = Expression!.Evaluate(batch);
        var baseArray = (ArrayColumnValue)IntoArray(baseValue, batch.RowCount);
        var baseNulls = baseArray.Values.Cast<object>().Select(v => v is null).ToList();

        var currentValue = RecordBatch.CreateRecordArray(dataType).FillWithNull(batch.RowCount);
        var remainder = new BooleanArray(baseNulls.Select(v => !v));

        foreach (var expr in WhenThenExpressions)
        {
            var (when, then) = expr;

            var whenValue = EvaluateSelection(when, batch, remainder);
            var whenArray = (ArrayColumnValue)IntoArray(whenValue, rowCount);
            var whenMatch = whenArray.Values.Cast<object>().Zip(baseArray.Values.Cast<object>())
                .Select(v => v.First.CompareValueEquality(v.Second))
                .ToList();

            var whenMask = PrepNullMaskFilter(new BooleanColumnValue(whenMatch));

            var thenValue = EvaluateSelection(then, batch, whenMask);
            var thenArray = IntoArray(thenValue, rowCount);

            AssignCaseValues(thenArray, currentValue, whenMask);

            for (var i = 0; i < whenArray.Size; i++)
            {
                remainder[i] &= !whenMask[i];
            }
        }

        if (ElseExpression != null)
        {
            for (var i = 0; i < baseNulls.Count; i++)
            {
                remainder[i] = baseNulls[i] | remainder[i];
            }

            var elseValue = EvaluateSelection(ElseExpression, batch, remainder);
            var elseArray = IntoArray(elseValue, rowCount);

            AssignCaseValues(elseArray, currentValue, remainder);
        }

        return new ArrayColumnValue(currentValue.Values, dataType);
    }
    /// <summary>
    /// Assigns values to each column value where the bit mask is true.
    /// </summary>
    /// <param name="value">Column value containing values to use during update</param>
    /// <param name="array">Record array to assign values</param>
    /// <param name="mask">Mask flagging which values to update</param>
    private static void AssignCaseValues(ColumnValue? value, RecordArray? array, BooleanArray mask)
    {
        if (value == null || array == null)
        {
            return;
        }

        for (var i = 0; i < value.Size; i++)
        {
            if (mask[i])
            {
                array.Values[i] = value.GetValue(i);
            }
        }
    }
    /// <summary>
    /// Creates a bit mask filling in nulls as false.
    /// </summary>
    /// <param name="array">Array to convert to a bit mask</param>
    /// <returns>BooleanArray bit mask</returns>
    private static BooleanArray PrepNullMaskFilter(BooleanColumnValue array)
    {
        var data = array.Values.Select(v => v ?? false).ToList();

        return new BooleanArray(data);
    }
    /// <summary>
    /// Evaluates an expression against a temporary batch and fills in the
    /// bit mask with the results of the comparision operation.
    /// </summary>
    /// <param name="expression">Expressions to compare and filter</param>
    /// <param name="batch">Batch containing data to evaluate</param>
    /// <param name="selection">Selection filter providing the bit mask</param>
    /// <returns></returns>
    private static ColumnValue EvaluateSelection(IPhysicalExpression expression, RecordBatch batch, BooleanArray selection)
    {
        var filter = selection.Values.Cast<bool>().ToArray();
        var tempBatch = batch.Copy();
        tempBatch.Filter(filter);

        var tempResult = expression.Evaluate(tempBatch);

        if (batch.RowCount == tempBatch.RowCount)
        {
            return tempResult;
        }

        if (tempResult is ScalarColumnValue)
        {
            return tempResult;

        }

        var mask = selection.Values.Cast<bool?>().ToArray();
        return Scatter(mask, (BooleanColumnValue)tempResult);
    }
    /// <summary>
    /// Converts scalar column values into repeated array values.
    /// </summary>
    /// <param name="value">Column to convert to an array</param>
    /// <param name="rowCount">Number of rows Required in the column</param>
    /// <returns>Array column value</returns>
    private static ColumnValue IntoArray(ColumnValue value, int rowCount)
    {
        if (value is ScalarColumnValue scalar)
        {
            return scalar.ToValueArray(rowCount);
        }

        return value;
    }
    /// <summary>
    /// Scatter array by boolean mask.  When the mask evaluates to true, the
    /// next values are filled in until a false is reached.
    /// </summary>
    /// <param name="mask">Bit mask used to determine where to insert true values</param>
    /// <param name="infill">All values of the array to scatter according to the mask</param>
    /// <returns>Boolean column value filled in according to the mask</returns>
    private static BooleanColumnValue Scatter(IEnumerable<bool?> mask, BooleanColumnValue infill)
    {
        var values = mask.Select(m => m == false ? (bool?)null : false).ToList();

        var notNullIndices = new List<int>();

        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (value is null)
            {
                continue;
            }
            notNullIndices.Add(i);
        }

        var notNullIndex = notNullIndices.FirstOrDefault();
        var position = 0;

        foreach (var val in infill.Values)
        {
            values[notNullIndex] = val;
            position++;

            if (position < notNullIndices.Count)
            {
                notNullIndex = notNullIndices[position];
            }
            else
            {
                break;
            }
        }

        return new BooleanColumnValue(values.ToArray());
    }
}