using SqlParser.Ast;
using System.Collections;
using Prequel.Values;
using Prequel.Data;
using Prequel.Logical;

namespace Prequel.Physical.Expressions;

/// <summary>
/// Binary physical expression
/// </summary>
/// <param name="Left">Left side expression</param>
/// <param name="Op">Binary expression operator</param>
/// <param name="Right">Right side expression</param>
internal record Binary(IPhysicalExpression Left, BinaryOperator Op, IPhysicalExpression Right) : IPhysicalExpression
{
    /// <summary>
    /// Gets the binary evaluation data type.  Comparisons always return
    /// a boolean expression; math expressions and nested expressions
    /// are also supported
    /// </summary>
    /// <param name="schema">Schema containing binary expression field definitions</param>
    /// <returns>Binary column data type</returns>
    public ColumnDataType GetDataType(Schema schema)
    {
        return LogicalExtensions.GetBinaryDataType(Left.GetDataType(schema), Op, Right.GetDataType(schema));
    }
    /// <summary>
    /// Evaluates both sides of a binary expression and returns the column value result
    /// </summary>
    /// <param name="batch">Batch to run the binary expression against</param>
    /// <param name="schemaIndex">Unused</param>
    /// <returns>Evaluated column value</returns>
    public ColumnValue Evaluate(RecordBatch batch, int? schemaIndex = null)
    {
        var leftValue = Left.Evaluate(batch);
        var rightValue = Right.Evaluate(batch);

        if (leftValue.Size != rightValue.Size)
        {
            throw new InvalidOperationException("size mismatch");
        }

        //todo check types
        //var leftDataType = leftValue.DataType;
        //var rightDataType = rightValue.DataType;

        //if (leftDataType != rightDataType)
        //{
        //    throw new InvalidOperationException("Data type mismatch");
        //}

        //ArrayColumnValue? scalarResult = null;

        return leftValue switch
        {
            ArrayColumnValue leftArray when rightValue is ScalarColumnValue rightScalar => EvaluateScalarArray(leftArray, rightScalar),
            ScalarColumnValue leftScalar when rightValue is ArrayColumnValue rightArray => EvaluateScalarArray(rightArray, leftScalar),

            _ => Op switch
            {
                BinaryOperator.Lt or BinaryOperator.LtEq or BinaryOperator.Gt or BinaryOperator.GtEq
                    or BinaryOperator.Eq or BinaryOperator.NotEq => Compare(leftValue, rightValue),

                BinaryOperator.Plus => Add(leftValue, rightValue),
                BinaryOperator.Minus => Subtract(leftValue, rightValue),
                BinaryOperator.Multiply => Multiple(leftValue, rightValue),
                BinaryOperator.Divide => Divide(leftValue, rightValue),
                BinaryOperator.Modulo => Modulo(leftValue, rightValue),

                BinaryOperator.And => EvaluateAnd(leftValue, rightValue),
                BinaryOperator.Or => EvaluateOr(leftValue, rightValue),
                //TODO bitwise operators

                _ => throw new NotImplementedException("Binary Evaluation not yet implemented")
            }
        };
    }
    /// <summary>
    /// Evaluates a scalar value on one side of the binary expression against an
    /// array of values on the other side of the value.  For example, an expression
    ///
    /// column_1 > 5
    ///
    /// 
    /// The left side produces an array of values for each row in a batch while the right
    /// side produces a scalar value.  The scalar is converted into an array matching
    /// the size of the right hand side and compared against each value.
    /// </summary>
    /// <param name="array">Array with values to compare</param>
    /// <param name="scalar">Scalar value to compare against each array value</param>
    /// <returns></returns>
    internal ColumnValue EvaluateScalarArray(ArrayColumnValue array, ScalarColumnValue scalar)
    {
        var value = scalar.Value.RawValue;
        var valueArray = Enumerable.Range(0, array.Size).Select(_ => value).ToList();
        var scalarArray = new ArrayColumnValue(valueArray, array.DataType);

        return Compare(array, scalarArray);
    }
    /// <summary>
    /// Evaluate an AND binary expression
    /// </summary>
    /// <param name="leftValue">Left side column value</param>
    /// <param name="rightValue">Right side column value</param>
    /// <returns>Boolean value.  True if the condition succeeded; otherwise false.</returns>
    internal static ColumnValue EvaluateAnd(ColumnValue leftValue, ColumnValue rightValue)
    {
        var data = new bool[leftValue.Size];

        for (var i = 0; i < leftValue.Size; i++)
        {
            var left = (bool)(leftValue.GetValue(i) ?? false);
            var right = (bool)(rightValue.GetValue(i) ?? false);

            var value = left && right;

            data[i] = value;
        }

        return new BooleanColumnValue(data);
    }
    /// <summary>
    /// Evaluate an OR binary expression
    /// </summary>
    /// <param name="leftValue">Left side column value</param>
    /// <param name="rightValue">Right side column value</param>
    /// <returns>Boolean value.  True if the condition succeeded; otherwise false.</returns>
    private static ColumnValue EvaluateOr(ColumnValue leftValue, ColumnValue rightValue)
    {
        var data = new bool[leftValue.Size];

        for (var i = 0; i < leftValue.Size; i++)
        {
            var left = (bool)(leftValue.GetValue(i) ?? false);
            var right = (bool)(rightValue.GetValue(i) ?? false);

            var value = left || right;

            data[i] = value;
        }

        return new BooleanColumnValue(data);
    }
    /// <summary>
    /// Calculates a value in math expressions such as
    ///
    /// column_1 + column_2
    /// 
    /// </summary>
    /// <param name="leftValue">Left side column value</param>
    /// <param name="rightValue">Right side column value</param>
    /// <param name="calculate">Lambda used th calculate left and right side values</param>
    /// <returns>Array containing calculated value output</returns>
    public static ArrayColumnValue Calculate(ColumnValue leftValue, ColumnValue rightValue, Func<double, double, double> calculate)
    {
        var results = new double[leftValue.Size];

        for (var i = 0; i < leftValue.Size; i++)
        {
            var left = leftValue.GetValue(i);
            var right = rightValue.GetValue(i);

            var value = calculate(Convert.ToDouble(left), Convert.ToDouble(right));

            results[i] = value;
        }

        var outputType = LogicalExtensions.GetMathNumericalCoercion(leftValue.DataType, rightValue.DataType);

        IList data = outputType == ColumnDataType.Integer
            ? results.Select(Convert.ToInt64).ToList()
            : results.ToList();

        return new ArrayColumnValue(data, outputType);
    }
    /// <summary>
    /// Adds two numeric values together
    /// </summary>
    /// <param name="leftValue">Left side numeric value</param>
    /// <param name="rightValue">Right side numeric value</param>
    /// <returns>Calculated result</returns>
    public static ArrayColumnValue Add(ColumnValue leftValue, ColumnValue rightValue)
    {
        return Calculate(leftValue, rightValue, (l, r) => l + r);
    }
    /// <summary>
    /// Subtracts two numeric values together
    /// </summary>
    /// <param name="leftValue">Left side numeric value</param>
    /// <param name="rightValue">Right side numeric value</param>
    /// <returns>Calculated result</returns>
    public static ArrayColumnValue Subtract(ColumnValue leftValue, ColumnValue rightValue)
    {
        return Calculate(leftValue, rightValue, (l, r) => l - r);
    }
    /// <summary>
    /// Multiplies two numeric values together
    /// </summary>
    /// <param name="leftValue">Left side numeric value</param>
    /// <param name="rightValue">Right side numeric value</param>
    /// <returns>Calculated result</returns>
    public static ArrayColumnValue Multiple(ColumnValue leftValue, ColumnValue rightValue)
    {
        return Calculate(leftValue, rightValue, (l, r) => l * r);

    }
    /// <summary>
    /// Divides two numeric values together
    /// </summary>
    /// <param name="leftValue">Left side numeric value</param>
    /// <param name="rightValue">Right side numeric value</param>
    /// <returns>Calculated result</returns>
    public static ArrayColumnValue Divide(ColumnValue leftValue, ColumnValue rightValue)
    {
        return Calculate(leftValue, rightValue, (l, r) => l / r);
    }
    /// <summary>
    /// Calculates the modulo of two numeric values together
    /// </summary>
    /// <param name="leftValue">Left side numeric value</param>
    /// <param name="rightValue">Right side numeric value</param>
    /// <returns>Calculated result</returns>
    public static ArrayColumnValue Modulo(ColumnValue leftValue, ColumnValue rightValue)
    {
        return Calculate(leftValue, rightValue, (l, r) => l % r);
    }
    /// <summary>
    /// Compares two values for equality
    /// </summary>
    /// <param name="leftValue">Left side value</param>
    /// <param name="rightValue">Right side value</param>
    /// <returns>Calculated result</returns>
    /// <returns></returns>
    public ColumnValue Compare(ColumnValue leftValue, ColumnValue rightValue)
    {
        switch (Op)
        {
            case BinaryOperator.Eq:
            case BinaryOperator.NotEq:
            case BinaryOperator.Gt:
            case BinaryOperator.Lt:
            case BinaryOperator.GtEq:
            case BinaryOperator.LtEq:
                {
                    var bitVector = new bool[leftValue.Size];

                    for (var i = 0; i < leftValue.Size; i++)
                    {
                        var value = CompareValues(leftValue.GetValue(i), rightValue.GetValue(i), leftValue.DataType);
                        bitVector[i] = value;
                    }

                    return new BooleanColumnValue(bitVector);
                }

            case BinaryOperator.Plus:
            case BinaryOperator.Minus:
            case BinaryOperator.Divide:
            case BinaryOperator.Multiply:
            case BinaryOperator.Modulo:
                var values = new object[leftValue.Size];
                for (var i = 0; i < leftValue.Size; i++)
                {
                    var value = CalculateValues(leftValue.GetValue(i), rightValue.GetValue(i), leftValue.DataType);
                    values[i] = value;
                }

                return new ArrayColumnValue(values, leftValue.DataType);


            default:
                throw new NotImplementedException("$Comparison type {Op} is not implemented");
        }

    }

    /// <summary>
    /// Compares two values using type-specific comparisons
    /// </summary>
    /// <param name="left">Left side value</param>
    /// <param name="right">Right side value</param>
    /// <param name="dataType">Data type of the values being compared</param>
    /// <returns>Calculated result</returns>
    private bool CompareValues(object? left, object? right, ColumnDataType dataType)
    {
        switch (dataType)
        {
            case ColumnDataType.Utf8:
                return CompareStrings(left, right);

            case ColumnDataType.Integer:
                return CompareIntegers(left, right);

            case ColumnDataType.Double:
                return CompareDoubles(left, right);

            case ColumnDataType.Boolean:
                return CompareBooleans(left, right);

            case ColumnDataType.Date32:
                return CompareDates(left, right);

            default:
                throw new NotImplementedException("CompareValues data type not implemented");
        }
    }
    /// <summary>
    /// Runs calculations on integer (long) and double values
    /// </summary>
    /// <param name="left">Left side value</param>
    /// <param name="right">Right side value</param>
    /// <param name="dataType">Data type of the values being compared</param>
    /// <returns>Calculated result</returns>
    private double CalculateValues(object? left, object? right, ColumnDataType dataType)
    {
        switch (dataType)
        {
            //case ColumnDataType.Utf8:
            //    return CompareStrings(left, right);

            case ColumnDataType.Integer:
                return CalculateIntegers(left, right);

            case ColumnDataType.Double:
                return CalculateDoubles(left, right);

            //case ColumnDataType.Boolean:
            //    return CompareBooleans(left, right);

            //case ColumnDataType.Date32:
            //    return CompareDates(left, right);

            default:
                throw new NotImplementedException("CompareValues data type not implemented");
        }
    }
    /// <summary>
    /// Compares two string values
    /// </summary>
    /// <param name="left">Left side value</param>
    /// <param name="right">Right side value</param>
    /// <returns>True if the comparision succeeds; otherwise false</returns>
    private bool CompareStrings(object? left, object? right)
    {
        var leftValue = left as string ?? (left ?? "").ToString();
        var rightValue = right as string ?? (right ?? "").ToString();

        return Op switch
        {
            BinaryOperator.Eq => leftValue == rightValue,
            BinaryOperator.NotEq => leftValue != rightValue,
            _ => throw new NotImplementedException("CompareStrings not implemented for operator")
        };
    }
    /// <summary>
    /// Compares two integer values
    /// </summary>
    /// <param name="left">Left side value</param>
    /// <param name="right">Right side value</param>
    /// <returns>True if the comparision succeeds; otherwise false</returns>
    private bool CompareIntegers(object? left, object? right)
    {
        var leftValue = Convert.ToInt64(left);
        var rightValue = Convert.ToInt64(right);

        return Op switch
        {
            BinaryOperator.Eq => leftValue == rightValue,
            BinaryOperator.NotEq => leftValue != rightValue,
            BinaryOperator.Gt => leftValue > rightValue,
            BinaryOperator.Lt => leftValue < rightValue,
            BinaryOperator.GtEq => leftValue >= rightValue,
            BinaryOperator.LtEq => leftValue <= rightValue,

            _ => throw new NotImplementedException("CompareIntegers not implemented for operator")
        };
    }
    /// <summary>
    /// Compares two double values
    /// </summary>
    /// <param name="left">Left side value</param>
    /// <param name="right">Right side value</param>
    /// <returns>True if the comparision succeeds; otherwise false</returns>
    private bool CompareDoubles(object? left, object? right)
    {
        var leftValue = Convert.ToDouble(left ?? double.NaN);
        var rightValue = Convert.ToDouble(right ?? double.NaN);

        return Op switch
        {
            BinaryOperator.Eq => leftValue == rightValue,
            BinaryOperator.NotEq => leftValue != rightValue,
            BinaryOperator.Gt => leftValue > rightValue,
            BinaryOperator.Lt => leftValue < rightValue,
            BinaryOperator.GtEq => leftValue >= rightValue,
            BinaryOperator.LtEq => leftValue <= rightValue,

            _ => throw new NotImplementedException("CompareDecimals not implemented for operator")
        };
    }
    /// <summary>
    /// Compares two boolean values
    /// </summary>
    /// <param name="left">Left side value</param>
    /// <param name="right">Right side value</param>
    /// <returns>True if the comparision succeeds; otherwise false</returns>
    private bool CompareBooleans(object? left, object? right)
    {
        var leftValue = Convert.ToBoolean(left ?? false);
        var rightValue = Convert.ToBoolean(right ?? false);

        return Op switch
        {
            BinaryOperator.Eq => leftValue == rightValue,
            BinaryOperator.NotEq => leftValue != rightValue,
            BinaryOperator.And => leftValue && rightValue,
            BinaryOperator.Or => leftValue || rightValue,

            _ => throw new NotImplementedException("CompareBooleans not implemented for integers yet")
        };
    }
    /// <summary>
    /// Compares two date values
    /// </summary>
    /// <param name="left">Left side value</param>
    /// <param name="right">Right side value</param>
    /// <returns>True if the comparision succeeds; otherwise false</returns>
    private bool CompareDates(object? left, object? right)
    {
        var leftValue = Convert.ToDateTime(left ?? DateTime.MinValue);
        var rightValue = Convert.ToDateTime(right ?? DateTime.MinValue);

        return Op switch
        {
            BinaryOperator.Eq => leftValue == rightValue,
            BinaryOperator.NotEq => leftValue != rightValue,
            BinaryOperator.Gt => leftValue > rightValue,
            BinaryOperator.Lt => leftValue < rightValue,
            BinaryOperator.GtEq => leftValue >= rightValue,
            BinaryOperator.LtEq => leftValue <= rightValue,

            _ => throw new NotImplementedException("CompareDates not implemented for operator")
        };
    }
    /// <summary>
    /// Runs a calculation on two integer values based on the
    /// binary math operator
    /// </summary>
    /// <param name="left">Left side value</param>
    /// <param name="right">Right side value</param>
    /// <returns>Calculated value</returns>
    private double CalculateIntegers(object? left, object? right)
    {
        var leftValue = Convert.ToInt64(left);
        var rightValue = Convert.ToInt64(right);

        return Op switch
        {
            BinaryOperator.Plus => leftValue + rightValue,
            BinaryOperator.Minus => leftValue - rightValue,
            BinaryOperator.Divide => leftValue / rightValue,
            BinaryOperator.Multiply => leftValue * rightValue,
            BinaryOperator.Modulo => leftValue * rightValue,

            _ => throw new NotImplementedException("CalculateIntegers not implemented for operator")
        };
    }
    /// <summary>
    /// Runs a calculation on two double values based on the
    /// binary math operator
    /// </summary>
    /// <param name="left">Left side value</param>
    /// <param name="right">Right side value</param>
    /// <returns>Calculated value</returns>
    private double CalculateDoubles(object? left, object? right)
    {
        var leftValue = Convert.ToDouble(left ?? double.NaN);
        var rightValue = Convert.ToDouble(right ?? double.NaN);

        return Op switch
        {
            BinaryOperator.Plus => leftValue + rightValue,
            BinaryOperator.Minus => leftValue - rightValue,
            BinaryOperator.Divide => leftValue / rightValue,
            BinaryOperator.Multiply => leftValue * rightValue,
            BinaryOperator.Modulo => leftValue * rightValue,

            _ => throw new NotImplementedException("CalculateIntegers not implemented for operator")
        };
    }
}