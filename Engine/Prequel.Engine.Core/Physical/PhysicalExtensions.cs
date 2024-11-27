using Prequel.Engine.Core.Physical.Functions;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Logical;
using Prequel.Engine.Core.Logical.Expressions;
using Prequel.Engine.Core.Physical.Expressions;
using Prequel.Engine.Core.Values;

namespace Prequel.Engine.Core.Physical;

internal static class PhysicalExtensions
{
    /// <summary>
    /// Converts a column value to an array column value
    /// </summary>
    /// <param name="scalar">Scalar value to convert</param>
    /// <param name="size">Size of the array</param>
    /// <returns>Converted ArrayColumnValue</returns>
    internal static ArrayColumnValue ToValueArray(this ColumnValue scalar, int size)
    {
        if (scalar is ArrayColumnValue a)
        {
            return a;
        }

        var value = ((ScalarColumnValue)scalar).Value.RawValue;
        var list = new List<object?>(size);

        for (var i = 0; i < size; i++)
        {
            list.Add(value ?? null);
        }
        var array = new ArrayColumnValue(list, scalar.DataType);

        return array;
    }
    /// <summary>
    /// Creates a physical group expression from a logical group expression
    /// </summary>
    /// <param name="groupExpressions">Logical group expression</param>
    /// <param name="inputDataSchema">Input data schema</param>
    /// <param name="inputSchema">Input schema</param>
    /// <returns>GroupBy expression</returns>
    internal static GroupBy CreateGroupingPhysicalExpression(
        this List<ILogicalExpression> groupExpressions,
        Schema inputDataSchema,
        Schema inputSchema)
    {
        if (groupExpressions.Count == 1)
        {
            var expr = groupExpressions[0].CreatePhysicalExpression(inputDataSchema, inputSchema);
            var name = groupExpressions[0].CreatePhysicalName(true);

            var groupExpr = new List<(IPhysicalExpression Expression, string Name)>
                {
                    (expr, name)
                };

            return GroupBy.NewSingle(groupExpr);
        }

        var group = groupExpressions.Select(e =>
            (
                e.CreatePhysicalExpression(inputDataSchema, inputSchema),
                e.CreatePhysicalName(true)
            ))
            .ToList();

        return GroupBy.NewSingle(group);
    }
    /// <summary>
    /// Converts aggregate function types into the correct column types
    /// based on the type of calculation
    /// </summary>
    /// <param name="fn">Aggregate function</param>
    /// <param name="inputTypes">List of function input types</param>
    /// <returns>List of column types</returns>
    internal static List<ColumnDataType> CoerceTypes(AggregateFunction fn, List<ColumnDataType> inputTypes)
    {
        switch (fn.FunctionType)
        {
            case AggregateFunctionType.Count:
            case AggregateFunctionType.ApproxDistinct:
            //return inputTypes;

            case AggregateFunctionType.ArrayAgg:
            case AggregateFunctionType.Min:
            case AggregateFunctionType.Max:
            //return inputTypes;

            case AggregateFunctionType.Sum:
            case AggregateFunctionType.Avg:
            case AggregateFunctionType.Median:
            case AggregateFunctionType.StdDev:
            case AggregateFunctionType.StdDevPop:
            case AggregateFunctionType.Variance:
            case AggregateFunctionType.VariancePop:
            case AggregateFunctionType.Covariance:
            case AggregateFunctionType.CovariancePop:
                return inputTypes;


            default:
                throw new NotImplementedException($"Function coercion not yet implemented for {fn.FunctionType}");
        }
    }
    /// <summary>
    /// Gets an aggregate function return type
    /// </summary>
    /// <param name="fn">Aggregate function</param>
    /// <param name="inputPhysicalTypes">List of function input types</param>
    /// <returns>Column data type</returns>
    internal static ColumnDataType GetReturnTypes(AggregateFunction fn, List<ColumnDataType> inputPhysicalTypes)
    {
        var coercedDataTypes = CoerceTypes(fn, inputPhysicalTypes);

        return fn.FunctionType switch
        {
            AggregateFunctionType.Count or AggregateFunctionType.ApproxDistinct => ColumnDataType.Integer,

            AggregateFunctionType.Min or AggregateFunctionType.Max => coercedDataTypes[0],

            AggregateFunctionType.Sum => SumReturnType(coercedDataTypes[0]),

            AggregateFunctionType.Avg => NumericReturnType("AVG", coercedDataTypes[0]),

            AggregateFunctionType.Median
                or AggregateFunctionType.StdDev
                or AggregateFunctionType.StdDevPop
                or AggregateFunctionType.Variance
                or AggregateFunctionType.VariancePop
                or AggregateFunctionType.Covariance
                or AggregateFunctionType.CovariancePop
                => NumericReturnType(fn.FunctionType.ToString(), coercedDataTypes[0]),


            _ => throw new NotImplementedException("GetReturnTypes not implemented")
        };

        ColumnDataType SumReturnType(ColumnDataType dataType)
        {
            return dataType switch
            {
                ColumnDataType.Integer => ColumnDataType.Integer,
                ColumnDataType.Double => ColumnDataType.Double,
                _ => throw new InvalidOperationException($"SUM does not support data type {dataType}")
            };
        }

        ColumnDataType NumericReturnType(string functionName, ColumnDataType dataType)
        {
            return dataType switch
            {
                ColumnDataType.Integer
                    or ColumnDataType.Double
                    //or ColumnDataType.Integer | ColumnDataType.Double
                    => ColumnDataType.Double,
                _ => throw new InvalidOperationException($"{functionName} does not support data type {dataType}")
            };
        }
    }
    /// <summary>
    /// Creates an aggregate expression from a logical aggregate function
    /// </summary>
    /// <param name="fn">Aggregate function</param>
    /// <param name="distinct">True if distinct; otherwise false</param>
    /// <param name="inputPhysicalExpressions">List of function input types</param>
    /// <param name="physicalSchema">Schema with field definitions</param>
    /// <param name="name">Function name</param>
    /// <returns>Aggregate expression</returns>
    internal static Aggregate CreateAggregateExpression(
        AggregateFunction fn,
        bool distinct,
        List<IPhysicalExpression> inputPhysicalExpressions,
        Schema physicalSchema,
        string name)
    {
        var inputPhysicalTypes = inputPhysicalExpressions.Select(e => e.GetDataType(physicalSchema)).ToList();
        var returnType = GetReturnTypes(fn, inputPhysicalTypes);

        switch (fn.FunctionType, distinct)
        {
            case (AggregateFunctionType.Count, _):
                return new CountFunction(inputPhysicalExpressions[0], name, returnType);

            case (AggregateFunctionType.Sum, _):
                return new SumFunction(inputPhysicalExpressions[0], name, returnType);

            case (AggregateFunctionType.Min, _):
                return new MinFunction(inputPhysicalExpressions[0], name, returnType);

            case (AggregateFunctionType.Max, _):
                return new MaxFunction(inputPhysicalExpressions[0], name, returnType);

            case (AggregateFunctionType.Avg, _):
                return new AverageFunction(inputPhysicalExpressions[0], name, returnType);

            case (AggregateFunctionType.Median, _):
                return new MedianFunction(inputPhysicalExpressions[0], name, returnType);

            case (AggregateFunctionType.StdDev, _):
                return new StandardDeviationFunction(inputPhysicalExpressions[0], name, returnType, StatisticType.Sample);

            case (AggregateFunctionType.StdDevPop, _):
                return new StandardDeviationFunction(inputPhysicalExpressions[0], name, returnType, StatisticType.Population);

            case (AggregateFunctionType.Variance, _):
                return new VarianceFunction(inputPhysicalExpressions[0], name, returnType, StatisticType.Sample);

            case (AggregateFunctionType.VariancePop, _):
                return new VarianceFunction(inputPhysicalExpressions[0], name, returnType, StatisticType.Population);

            case (AggregateFunctionType.Covariance, _):
                return new CovarianceFunction(inputPhysicalExpressions[0], inputPhysicalExpressions[1],
                    name, returnType, StatisticType.Sample);

            case (AggregateFunctionType.CovariancePop, _):
                return new CovarianceFunction(inputPhysicalExpressions[0], inputPhysicalExpressions[1],
                    name, returnType, StatisticType.Population);

            default:
                throw new NotImplementedException($"Aggregate function not yet implemented: {fn.FunctionType}");
        }
    }
    /// <summary>
    /// Creates an aggregate expression from a logical expression
    /// </summary>
    /// <param name="expression">Logical expression to convert</param>
    /// <param name="logicalSchema">Schema with field definitions</param>
    /// <param name="physicalSchema">Physical schema with field definitions</param>
    /// <returns>Aggregate expression</returns>
    internal static Aggregate CreateAggregateExpression(this ILogicalExpression expression, Schema logicalSchema, Schema physicalSchema)
    {
        //todo handle alias
        var name = expression.CreatePhysicalName(true);

        return CreateAggregateExprWithName(expression, name, logicalSchema, physicalSchema);
    }
    /// <summary>
    /// Creates an aggregate expression from a logical expression
    /// </summary>
    /// <param name="expression">Logical expression to convert</param>
    /// <param name="name">Aggregate name</param>
    /// <param name="logicalSchema">Schema with field definitions</param>
    /// <param name="physicalSchema">Physical schema with field definitions</param>
    /// <returns>Aggregate expression</returns>
    internal static Aggregate CreateAggregateExprWithName(ILogicalExpression expression, string name, Schema logicalSchema, Schema physicalSchema)
    {
        switch (expression)
        {
            case AggregateFunction fn:
                var args = fn.Args.Select(e => e.CreatePhysicalExpression(logicalSchema, physicalSchema)).ToList();
                return CreateAggregateExpression(fn, fn.Distinct, args, physicalSchema, name);

            default:
                throw new NotImplementedException("Aggregate function not implemented");
        }
    }
    /// <summary>
    /// 
    /// </summary>
    /// <param name="expression">Logical expression to convert</param>
    /// <param name="inputSchema">Input schema with field definitions</param>
    /// <param name="sortSchema">Sort schema with field definitions</param>
    /// <param name="ascending">True if ascending; otherwise false</param>
    /// <returns>Physical sort expression</returns>
    internal static PhysicalSortExpression CreatePhysicalSortExpression(
        this ILogicalExpression expression,
        Schema sortSchema,
        Schema inputSchema,
        bool ascending)
    {
        var physicalExpression = expression.CreatePhysicalExpression(sortSchema, inputSchema);

        return new PhysicalSortExpression(physicalExpression, sortSchema, inputSchema, ascending);
    }
}
