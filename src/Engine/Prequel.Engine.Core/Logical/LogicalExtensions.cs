using System.Runtime.InteropServices;
using SqlParser.Ast;

using static SqlParser.Ast.Expression;

using Expression = SqlParser.Ast.Expression;
using Aggregate = Prequel.Engine.Core.Logical.Plans.Aggregate;
using Binary = Prequel.Engine.Core.Logical.Expressions.Binary;
using Column = Prequel.Engine.Core.Logical.Expressions.Column;
using Literal = Prequel.Engine.Core.Logical.Expressions.Literal;
using InList = Prequel.Engine.Core.Logical.Expressions.InList;
using Between = Prequel.Engine.Core.Logical.Expressions.Between;
using Like = Prequel.Engine.Core.Logical.Expressions.Like;
using Case = Prequel.Engine.Core.Logical.Expressions.Case;
using Cast = Prequel.Engine.Core.Logical.Expressions.Cast;
using Wildcard = Prequel.Engine.Core.Logical.Expressions.Wildcard;
using Join = Prequel.Engine.Core.Logical.Plans.Join;
using Schema = Prequel.Engine.Core.Data.Schema;
using Prequel.Engine.Core.Values;
using Prequel.Engine.Core.Logical.Plans;
using Prequel.Engine.Core.Logical.Values;
using Prequel.Engine.Core;
using Prequel.Engine.Core.Logical.Expressions;
using Prequel.Engine.Core.Physical.Expressions;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Logical.Rules;
using Prequel.Engine.Core.Physical.Joins;

namespace Prequel.Engine.Core.Logical;

public static class LogicalExtensions
{
    #region Logical Expression
    /// <summary>
    /// Creates a name for the underlying expression based on the
    /// expression's type and (optionally) contained value
    /// </summary>
    /// <param name="expression">Logical expression</param>
    /// <returns>Expression name</returns>
    /// <exception cref="NotImplementedException"></exception>
    internal static string CreateLogicalName(this ILogicalExpression expression)
    {
        return (expression switch
        {
            Alias alias => alias.Name,
            Column column => column.FlatName,
            Binary binary => $"{binary.Left.CreateLogicalName()} {binary.Op.GetDisplayText()} {binary.Right.CreateLogicalName()}",
            AggregateFunction fn => GetFunctionName(fn, fn.Distinct, fn.Args),
            Literal { Value.RawValue: not null } literal => literal.Value.RawValue.ToString(),
            Like like => GetLikeName(like),
            Case @case => GetCaseName(@case),
            // cast
            // not
            // is null
            // is not null
            Wildcard => "*",

            _ => throw new NotImplementedException("need to implement")
        })!;

        static string GetFunctionName(AggregateFunction fn, bool distinct, IEnumerable<ILogicalExpression> args)
        {
            var names = args.Select(CreateLogicalName).ToList();
            var distinctName = distinct ? "DISTINCT " : string.Empty;
            var functionName = fn.FunctionType.ToString().ToUpperInvariant();

            return $"{functionName}({distinctName}{string.Join(",", names)})";
        }

        static string GetLikeName(Like like)
        {
            var likeName = like.CaseSensitive ? "LIKE" : "ILIKE";

            var negated = like.Negated ? $"NOT {likeName}" : $"{likeName}";
            var escape = like.EscapeCharacter != null ? $"CHAR '{like.EscapeCharacter}'" : string.Empty;

            return $"{like.Expression} {negated} {like.Pattern} {escape}";
        }

        static string GetCaseName(Case @case)
        {
            var name = "CASE ";

            if (@case.Expression != null)
            {
                var exprName = @case.Expression.CreateLogicalName();
                name += $"{exprName} ";
            }

            foreach (var (w, t) in @case.WhenThenExpression)
            {
                var when = w.CreateLogicalName();
                var then = t.CreateLogicalName();
                name += $"WHEN {when} THEN {then} ";
            }

            if (@case.ElseExpression != null)
            {
                var elseName = @case.ElseExpression.CreateLogicalName();
                name += $"ELSE {elseName} ";
            }

            name += "END";
            return name;
        }
    }
    /// <summary>
    /// Iterates a list of expressions and appends unique found across
    /// all expressions to a given HashSet
    /// </summary>
    /// <param name="expressions">Logical expressions to iterate and convert to columns</param>
    /// <param name="accumulator">HashSet to append with unique columns</param>
    internal static void ExpressionListToColumns(this List<ILogicalExpression> expressions, HashSet<Column> accumulator)
    {
        foreach (var expr in expressions)
        {
            expr.ExpressionToColumns(accumulator);
        }
    }
    /// <summary>
    /// Converts a logical expression into one or more columns.  A single expression
    /// may yield multiple columns.  A Binary expression, for example, may compare
    /// one column to another and yield a pair of columns
    /// </summary>
    /// <param name="expression">Logical expressions to convert</param>
    /// <param name="accumulator">HashSet to append with unique columns</param>
    internal static void ExpressionToColumns(this ILogicalExpression expression, HashSet<Column> accumulator)
    {
        expression.Apply(expr =>
        {
            try
            {
                Inspect((ILogicalExpression)expr);

                return VisitRecursion.Continue;
            }
            catch
            {
                return VisitRecursion.Stop;
            }
        });
        return;

        void Inspect(ILogicalExpression expr)
        {
            switch (expr)
            {
                case Column col:
                    accumulator.Add(col);
                    break;

                case ScalarVariable sv:
                    accumulator.Add(new Column(string.Join(".", sv.Names)));
                    break;
            }
        }
    }
    /// <summary>
    /// Convenience method to extract columns without needing an external hash set
    /// for every call to ExpressionToColumns
    /// </summary>
    /// <param name="expression">Expression to query</param>
    /// <returns>Hash set containing the expression's columns</returns>
    internal static HashSet<Column> ToColumns(this ILogicalExpression expression)
    {
        var columns = new HashSet<Column>();

        expression.ExpressionToColumns(columns);

        return columns;
    }
    /// <summary>
    /// Converts a list of expressions into a list of qualified fields
    /// </summary>
    /// <param name="expressions">Logical expressions to iterate</param>
    /// <param name="plan">Plan containing field definitions</param>
    /// <returns>List of qualified fields</returns>
    internal static List<QualifiedField> ExpressionListToFields(this IEnumerable<ILogicalExpression> expressions, ILogicalPlan plan)
    {
        return expressions.Select(e => e.ToField(plan.Schema)).ToList();
    }
    /// <summary>
    /// Converts a logical expression into a qualified field
    /// </summary>
    /// <param name="expression">Logical expression to convert into a field</param>
    /// <param name="schema">Schema containing the underlying field</param>
    /// <returns>Qualified field</returns>
    internal static QualifiedField ToField(this ILogicalExpression expression, Schema schema)
    {
        var dataType = expression.GetDataType(schema);

        if (expression is Column { Relation: not null } c)
        {
            return new QualifiedField(c.Name, dataType, c.Relation!);
        }

        return QualifiedField.Unqualified(expression.CreateLogicalName(), dataType);
    }
    /// <summary>
    /// Gets the data type for a given logical expression
    /// </summary>
    /// <param name="expression">Logical expression to interrogate</param>
    /// <param name="schema">Schema containing the underlying field and data type</param>
    /// <returns>Expression's return data type</returns>
    /// <exception cref="NotImplementedException"></exception>
    /// <exception cref="InvalidOperationException"></exception>
    internal static ColumnDataType GetDataType(this ILogicalExpression expression, Schema schema)
    {
        return expression switch
        {
            Column column => schema.GetField(column.Name)!.DataType,
            Alias alias => alias.Expression.GetDataType(schema),
            AggregateFunction fn => GetAggregateDataType(fn),
            Binary binary => GetBinaryDataType(binary.Left.GetDataType(schema), binary.Op, binary.Right.GetDataType(schema)),
            Literal literal => literal.Value.DataType,
            Case @case => GetCaseDataType(@case),

            _ => throw new NotImplementedException("GetDataType not implemented for ColumnDataType"),
        };

        ColumnDataType GetAggregateDataType(AggregateFunction function)
        {
            var dataTypes = function.Args.Select(e => e.GetDataType(schema)).ToList();

            return function.FunctionType switch
            {
                AggregateFunctionType.Min or AggregateFunctionType.Max => CoercedTypes(function, dataTypes),
                AggregateFunctionType.Sum or AggregateFunctionType.Count => ColumnDataType.Integer,
                AggregateFunctionType.Avg
                    or AggregateFunctionType.Median
                    or AggregateFunctionType.StdDev
                    or AggregateFunctionType.StdDevPop
                    or AggregateFunctionType.Variance
                    or AggregateFunctionType.VariancePop
                    or AggregateFunctionType.Covariance
                    or AggregateFunctionType.CovariancePop
                    => ColumnDataType.Double,

                _ => throw new NotImplementedException("GetAggregateDataType need to implement"),
            };
        }

        ColumnDataType CoercedTypes(AggregateFunction function, IReadOnlyList<ColumnDataType> inputTypes)
        {
            return function.FunctionType switch
            {
                AggregateFunctionType.Min or AggregateFunctionType.Max => GetMinMaxType(),
                _ => throw new NotImplementedException("CoercedTypes need to implement"),
            };

            ColumnDataType GetMinMaxType()
            {
                if (inputTypes.Count != 1)
                {
                    throw new InvalidOperationException();
                }

                return inputTypes[0];
            }
        }

        ColumnDataType GetCaseDataType(Case @case)
        {
            var thenTypes = @case.WhenThenExpression.Select(e => e.Then.GetDataType(schema)).ToList();
            ColumnDataType? elseType = null;

            if (@case.ElseExpression != null)
            {
                elseType = @case.ElseExpression.GetDataType(schema);
            }

            var caseOrElseType = elseType ?? thenTypes[0];

            return thenTypes.Aggregate(caseOrElseType, GetComparisonCoercion);
        }
    }
    /// <summary>
    /// Gets the data type for a given binary expression
    /// </summary>
    /// <param name="leftDataType">Binary operation left side expression data type</param>
    /// <param name="op">Binary operation</param>
    /// <param name="rightDataType">Binary operation right side expression data type</param>
    /// <returns>Column data type</returns>
    internal static ColumnDataType GetBinaryDataType(ColumnDataType leftDataType, BinaryOperator op, ColumnDataType rightDataType)
    {
        //var leftDataType = GetDataType(binary.Left, schema);
        //var rightDataType = GetDataType(binary.Left, schema);

        var resultType = CoerceBinaryTypes(leftDataType, op, rightDataType);

        return op switch
        {
            BinaryOperator.Eq
                or BinaryOperator.NotEq
                or BinaryOperator.And
                or BinaryOperator.Or
                or BinaryOperator.Lt
                or BinaryOperator.Gt
                or BinaryOperator.GtEq
                or BinaryOperator.LtEq
                // or BinaryOperator.RegexMatch
                // or BinaryOperator.RegexIMatch
                // or BinaryOperator.RegexNotMatch
                // or BinaryOperator.RegexNotIMatch
                // or BinaryOperator.IsDistinctFrom
                // or BinaryOperator.IsNotDistinctFrom
                => ColumnDataType.Boolean,

            _ => resultType
        };
    }
    /// <summary>
    /// Coerces a left and right data type into the appropriate output data type
    /// </summary>
    /// <param name="leftDataType">Binary operation left side expression data type</param>
    /// <param name="op">Binary operation</param>
    /// <param name="rightDataType">Binary operation right side expression data type</param>
    /// <returns>Column data type</returns>
    internal static ColumnDataType CoerceBinaryTypes(ColumnDataType leftDataType, BinaryOperator op, ColumnDataType rightDataType)
    {
        switch (op)
        {
            case BinaryOperator.BitwiseAnd
                or BinaryOperator.BitwiseOr
                or BinaryOperator.BitwiseXor:
                return GetBitwiseCoercion(leftDataType, rightDataType);

            //case BinaryOperator.And or BinaryOperator.Or:
            //    return ColumnDataType.Boolean;
            case BinaryOperator.And or BinaryOperator.Or
                when leftDataType is ColumnDataType.Boolean &&
                     rightDataType is ColumnDataType.Boolean:
                return ColumnDataType.Boolean;

            case BinaryOperator.Eq
                or BinaryOperator.NotEq
                or BinaryOperator.Lt
                or BinaryOperator.Gt
                or BinaryOperator.LtEq
                or BinaryOperator.GtEq:
                return GetComparisonCoercion(leftDataType, rightDataType);

            //case BinaryOperator.Plus
            //    or BinaryOperator.Minus when IsDateOrTime(leftDataType) && IsDateOrTime(rightDataType):
            //    return TemporalAddSubCoercion();

            case BinaryOperator.Plus
                or BinaryOperator.Minus
                or BinaryOperator.Modulo
                or BinaryOperator.Divide
                or BinaryOperator.Multiply:
                return GetMathNumericalCoercion(leftDataType, rightDataType);

            case BinaryOperator.StringConcat:
            default:
                return ColumnDataType.Utf8;
        }
    }
    /// <summary>
    /// Finds the coerced data type for a bitwise operation
    /// </summary>
    /// <param name="leftDataType">Left hand data type</param>
    /// <param name="rightDataType">Right hand data type</param>
    /// <returns>Coerced column data type</returns>
    internal static ColumnDataType GetBitwiseCoercion(ColumnDataType leftDataType, ColumnDataType rightDataType)
    {
        if (!leftDataType.IsNumeric() || !rightDataType.IsNumeric())
        {
            return ColumnDataType.Utf8;
        }

        return (leftDataType, rightDataType) switch
        {
            (ColumnDataType.Double, ColumnDataType.Double)
                or (ColumnDataType.Double, ColumnDataType.Integer)
                or (ColumnDataType.Integer, ColumnDataType.Double)
                or (_, ColumnDataType.Double)
                or (ColumnDataType.Double, _) => ColumnDataType.Double,

            (ColumnDataType.Integer, ColumnDataType.Integer)
                or (ColumnDataType.Integer, _)
                or (_, ColumnDataType.Integer) => ColumnDataType.Integer,

            _ => throw new NotImplementedException("Coercion not implemented")
        };
    }
    /// <summary>
    /// Finds the coerced data type for a comparison operation
    /// </summary>
    /// <param name="leftDataType">Left hand data type</param>
    /// <param name="rightDataType">Right hand data type</param>
    /// <returns>Coerced column data type</returns>
    internal static ColumnDataType GetComparisonCoercion(ColumnDataType leftDataType, ColumnDataType rightDataType)
    {
        if (leftDataType == rightDataType)
        {
            return leftDataType;
        }

        return ComparisonBinaryNumericCoercion(leftDataType, rightDataType) ??
               //DictionaryCoercion() ??
               ComparisonTemporalCoercion(leftDataType, rightDataType) ??
               StringCoercion(leftDataType, rightDataType) ??
               // null coercion
               // StringNumericCoercion()
               throw new NotImplementedException("Coercion not implemented");
    }
    internal static ColumnDataType? StringCoercion(ColumnDataType leftDataType, ColumnDataType rightDataType)
    {
        return leftDataType == ColumnDataType.Utf8 && rightDataType == ColumnDataType.Utf8
            ? ColumnDataType.Utf8
            : null;
    }
    /// <summary>
    /// Coerce the input data types to the correct binary comparison output data type
    /// </summary>
    /// <param name="leftDataType">Left side data type</param>
    /// <param name="rightDataType">Right side data type</param>
    /// <returns>Coerced data type</returns>
    internal static ColumnDataType? ComparisonBinaryNumericCoercion(ColumnDataType leftDataType, ColumnDataType rightDataType)
    {
        if (leftDataType == rightDataType)
        {
            return leftDataType;
        }

        return (leftDataType, rightDataType) switch
        {
            (ColumnDataType.Double, ColumnDataType.Double)
                or (ColumnDataType.Integer, ColumnDataType.Double)
                or (ColumnDataType.Double, ColumnDataType.Integer)
                or (ColumnDataType.Double, _)
                or (_, ColumnDataType.Double) => ColumnDataType.Double,

            (ColumnDataType.Integer, ColumnDataType.Integer)
                or (ColumnDataType.Integer, _)
                or (_, ColumnDataType.Integer) => ColumnDataType.Integer,

            _ => null
        };
    }
    /// <summary>
    /// Coerce the input data types to the correct temporal comparison output data type
    /// </summary>
    /// <param name="leftDataType">Left side data type</param>
    /// <param name="rightDataType">Right side data type</param>
    /// <returns>Coerced data type</returns>
    internal static ColumnDataType? ComparisonTemporalCoercion(ColumnDataType leftDataType, ColumnDataType rightDataType)
    {
        if (leftDataType == rightDataType)
        {
            return leftDataType;
        }

        return (leftDataType, rightDataType) switch
        {
            (ColumnDataType.Date32, ColumnDataType.Utf8)
                or (ColumnDataType.Utf8, ColumnDataType.Date32) => ColumnDataType.Date32,

            _ => null
        };
    }
    /// <summary>
    /// Finds the coerced data type for a math operation
    /// </summary>
    /// <param name="leftDataType">Left hand data type</param>
    /// <param name="rightDataType">Right hand data type</param>
    /// <returns>Coerced column data type</returns>
    internal static ColumnDataType GetMathNumericalCoercion(ColumnDataType leftDataType, ColumnDataType rightDataType)
    {
        if (!leftDataType.IsNumeric() || !rightDataType.IsNumeric())
        {
            return ColumnDataType.Utf8;
        }

        return (leftDataType, rightDataType) switch
        {
            (ColumnDataType.Double, _)
                or (_, ColumnDataType.Double) => ColumnDataType.Double,

            (ColumnDataType.Integer, _)
                or (_, ColumnDataType.Integer) => ColumnDataType.Integer,

            _ => throw new NotImplementedException("Coercion not implemented")
        };
    }
    /// <summary>
    /// Checks if a column data type is numeric
    /// </summary>
    /// <param name="dataType">Data type to evaluate</param>
    /// <returns>True if numeric; otherwise false.</returns>
    internal static bool IsNumeric(this ColumnDataType dataType)
    {
        return dataType is ColumnDataType.Integer or ColumnDataType.Double;
    }
    /// <summary>
    /// Removes an alias and reverts to the underlying field name
    ///  for an expression that has been aliased
    /// </summary>
    /// <param name="expression"></param>
    /// <returns></returns>
    // ReSharper disable once IdentifierTypo
    internal static ILogicalExpression Unalias(this ILogicalExpression expression)
    {
        return expression is not Alias alias ? expression : alias.Expression;
    }
    /// <summary>
    /// Returns a cloned expression, but any of the expressions in the tree may be
    /// replaced or customized by the replacement function.
    ///
    /// The replace function is called repeatedly with expression, starting with
    /// the argument, then descending depth-first through its
    /// descendants. The function chooses to replace or keep (clone) each expression.
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="replacement"></param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    internal static ILogicalExpression CloneWithReplacement(this ILogicalExpression expression,
        Func<ILogicalExpression, ILogicalExpression?> replacement)
    {
        var replacementOpt = replacement(expression);

        if (replacementOpt != null)
        {
            return replacementOpt;
        }

        return expression switch
        {
            Column or Literal => expression,
            AggregateFunction fn => fn with { Args = fn.Args.Select(a => a.CloneWithReplacement(replacement)).ToList() },
            Alias alias => alias with { Expression = alias.Expression.CloneWithReplacement(replacement) },
            Binary binary => new Binary(binary.Left.CloneWithReplacement(replacement), binary.Op, binary.Right.CloneWithReplacement(replacement)),

            _ => throw new NotImplementedException() //todo other types
        };
    }
    /// <summary>
    /// Resolve all columns in the expression tree
    /// </summary>
    /// <param name="expression">Expression being resolved</param>
    /// <param name="schema">Schema containing fields being resolved</param>
    /// <returns>Resolved schema</returns>
    /// <exception cref="NotImplementedException"></exception>
    public static ILogicalExpression ResolveColumns(ILogicalExpression expression, Schema schema)
    {
        return expression.CloneWithReplacement(nested => nested is not Column c
            ? null
            : schema.GetFieldFromColumn(c)!.QualifiedColumn());
    }
    /// <summary>
    /// Relational expression from sql expression
    /// </summary>
    internal static ILogicalExpression SqlToExpression(this Expression? predicate, Schema schema, PlannerContext context)
    {
        var expr = SqlExpressionToLogicalExpression(predicate, schema, context);

        //TODO ?? rewrite qualifier
        //  ?? validate
        //  ?? infer

        return expr;
    }
    /// <summary>
    /// Rebuilds an expression as a projection on top of
    /// a collection of expressions.
    ///
    /// "a + b = 1" would require 2 individual input columns
    /// for 'a' and 'b'.  However, if the base expressions
    /// already contain the "a + b" result, then that can
    /// be used in place of the columns.
    /// </summary>
    /// <param name="expression">Expression to rebase</param>
    /// <param name="baseExpressions">Base expressions</param>
    /// <param name="schema">Schema to search for replacement columns</param>
    /// <returns></returns>
    internal static ILogicalExpression RebaseExpression(
        this ILogicalExpression expression,
        ICollection<ILogicalExpression> baseExpressions,
        Schema schema)
    {
        return expression.CloneWithReplacement(nested => baseExpressions.Contains(nested)
            ? ExpressionAsColumn(nested, schema)
            : null);

        static ILogicalExpression ExpressionAsColumn(ILogicalExpression expression, Schema schema)
        {
            if (expression is not Column c)
            {
                return new Column(expression.CreateLogicalName());
            }

            var field = schema.GetFieldFromColumn(c);
            return field!.QualifiedColumn();
        }
    }
    /// <summary>
    /// Converts an AST SQL expression into a logical expression
    /// </summary>
    /// <param name="expression">Expression to convert</param>
    /// <param name="schema">Schema containing column definitions</param>
    /// <param name="context">Planner context</param>
    /// <returns>Logical expression instance</returns>
    internal static ILogicalExpression SqlExpressionToLogicalExpression(
        Expression? expression, Schema schema, PlannerContext context)
    {
        var stack = new Stack<IStackEntry>();
        var evalStack = new Stack<ILogicalExpression>();

        stack.Push(new ExpressionStackEntry(expression));

        while (stack.TryPop(out var entry))
        {
            if (entry is ExpressionStackEntry exp)
            {
                if (exp.Expression is BinaryOp binary)
                {
                    stack.Push(new OperatorStackEntry(binary.Op));
                    stack.Push(new ExpressionStackEntry(binary.Right));
                    stack.Push(new ExpressionStackEntry(binary.Left));
                }
                else
                {
                    var logical = SqlExpressionToLogicalInternal(exp.Expression, schema, context);
                    evalStack.Push(logical);
                }
            }
            else
            {
                var op = (OperatorStackEntry)entry;
                var right = evalStack.Pop();
                var left = evalStack.Pop();
                var expr = new Binary(left, op.Operator, right);

                evalStack.Push(expr);
            }
        }

        return evalStack.Pop();
    }
    /// <summary>
    /// Converts literal values, identifiers, compound identifiers,
    /// and functions into logical expressions
    /// </summary>
    /// <param name="expression">Expression to convert</param>
    /// <param name="schema">Schema containing column definitions</param>
    /// <param name="context">Planner context</param>
    /// <returns>Logical expression instance</returns>
    /// <exception cref="NotImplementedException"></exception>
    internal static ILogicalExpression SqlExpressionToLogicalInternal(Expression? expression, Schema schema, PlannerContext context)
    {
        return expression switch
        {
            LiteralValue literalValue => literalValue.ParseValue(),
            Identifier ident => ident.SqlIdentifierToExpression(schema, context),
            Function fn => SqlFunctionToExpression(fn, schema, context),
            CompoundIdentifier compound => SqlCompoundIdentToExpression(compound, schema, context),
            Subquery subquery => ParseScalarSubquery(subquery, schema, context),
            Expression.InList inList => SqlInListToExpression(inList.Expression, inList.List, inList.Negated, schema, context),
            Expression.Between between => SqlBetweenExpression(between, schema, context),
            Expression.Like like => SqlLikeToExpression(like, schema, context),
            ILike like => SqlLikeToExpression(like, schema, context),
            Expression.Case @case => SqlCaseIdentifierToExpression(@case, schema, context),

            _ => throw new NotImplementedException()
        };
    }
    /// <summary>
    /// Converts a SQL Identifier into an unqualified column
    /// </summary>
    /// <param name="ident">SQL identifier</param>
    /// <param name="schema">Schema containing column definitions</param>
    /// <param name="context">Planner context</param>
    /// <returns>Column instance</returns>
    internal static Column SqlIdentifierToExpression(this Identifier ident, Schema schema, PlannerContext context)
    {
        // Found a match without a qualified name, this is an inner table column
        var field = schema.GetField(ident.Ident.Value);
        if (field != null)
        {
            return new Column(schema.GetField(ident.Ident.Value)!.Name);
        }

        //if (context.OuterQuerySchema != null)
        //{
        //    //todo df/sql/source/expr/identifier.rs
        //}

        return new Column(ident.Ident.Value);
    }
    /// <summary>
    /// Parses SQL literal values into logical expression types
    /// </summary>
    /// <param name="literalValue">SQL literal value</param>
    /// <returns>Logical expression instance</returns>
    /// <exception cref="NotImplementedException"></exception>
    internal static ILogicalExpression ParseValue(this LiteralValue literalValue)
    {
        switch (literalValue.Value)
        {
            //TODO case Value.Null: literal scalar value

            case Value.Number number:
                return number.ParseSqlNumber();

            case Value.SingleQuotedString quotedString:
                return new Literal(new StringScalar(quotedString.Value));

            case Value.Boolean boolean:
                return new Literal(new BooleanScalar(boolean.Value));

            default:
                throw new NotImplementedException();
        }
    }
    /// <summary>
    /// Converts a SQL function to a logical expression
    /// </summary>
    /// <param name="function">SQL function to convert</param>
    /// <param name="schema">Schema containing column definitions</param>
    /// <param name="context">Planner context</param>
    /// <returns>Logical expression instance</returns>
    /// <exception cref="InvalidOperationException"></exception>
    internal static ILogicalExpression SqlFunctionToExpression(Function function, Schema schema, PlannerContext context)
    {
        // scalar functions

        // aggregate functions
        var name = function.Name;

        var aggregateType = AggregateFunction.GetFunctionType(name);
        if (aggregateType.HasValue)
        {
            var distinct = false;// function.Distinct;

            Sequence<FunctionArg>? args = null;

            if (function.Args is FunctionArguments.List list)
            {
                args = list.ArgumentList.Args;
            }
            var (aggregateFunction, expressionArgs) =
                AggregateFunctionToExpression(aggregateType.Value, args, schema, context);

            return new AggregateFunction(aggregateFunction, expressionArgs, distinct);
        }

        throw new InvalidOperationException("Invalid function");
    }
    /// <summary>
    /// Parses an AST IN expression containing a list of values that
    /// will eventually be compared against values in a record batch
    /// </summary>
    /// <param name="in">AST expression to parse</param>
    /// <param name="list">AST expression value list</param>
    /// <param name="negated">'NOT' prefix. True if negated; otherwise false.</param>
    /// <param name="schema">Schema with field definitions</param>
    /// <param name="context">Planner context</param>
    /// <returns>In List logical expression</returns>
    internal static ILogicalExpression SqlInListToExpression(
        Expression @in,
        Sequence<Expression> list,
        bool negated,
        Schema schema,
        PlannerContext context)
    {
        var expressionList = list.Select(i => SqlExpressionToLogicalExpression(i, schema, context)).ToList();
        var expression = SqlExpressionToLogicalExpression(@in, schema, context);

        return new InList(expression, expressionList, negated);
    }
    /// <summary>
    /// Parses an AST LIKE expression to create a Like logical expression
    /// </summary>
    /// <param name="evalExpression">Like or ILike expression to evaluate</param>
    /// <param name="schema">Schema with comparison fields</param>
    /// <param name="context">Planner context</param>
    /// <returns>Like logical expression</returns>
    private static ILogicalExpression SqlLikeToExpression(Expression evalExpression, Schema schema, PlannerContext context)
    {
        Expression likePattern = null!;
        Expression? likeExpression = null!;
        char? escapeCharacter = null!;
        var negated = false;
        var caseSensitive = false;

        switch (evalExpression)
        {
            case Expression.Like like:
                likePattern = like.Pattern;
                likeExpression = like.Expression;
                escapeCharacter = string.IsNullOrEmpty(like.EscapeChar) ? null : like.EscapeChar![0];
                negated = like.Negated;
                caseSensitive = true;
                break;

            case ILike like:
                likePattern = like.Pattern;
                likeExpression = like.Expression;
                escapeCharacter = string.IsNullOrEmpty(like.EscapeChar) ? null : like.EscapeChar![0];
                negated = like.Negated;
                break;
        }

        var pattern = SqlExpressionToLogicalExpression(likePattern, schema, context);
        var expression = SqlExpressionToLogicalExpression(likeExpression, schema, context);

        return new Like(negated, expression, pattern, escapeCharacter, caseSensitive);
    }

    /// <summary>
    /// Parses an AST CASE/WHEN expression containing expressions
    /// will eventually be converted to logical expressions
    /// </summary>
    /// <param name="case">Case expression</param>
    /// <param name="schema">Schema with case fields</param>
    /// <param name="context">Planner context</param>
    /// <returns>Case logical expression</returns>
    private static ILogicalExpression SqlCaseIdentifierToExpression(Expression.Case @case, Schema schema, PlannerContext context)
    {
        ILogicalExpression? expr = null;

        if (@case.Operand != null)
        {
            expr = SqlExpressionToLogicalExpression(@case.Operand, schema, context);
        }

        var whenExpression = @case.Conditions.Select(e => SqlExpressionToLogicalExpression(e, schema, context));
        var thenExpression = @case.Results.Select(e => SqlExpressionToLogicalExpression(e, schema, context));
        ILogicalExpression? elseExpression = null;

        if (@case.ElseResult != null)
        {
            elseExpression = SqlExpressionToLogicalExpression(@case.ElseResult, schema, context);
        }

        var whenThen = whenExpression.Zip(thenExpression).ToList();

        return new Case(expr, whenThen, elseExpression);
    }
    /// <summary>
    /// Parses an AST BETWEEN expression containing a pair of values that
    /// will eventually be compared against values in a record batch
    /// </summary>
    /// <param name="between">AST expression to parse</param>
    /// <param name="schema">Schema with field definitions</param>
    /// <param name="context">Planner context</param>
    /// <returns>Between logical expression</returns>
    private static ILogicalExpression SqlBetweenExpression(Expression.Between between, Schema schema, PlannerContext context)
    {
        return new Between(
            SqlExpressionToLogicalExpression(between.Expression, schema, context),
            between.Negated,
            SqlExpressionToLogicalExpression(between.Low, schema, context),
            SqlExpressionToLogicalExpression(between.High, schema, context));
    }
    /// <summary>
    /// Parses a SQL numeric value into a literal value
    /// containing a type-specific scalar value
    /// </summary>
    /// <param name="number">SQL number to convert </param>
    /// <returns>Logical expression instance</returns>
    internal static ILogicalExpression ParseSqlNumber(this Value.Number number)
    {
        if (long.TryParse(number.Value, out var parsedInt))
        {
            return new Literal(new IntegerScalar(parsedInt));
        }

        return double.TryParse(number.Value, out var parsedDouble)
            ? new Literal(new DoubleScalar(parsedDouble))
            : new Literal(new StringScalar(number.Value));
    }
    /// <summary>
    /// Converts a compound identifier into a logical expression
    /// </summary>
    /// <param name="ident">Identifier to convert</param>
    /// <param name="schema">Schema containing column definitions</param>
    /// <param name="context">Planner context</param>
    /// <returns>Logical expression instance</returns>
    /// <exception cref="InvalidOperationException"></exception>
    /// <exception cref="NotImplementedException"></exception>
    internal static ILogicalExpression SqlCompoundIdentToExpression(CompoundIdentifier ident, Schema schema, PlannerContext context)
    {
        if (ident.Idents.Count > 2)
        {
            throw new InvalidOperationException("Not a valid compound identifier");
        }

        var idents = ident.Idents.Select(i => i.Value).ToList();
        var terms = idents.GenerateSearchTerms().ToList();

        var result = SearchSchema(schema, terms);

        if (result.Field != null)
        {
            // found matching field with spare identifier(s) for nested field(s) in structure
            if (result.NestedNames.Length > 0)
            {
                // TODO how is this expression used elsewhere?
                //return new GetIndexedField(result.Field.QualifiedColumn(), new StringScalar(result.NestedNames[0]));
                throw new NotImplementedException("SqlCompoundIdentToExpression Nested names not yet supported");
            }

            // found matching field with no spare identifier(s)
            return result.Field.QualifiedColumn();
        }

        if (context.OuterQuerySchema != null)
        {
            result = SearchSchema(context.OuterQuerySchema, terms);

            if (result.Field != null)
            {
                if (result.NestedNames.Length > 0)
                {
                    throw new InvalidOperationException("Nested identifiers are not supported.");
                }

                return new OuterReferenceColumn(result.Field.DataType, result.Field.QualifiedColumn());
            }

            // Fall through to the default column and table reference
        }

        var relation = idents[0];
        var name = idents[1];

        return new Column(name, new TableReference(relation));
    }
    /// <summary>
    /// Searches a schema for a field by building a list of possible terms
    /// and finding the field based on format of the segments in the terms
    /// </summary>
    /// <param name="schema">Schema to search</param>
    /// <param name="terms">Terms containing field name segments</param>
    /// <returns>Qualified field and nested name</returns>
    internal static (QualifiedField? Field, string[] NestedNames) SearchSchema(
        Schema schema,
        IEnumerable<(TableReference? Table, string ColumnName, string[] NestedNames)> terms)
    {
        return terms.Select(term => new
        {
            Term = term,
            Field = schema.FieldWithName(new Column(term.ColumnName, term.Table))
        })
        .Where(term => term.Field != null)
        .Select(term => (term.Field, term.Term.NestedNames))
        .FirstOrDefault();
    }
    /// <summary>
    /// Parses a scalar query into a logical expression
    /// </summary>
    /// <param name="subquery">Subquery to parse into a Scalar Subquery expression</param>
    /// <param name="schema">Schema containing subquery fields; used to set the outer schema context.</param>
    /// <param name="context">Planner context used to manage the state of the outer query schema</param>
    /// <returns>Scalar subquery expression</returns>
    internal static ScalarSubquery ParseScalarSubquery(Subquery subquery, Schema schema, PlannerContext context)
    {
        var oldOuterQuerySchema = context.SetOuterQuerySchema(schema);
        var subPlan = LogicalPlanner.CreateLogicalPlan(subquery.Query.Body.AsSelect(), context);
        var outerRefColumns = subPlan.AllOutReferenceExpressions();
        context.SetOuterQuerySchema(oldOuterQuerySchema);

        return new ScalarSubquery(subPlan, outerRefColumns);
    }
    /// <summary>
    /// Finds all references to an outer schema
    /// </summary>
    /// <param name="plan">Plan to search for outer references</param>
    /// <returns>List of outer reference logical expressions</returns>
    internal static List<ILogicalExpression> AllOutReferenceExpressions(this ILogicalPlan plan)
    {
        var expressions = new List<ILogicalExpression>();

        foreach (var exp in plan.GetExpressions())
        {
            var refExpressions = FindOutReferenceExpressions(exp, e => e is OuterReferenceColumn);

            foreach (var refExp in refExpressions)
            {
                if (!expressions.Contains(refExp))
                {
                    expressions.Add(refExp);
                }
            }
        }

        var inputs = plan.GetInputs();

        foreach (var exp in inputs.SelectMany(e => e.AllOutReferenceExpressions()))
        {
            if (!expressions.Contains(exp))
            {
                expressions.Add(exp);
            }
        }

        return expressions;
    }
    /// <summary>
    /// Finds all outer reference expressions by searching the expression
    /// tree for a given logical expression.  
    /// </summary>
    /// <param name="expression">Expression to search for outer references</param>
    /// <param name="predicate">Callback checking if the expression matches the given criteria</param>
    /// <returns>List of expressions matching the callback criteria</returns>
    internal static IEnumerable<ILogicalExpression> FindOutReferenceExpressions(INode expression, Func<ILogicalExpression, bool> predicate)
    {
        var expressions = new List<ILogicalExpression>();

        expression.Apply(e =>
        {
            if (predicate((ILogicalExpression)e))
            {
                if (!expressions.Contains(e))
                {
                    expressions.Add((ILogicalExpression)e);
                }

                return VisitRecursion.Skip;
            }

            return VisitRecursion.Continue;
        });

        return expressions;
    }
    /// <summary>
    /// Finds all nested expressions contained in a list of expressions
    /// logical expression
    /// </summary>
    /// <param name="expressions">Expression to search</param>
    /// <param name="predicate">Callback to check for nested expressions</param>
    /// <returns>List of nested expressions</returns>
    internal static List<ILogicalExpression> FindNestedExpressions(List<ILogicalExpression> expressions, Func<ILogicalExpression, bool> predicate)
    {
        return expressions
            .SelectMany(e => FindNestedExpression(e, predicate))
            .Aggregate(new List<ILogicalExpression>(), (list, value) =>
            {
                if (!list.Contains(value)) { list.Add(value); }

                return list;
            })
            .ToList();
    }
    /// <summary>
    /// Finds all nested expressions contained in a
    /// give logical expression
    /// </summary>
    /// <param name="expression">Expression to search</param>
    /// <param name="predicate">Callback to check for nested expressions</param>
    /// <returns>List of nested expressions</returns>
    internal static IEnumerable<ILogicalExpression> FindNestedExpression(ILogicalExpression expression, Func<ILogicalExpression, bool> predicate)
    {
        var expressions = new List<ILogicalExpression>();
        expression.Apply(e =>
        {
            if (!predicate((ILogicalExpression)e))
            {
                return VisitRecursion.Continue;
            }

            if (!expressions.Contains(e))
            {
                expressions.Add((ILogicalExpression)e);
            }

            return VisitRecursion.Skip;

        });

        return expressions;
    }
    /// <summary>
    /// Converts a schema into a nested list containing the single schema
    /// </summary>
    /// <param name="schema">Schema to insert into the hierarchy</param>
    /// <returns>List of schema lists</returns>
    internal static List<List<Schema>> AsNested(this Schema schema)
    {
        return [[schema]];
    }
    /// <summary>
    /// Converts a hash set into a nested list containing the single hash set
    /// </summary>
    /// <param name="usingColumns">Hash set to insert into the hierarchy</param>
    /// <returns>List of hash sets with column values</returns>
    internal static List<HashSet<Column>> AsNested(this HashSet<Column> usingColumns)
    {
        return [usingColumns];
    }
    /// <summary>
    /// Merges one schema into another by selecting distinct fields from the second schema into the first
    /// </summary>
    /// <param name="self">Target schema to merge fields into</param>
    /// <param name="fromSchema">Schema containing fields to merge</param>
    internal static void MergeSchemas(this Schema self, Schema fromSchema)
    {
        if (!fromSchema.Fields.Any())
        {
            return;
        }

        foreach (var field in fromSchema.Fields)
        {
            var duplicate = self.Fields.FirstOrDefault(f => f.Name == field.Name) != null;

            if (!duplicate)
            {
                self.Fields.Add(field);
            }
        }
    }
    /// <summary>
    /// Used for normalizing columns as the fallback schemas to
    /// a plan's main schema
    /// </summary>
    /// <param name="plan">Plan to normalize</param>
    /// <returns>List of schemas</returns>
    internal static List<Schema> FallbackNormalizeSchemas(this ILogicalPlan plan)
    {
        return plan switch
        {
            Projection
                or Aggregate
                or Join
                or CrossJoin => plan.GetInputs().Select(input => input.Schema).ToList(),

            _ => []
        };
    }
    /// <summary>
    /// Generates a list of possible search terms from a list
    /// of identifiers
    ///
    /// Length = 2
    /// (table.column)
    /// (column).nested
    ///
    /// Length = 3:
    /// 1. (schema.table.column)
    /// 2. (table.column).nested
    /// 3. (column).nested1.nested2
    ///
    /// Length = 4:
    /// 1. (catalog.schema.table.column)
    /// 2. (schema.table.column).nested1
    /// 3. (table.column).nested1.nested2
    /// 4. (column).nested1.nested2.nested3
    /// </summary>
    /// <param name="idents">Identifier list used to build search values</param>
    /// <returns>List of table references and column names</returns>
    internal static List<(TableReference? Table, string ColumnName, string[] NestedNames)>
        GenerateSearchTerms(this IReadOnlyCollection<string> idents)
    {
        var ids = idents.ToArray();
        // at most 4 identifiers to form a column to search with
        // 1 for the column name
        // 0 - 3 for the table reference
        var bound = Math.Min(idents.Count, 4);

        return Enumerable.Range(0, bound).Reverse().Select(i =>
        {
            var nestedNamesIndex = i + 1;
            var qualifierWithColumn = ids[..nestedNamesIndex];
            var (relation, columnName) = FromIdentifier(qualifierWithColumn);

            return (relation, columnName, ids[nestedNamesIndex..]);
        }).ToList();
    }
    /// <summary>
    /// Converts a list if field identifiers into a field name
    /// and optional table reference
    /// </summary>
    /// <param name="idents">Identifier segments to query</param>
    /// <returns>Name and optional table reference</returns>
    internal static (TableReference?, string) FromIdentifier(IReadOnlyList<string> idents)
    {
        return idents.Count switch
        {
            1 => (null, idents[0]),
            2 => (new TableReference(idents[0]), idents[1]),

            _ => throw new InvalidOperationException("Incorrect number of identifiers")
        };
    }

    internal static ILogicalExpression CastTo(this ILogicalExpression expression, ColumnDataType newType, Schema schema)
    {
        var exprType = expression.GetDataType(schema);
        if (exprType == newType)
        {
            return expression;
        }

        if (CanCastTypes(exprType, newType))
        {
            //if (expression is ScalarSubquery)
            //{
            //    return new 
            //}

            return new Cast(expression, newType);
        }

        throw new InvalidOperationException($"Cannot covert type {exprType} to {newType}");
    }

    internal static bool CanCastTypes(ColumnDataType fromType, ColumnDataType toType)
    {
        //TODO implement
        return true;
    }
    #endregion

    #region Logical Plan

    internal static ILogicalPlan CreateQuery(Query query, PlannerContext context)
    {
        var plan = query.Body is SetExpression.SelectExpression
            ? CreatePlanFromQuery(query, context)
            : CreatePlanFromExpression(query.Body, context);

        // Wrap the plan in a sort
        plan = plan.OrderBy(query.OrderBy?.Expressions ?? [], context);

        // Wrap the plan in a limit
        return plan.Limit(query.Offset, query.Limit);
    }

    internal static ILogicalPlan CreatePlanFromQuery(Query query, PlannerContext context)
    {
        var select = query.Body.AsSelect();
        context.TableReferences.AddRange(select.CreateTableRelations());
        return LogicalPlanner.CreateLogicalPlan(select, context);
    }

    internal static ILogicalPlan CreatePlanFromExpression(SetExpression expression, PlannerContext context)
    {
        if (expression is SetExpression.SetOperation setOp)
        {
            var all = setOp.SetQuantifier == SetQuantifier.All;

            var leftPlan = setOp.Left is SetExpression.SetOperation leftOp
                ? CreatePlanFromExpression(leftOp, context)
                : LogicalPlanner.CreateLogicalPlan(setOp.Left.AsSelect(), context);

            var rightPlan = setOp.Right is SetExpression.SetOperation rightOp
                ? CreatePlanFromExpression(rightOp, context)
                : LogicalPlanner.CreateLogicalPlan(setOp.Right.AsSelect(), context);

            switch (setOp.Op, all)
            {
                case (SetOperator.Union, true):
                    return LogicalPlanBuilder.Union(leftPlan, rightPlan);

                case (SetOperator.Union, false):
                    return LogicalPlanBuilder.UnionDistinct(leftPlan, rightPlan);

                case (SetOperator.Intersect, true):
                    return LogicalPlanBuilder.Intersect(leftPlan, rightPlan, true);

                case (SetOperator.Intersect, false):
                    return LogicalPlanBuilder.Intersect(leftPlan, rightPlan, false);

                case (SetOperator.Except, true):
                    return LogicalPlanBuilder.Except(leftPlan, rightPlan, true);

                case (SetOperator.Except, false):
                    return LogicalPlanBuilder.Except(leftPlan, rightPlan, false);
            }
        }

        throw new NotImplementedException("Set expression is not implemented");
    }

    #endregion

    #region Table Plan
    /// <summary>
    /// Gets the root logical plan.  The plan root will scan the data
    /// source for the query's projected values. The plan is empty in
    /// the case there is no from clause
    ///  e.g. `select 123`
    /// </summary>
    /// <param name="tables">Data sources used to look up the table being scanned</param>
    /// <param name="context">Planner context</param>
    /// <returns>ILogicalPlan instance as the plan root</returns>
    /// <exception cref="InvalidOperationException">Thrown for unsupported from clauses</exception>
    internal static ILogicalPlan PlanTableWithJoins(this IReadOnlyCollection<TableWithJoins>? tables, PlannerContext context)
    {
        if (tables == null || tables.Count == 0)
        {
            return new EmptyRelation();
        }

        if (tables.Count == 1)
        {
            var table = tables.First();
            var plan = table.Relation!.CreateRelation(context);

            if (table.Joins == null || !table.Joins.Any())
            {
                return plan;
            }

            return table.Joins.Aggregate(plan, (current, join) => current.ParseRelationJoin(join, context));
        }

        var plans = tables.Select(t => t.Relation!.CreateRelation(context)).ToList();

        var left = plans.First();

        for (var i = 1; i < plans.Count; i++)
        {
            var right = plans[i];
            left = new CrossJoin(left, right);
        }

        return left;
    }
    /// <summary>
    /// Creates a table reference from a table factor instance.  Relation
    /// tables are turned into either a table scan or subquery alias.
    /// Derived tables become a full plan with a possible table alias
    /// </summary>
    /// <param name="tableFactor">Table factor to parse</param>
    /// <param name="context">Planner context</param>
    /// <returns>Plan containing a table reference</returns>
    internal static ILogicalPlan CreateRelation(this TableFactor tableFactor, PlannerContext context)
    {
        switch (tableFactor)
        {
            case TableFactor.Table relation:
                {
                    // Get the table name used to query the data source
                    var name = relation.Name.Values[0];
                    var tableRef = context.TableReferences.Find(t => t.Name == name && (
                        t.Alias == null && relation.Alias == null ||
                        relation.Alias != null && t.Alias! == relation.Alias.Name));

                    // The root operation will scan the table for the projected values
                    var table = context.DataSources[name];
                    var qualifiedFields = table.Schema!.Fields.Select(f => new QualifiedField(f.Name, f.DataType, tableRef)).ToList();
                    var schema = new Schema(qualifiedFields);

                    var plan = (ILogicalPlan)new TableScan(name, schema, table);

                    return tableRef?.Alias == null
                        ? plan
                        : SubqueryAlias.TryNew(plan, tableRef.Alias);
                }
            case TableFactor.Derived derived:
                {
                    //var subqueryPlan = LogicalPlanner.CreateLogicalPlan(derived.SubQuery, context);//.Body.AsSelect(), context);
                    //var subqueryPlan =  derived.SubQuery.Body is SetExpression.SelectExpression
                    //    ? CreatePlanFromQuery(derived.SubQuery, context)
                    //    : CreatePlanFromExpression(derived.SubQuery.Body, context);
                    var subqueryPlan = CreateQuery(derived.SubQuery, context);

                    return derived.Alias != null
                        ? ApplyTableAlias(subqueryPlan, derived.Alias)
                        : subqueryPlan;
                }
            default:
                throw new InvalidOperationException("Relation type is not supported.");
        }
    }
    /// <summary>
    /// Wraps a logical plan with a subquery alias.
    /// </summary>
    /// <param name="plan">Plan to wrap</param>
    /// <param name="alias">Table alias related to the subquery</param>
    /// <returns>Subquery alias plan</returns>
    internal static ILogicalPlan ApplyTableAlias(ILogicalPlan plan, TableAlias alias)
    {
        var aliasPlan = SubqueryAlias.TryNew(plan, alias.Name);

        return ApplyExpressionAlias(aliasPlan, alias.Columns);
    }
    /// <summary>
    /// Applies an alias to an expression based on the contents
    /// of the syntax tree's list of field identifiers 
    /// </summary>
    /// <param name="plan">Plan with expressions to apply an alias</param>
    /// <param name="idents">Idents containing an alias for the relevant schema column</param>
    /// <returns>Projected plan with alias values applied</returns>
    internal static ILogicalPlan ApplyExpressionAlias(ILogicalPlan plan, IReadOnlyCollection<Ident>? idents)
    {
        if (idents == null || !idents.Any())
        {
            return plan;
        }

        if (idents.Count != plan.Schema.Fields.Count)
        {
            throw new InvalidOperationException(
                $"Source table contains {plan.Schema.Fields.Count} columns but {idents.Count} names given as column alias");
        }

        var fields = plan.Schema.Fields;

        var expressions = fields.Zip(idents).Select(c =>
        {
            var column = new Column(c.First.Name);
            ILogicalExpression alias = new Alias(column, c.Second.Value);
            return alias;
        }).ToList();

        return plan.PlanProjection(expressions);
    }
    /// <summary>
    /// Parses an AST join into a logical Join or Cross Join plan
    /// </summary>
    /// <param name="left">Logical plan on the left side of the join</param>
    /// <param name="join">Join to parse</param>
    /// <param name="context">Planner context</param>
    /// <returns>Join or Cross Join Logical Plan</returns>
    internal static ILogicalPlan ParseRelationJoin(this ILogicalPlan left, SqlParser.Ast.Join join, PlannerContext context)
    {
        var right = join.Relation!.CreateRelation(context);

        switch (join.JoinOperator)
        {
            case JoinOperator.LeftOuter l:
                return ParseJoin(left, right, l.JoinConstraint, JoinType.Left, context);

            case JoinOperator.RightOuter r:
                return ParseJoin(left, right, r.JoinConstraint, JoinType.Right, context);

            case JoinOperator.Inner i:
                return ParseJoin(left, right, i.JoinConstraint, JoinType.Inner, context);

            case JoinOperator.LeftSemi ls:
                return ParseJoin(left, right, ls.JoinConstraint, JoinType.LeftSemi, context);

            case JoinOperator.RightSemi rs:
                return ParseJoin(left, right, rs.JoinConstraint, JoinType.RightSemi, context);

            case JoinOperator.LeftAnti la:
                return ParseJoin(left, right, la.JoinConstraint, JoinType.LeftAnti, context);

            case JoinOperator.RightAnti ra:
                return ParseJoin(left, right, ra.JoinConstraint, JoinType.RightAnti, context);

            case JoinOperator.FullOuter f:
                return ParseJoin(left, right, f.JoinConstraint, JoinType.Full, context);

            case JoinOperator.CrossJoin:
                return new CrossJoin(left, right);

            default:
                throw new NotImplementedException("ParseRelationJoin Join type not implemented yet");
        }
    }
    /// <summary>
    /// Gets a list of table references based on a given logical expression
    /// by recursively visiting the AST looking for all table relations.
    /// </summary>
    /// <param name="select">Select element containing table relations to collect</param>
    /// <returns>List of table references in the syntax tree</returns>
    internal static List<TableReference> CreateTableRelations(this IElement select)
    {
        var relationVisitor = new RelationVisitor();
        select.Visit(relationVisitor);
        return relationVisitor.TableReferences;
    }
    #endregion

    #region Select Plan
    /// <summary>
    /// Builds a logical plan from a query filter
    /// </summary>
    /// <param name="selection">Filter expression</param>
    /// <param name="plan">Input plan</param>
    /// <param name="context">Planner context</param>
    /// <returns>ILogicalPlan instance to filter the input plan</returns>
    internal static ILogicalPlan PlanFromSelection(this Expression? selection, ILogicalPlan plan, PlannerContext context)
    {
        if (selection == null)
        {
            return plan;
        }

        var fallbackSchemas = plan.FallbackNormalizeSchemas();
        var outerQuerySchema = context.OuterQuerySchema;
        var outerSchemaList = new List<Schema>();
        if (outerQuerySchema != null)
        {
            outerSchemaList.Add(outerQuerySchema);
        }

        var filterExpression = selection.SqlToExpression(plan.Schema, context);
        var usingColumns = new HashSet<Column>();
        filterExpression.ExpressionToColumns(usingColumns);

        var schemas = new List<List<Schema>>
        {
            new(){ plan.Schema },
            fallbackSchemas,
            outerSchemaList
        };

        filterExpression = filterExpression.NormalizeColumnWithSchemas(schemas, usingColumns.AsNested());

        return new Filter(plan, filterExpression);
    }

    /// <summary>
    /// Create a projection from a `SELECT` statement
    /// </summary>
    /// <param name="projection"></param>
    /// <param name="plan"></param>
    /// <param name="emptyFrom"></param>
    /// <param name="context">Planner context</param>
    /// <returns>List of parsed select expressions</returns>
    internal static List<ILogicalExpression> PrepareSelectExpressions(
        this IEnumerable<SelectItem> projection,
        ILogicalPlan plan,
        bool emptyFrom,
        PlannerContext context)
    {
        return projection.Select(SelectToRelationalExpression).SelectMany(e => e).ToList();

        List<ILogicalExpression> SelectToRelationalExpression(SelectItem sql)
        {
            switch (sql)
            {
                case SelectItem.UnnamedExpression unnamed:
                    {
                        var expr = unnamed.Expression.SqlToExpression(plan.Schema, context);
                        var column = expr.NormalizeColumnWithSchemas(plan.Schema.AsNested(), plan.UsingColumns);
                        return [column];
                    }
                case SelectItem.ExpressionWithAlias expr:
                    {
                        var select = expr.Expression.SqlToExpression(plan.Schema, context);
                        var column = select.NormalizeColumnWithSchemas(plan.Schema.AsNested(), plan.UsingColumns);
                        return [new Alias(column, expr.Alias)];
                    }
                case SelectItem.Wildcard:
                    if (emptyFrom)
                    {
                        throw new InvalidOperationException("SELECT * with no table is not valid");
                    }

                    //TODO expand wildcard select.rs line 320
                    return plan.Schema.ExpandWildcard();

                case SelectItem.QualifiedWildcard qualified:
                    return ExpandQualifiedWildcard(qualified.Name, plan.Schema);

                default:
                    throw new InvalidOperationException("Invalid select expression");
            }
        }
    }
    /// <summary>
    /// See the Column-specific documentation
    /// </summary>
    /// <param name="expression">Expression to normalize</param>
    /// <param name="schemas">Schema hierarchy to search for column names</param>
    /// <param name="usingColumns">Column groups containing using columns</param>
    /// <returns>Logical expression with a normalized name</returns>
    internal static ILogicalExpression NormalizeColumnWithSchemas(
        this ILogicalExpression expression,
        List<List<Schema>> schemas,
        List<HashSet<Column>> usingColumns)
    {
        return expression.Transform(expression, e =>
        {
            if (e is Column c)
            {
                return c.NormalizeColumnWithSchemas(schemas, usingColumns);
            }

            return e;
        });
    }
    /// <summary>
    /// Qualify a column if it has not yet been qualified.  For unqualified
    /// columns, schemas are searched for the relevant column.  Due to
    /// SQL syntax behavior, columns can be ambiguous.  This will check
    /// for a single schema match when the column is unmatched (not assigned
    /// to a given table source).
    ///
    /// For example, the following SQL query
    /// <code>SELECT name table1 JOIN table2 USING (name)</code>
    /// 
    /// table1.name and table2.name will match unqualified column 'name'
    /// hence the list of HashSet column lists that maps columns
    /// together to help check ambiguity.  Schemas are also nested in a
    /// list of lists, so they can be checked at various logical depths
    /// </summary>
    /// <param name="column">Column to normalize</param>
    /// <param name="schemas">Schema hierarchy to search for column names</param>
    /// <param name="usingColumns">Column groups containing using columns</param>
    /// <returns>Logical expression with a normalized name</returns>
    /// <exception cref="NotImplementedException"></exception>
    /// <exception cref="InvalidOperationException"></exception>
    internal static Column NormalizeColumnWithSchemas(
        this Column column,
        List<List<Schema>> schemas,
        List<HashSet<Column>> usingColumns) //TODO: unused argument
    {
        if (column.Relation != null)
        {
            return column;
        }

        foreach (var schemaLevel in schemas)
        {
            var fields = schemaLevel.SelectMany(s => s.FieldsWithUnqualifiedName(column.Name)).ToList();

            switch (fields.Count)
            {
                case 0: continue;
                case 1: return fields[0].QualifiedColumn();
                default:
                    throw new NotImplementedException("Needs to be implemented");

            }
        }

        throw new InvalidOperationException("field not found");
    }
    /// <summary>
    /// Convert a Wildcard statement into a list of columns related to the
    /// underlying schema data source
    /// </summary>
    /// <param name="schema">Schema containing all fields to be expanded</param>
    /// <returns>List of expressions expanded from the wildcard</returns>
    internal static List<ILogicalExpression> ExpandWildcard(this Schema schema)
    {
        // todo using columns for join
        return schema.Fields.Select(f => (ILogicalExpression)f.QualifiedColumn()).ToList();
    }
    /// <summary>
    /// Expands a qualified wildcard into a list of qualified columns
    /// </summary>
    /// <param name="qualifierName">String qualifier to use as a table reference</param>
    /// <param name="schema">Schema to convert fields to qualified columns</param>
    /// <returns>List of qualified column</returns>
    internal static List<ILogicalExpression> ExpandQualifiedWildcard(string qualifierName, Schema schema)
    {
        var qualifier = new TableReference(qualifierName);

        var qualifiedFields = schema.FieldsWithQualified(qualifier).ToList();

        var qualifiedSchema = new Schema(qualifiedFields);

        return qualifiedSchema.Fields.Select(f => (ILogicalExpression)f.QualifiedColumn()).ToList();
    }

    #endregion

    #region Aggregate Plan
    /// <summary>
    /// Crete an aggregate plan from a list of SELECT, HAVING, GROUP BY, and Aggregate expressions
    /// </summary>
    /// <param name="plan">Logical plan to wrap</param>
    /// <param name="selectExpressions">Select expression list</param>
    /// <param name="havingExpression">Query HAVING expression list</param>
    /// <param name="groupByExpressions">Query GROUP BY expression list</param>
    /// <param name="aggregateExpressions">Aggregate function expression list</param>
    /// <returns></returns>
    internal static (ILogicalPlan, List<ILogicalExpression>, ILogicalExpression?) CreateAggregatePlan(
        this ILogicalPlan plan,
        List<ILogicalExpression> selectExpressions,
        ILogicalExpression? havingExpression,
        List<ILogicalExpression> groupByExpressions,
        List<ILogicalExpression> aggregateExpressions)
    {
        var groupingSets = groupByExpressions;
        var allExpressions = groupingSets.Concat(aggregateExpressions).ToList();

        var fields = allExpressions.ExpressionListToFields(plan);
        var schema = new Schema(fields);
        var aggregatePlan = new Aggregate(plan, groupByExpressions, aggregateExpressions, schema);

        var aggregateProjectionExpressions = groupByExpressions
            .ToList()
            .Concat(aggregateExpressions)
            .Select(e => ResolveColumns(e, plan.Schema))
            .ToList();

        var selectExpressionsPostAggregate = selectExpressions.Select(e => e.RebaseExpression(aggregateProjectionExpressions, plan.Schema)).ToList();

        // rewrite having columns to use columns in by the aggregation
        ILogicalExpression? havingPostAggregation = null;

        if (havingExpression != null)
        {
            havingPostAggregation = havingExpression.RebaseExpression(aggregateProjectionExpressions, plan.Schema);
        }

        return (aggregatePlan, selectExpressionsPostAggregate, havingPostAggregation);
    }
    /// <summary>
    /// Converts an aggregate function into a list of logical expressions
    /// </summary>
    /// <param name="functionType">Function type</param>
    /// <param name="arguments">Function arguments</param>
    /// <param name="schema">Schema containing fields related to the function</param>
    /// <param name="context">Planner context</param>
    /// <returns>Function type and a list of converted expressions</returns>
    internal static (AggregateFunctionType, List<ILogicalExpression>) AggregateFunctionToExpression(
        AggregateFunctionType functionType,
        IReadOnlyCollection<FunctionArg>? arguments,
        Schema schema,
        PlannerContext context)
    {
        var functionArguments = FunctionArgsToExpression();

        return (functionType, functionArguments);

        List<ILogicalExpression> FunctionArgsToExpression()
        {
            return arguments == null
                ? []
                : arguments.Select(SqlFnArgToLogicalExpression).ToList();

            ILogicalExpression SqlFnArgToLogicalExpression(FunctionArg functionArg)
            {
                if (functionType == AggregateFunctionType.Count && functionArg is FunctionArg.Unnamed { FunctionArgExpression: FunctionArgExpression.Wildcard })
                {
                    return new Literal(new IntegerScalar(1));
                }

                return functionArg switch
                {
                    FunctionArg.Named { Arg: FunctionArgExpression.Wildcard } => new Wildcard(),
                    FunctionArg.Named { Arg: FunctionArgExpression.FunctionExpression arg }
                        => SqlExpressionToLogicalExpression(arg.Expression, schema, context),

                    FunctionArg.Unnamed { FunctionArgExpression: FunctionArgExpression.FunctionExpression fe }
                        => SqlExpressionToLogicalExpression(fe.Expression, schema, context),

                    FunctionArg.Unnamed { FunctionArgExpression: FunctionArgExpression.Wildcard } => new Wildcard(),

                    _ => throw new InvalidOleVariantTypeException($"Unsupported qualified wildcard argument: {functionArg.ToSql()}")
                };
            }
        }
    }
    /// <summary>
    /// Finds group by expressions in a list of select expressions
    /// </summary>
    /// <param name="selectGroupBy">Select group by expression list</param>
    /// <param name="selectExpressions">Select expressions</param>
    /// <param name="combinedSchema">Schema of the combined expression containing all related fields</param>
    /// <param name="plan">Plan to search and parse</param>
    /// <param name="aliasMap">Alias map containing alias names to resolve and normalize </param>
    /// <param name="context">Planner context</param>
    /// <returns>List of group by expressions parsed from AST expressions</returns>
    internal static List<ILogicalExpression> FindGroupByExpressions(
        //this IReadOnlyCollection<Expression>? selectGroupBy,
        this GroupByExpression? selectGroupBy,
        List<ILogicalExpression> selectExpressions,
        Schema combinedSchema,
        ILogicalPlan plan,
        Dictionary<string, ILogicalExpression> aliasMap,
        PlannerContext context)
    {
        if (selectGroupBy == null)
        {
            return [];
        }

        if (selectGroupBy is GroupByExpression.All)
        {
            throw new NotImplementedException("Queries do not support 'GROUP BY ALL'");
        }

        var columns = (GroupByExpression.Expressions)selectGroupBy;

        return columns.ColumnNames.Select(expr =>
        {
            var groupByExpr = SqlExpressionToLogicalExpression(expr, combinedSchema, context);

            foreach (var field in plan.Schema.Fields)
            {
                aliasMap.Remove(field.Name);
            }

            groupByExpr = groupByExpr.ResolveAliasToExpressions(aliasMap);
            groupByExpr = groupByExpr.ResolvePositionsToExpressions(selectExpressions) ?? groupByExpr;
            groupByExpr = groupByExpr.NormalizeColumn(plan);

            return groupByExpr;
        }).ToList();
    }
    /// <summary>
    /// Normalizes a list of column expressions
    /// </summary>
    /// <param name="expressions">Expressions to normalize</param>
    /// <param name="plan">Plan containing schema used in normalization</param>
    /// <returns>Normalized columns; otherwise the input expressions unchanged</returns>
    internal static List<ILogicalExpression> NormalizeColumn(this IEnumerable<ILogicalExpression> expressions, ILogicalPlan plan)
    {
        return expressions.Select(e => e.NormalizeColumn(plan)).ToList();
    }
    /// <summary>
    /// Normalized an expression if the expression is a Column
    /// </summary>
    /// <param name="expression">Expression to normalize</param>
    /// <param name="plan">Plan containing schema used in normalization</param>
    /// <returns>Normalized column; otherwise the input expression unchanged</returns>
    internal static ILogicalExpression NormalizeColumn(this ILogicalExpression expression, ILogicalPlan plan)
    {
        return expression.Transform(expression, e =>
        {
            if (e is Column c)
            {
                return c.Normalize(plan);
            }

            return e;
        });
    }
    /// <summary>
    /// Queries a plan's schema for fields that can be used for normalizing
    /// column names.  Columns are converted into normal form and returned
    /// as a logical expression.
    /// </summary>
    /// <param name="column">Column to normalize</param>
    /// <param name="plan">Plan containing schema used in normalization</param>
    /// <returns>Normalized column</returns>
    internal static ILogicalExpression Normalize(this Column column, ILogicalPlan plan)
    {
        var schema = plan.Schema;
        var fallback = plan.FallbackNormalizeSchemas();
        var usingColumns = plan.UsingColumns;
        var schemas = new List<List<Schema>> { new() { schema }, fallback };

        return column.NormalizeColumnWithSchemas(schemas, usingColumns);
    }
    /// <summary>
    /// Checks a given AST expression that may contain a Having statement.
    /// If found, the Having expression is parsed into a logical expression
    /// and the resolved mapping the alias to the expression=
    /// </summary>
    /// <param name="having">Having statement</param>
    /// <param name="schema">Schema containing relevant fields</param>
    /// <param name="aliasMap">Alias map</param>
    /// <param name="context">Planner context</param>
    /// <returns>Resolved alias  logical expression; otherwise null</returns>
    internal static ILogicalExpression? MapHaving(
        this Expression? having,
        Schema schema,
        Dictionary<string, ILogicalExpression> aliasMap,
        PlannerContext context)
    {
        var havingExpression = having == null ? null : SqlExpressionToLogicalExpression(having, schema, context);

        // This step swaps aliases in the HAVING clause for the
        // underlying column name.  This is how the planner supports
        // queries with HAVING expressions that refer to aliased columns.
        //
        //   SELECT c1, MAX(c2) AS abc FROM tbl GROUP BY c1 HAVING abc > 10;
        //
        // is rewritten
        //
        //   SELECT c1, MAX(c2) AS abc FROM tbl GROUP BY c1 HAVING MAX(c2) > 10;
        return havingExpression?.ResolveAliasToExpressions(aliasMap);
    }
    /// <summary>
    /// Resolves a column to a given alias name if the alias exists
    /// </summary>
    /// <param name="expression">Expression to resolve</param>
    /// <param name="aliasMap">Alias map containing possible alias values</param>
    /// <returns>Resolved expression if found</returns>
    internal static ILogicalExpression ResolveAliasToExpressions(this ILogicalExpression expression,
        IReadOnlyDictionary<string, ILogicalExpression> aliasMap)
    {
        return expression.CloneWithReplacement(e =>
        {
            if (e is Column c && aliasMap.ContainsKey(c.Name))
            {
                return aliasMap[c.Name];
            }

            return null;
        });
    }
    /// <summary>
    /// Converts an integer position into an expression.
    ///
    /// For example, Select A, B, C ... Group By 2
    ///
    /// Resolves to would resolve to the "B" column since it's
    /// in the 2nd position in the select statement
    /// </summary>
    /// <param name="expression">Expression to resolve</param>
    /// <param name="selectExpressions">Select expression containing expressions to map positionally</param>
    /// <returns>Resolved expression</returns>
    internal static ILogicalExpression? ResolvePositionsToExpressions(this ILogicalExpression expression,
        IReadOnlyList<ILogicalExpression> selectExpressions)
    {
        if (expression is not Literal { Value: IntegerScalar i })
        {
            return null;
        }

        var position = (int)i.Value - 1;
        var expr = selectExpressions[position];

        if (expr is Alias a)
        {
            return a.Expression;
        }

        return expr;
    }
    /// <summary>
    /// Finds all aggregate expressions in a given list of logical expressions
    /// </summary>
    /// <param name="expressions">Expression list to search</param>
    /// <returns>Collected aggregate expressions</returns>
    internal static List<ILogicalExpression> FindAggregateExpressions(this List<ILogicalExpression> expressions)
    {
        return FindNestedExpressions(expressions, nested => nested is AggregateFunction);
    }
    #endregion

    #region Projection Plan
    /// <summary>
    /// Builds a project plan from a list of expressions
    /// </summary>
    /// <param name="plan">Plan to wrap in the projection</param>
    /// <param name="expressions">Expressions in the projection</param>
    /// <returns>Projection plan</returns>
    /// <exception cref="NotImplementedException"></exception>
    internal static Projection PlanProjection(this ILogicalPlan plan, List<ILogicalExpression> expressions)
    {
        var projectedExpressions = new List<ILogicalExpression>();

        foreach (var expr in expressions)
        {
            if (expr is Wildcard /*or QualifiedWildcard*/)
            {
                throw new NotImplementedException("Need to implement wildcard");
            }
            //else 
            //{
            var normalized = expr.NormalizeColumnWithSchemas(plan.Schema.AsNested(), []);
            projectedExpressions.Add(ToColumnExpression(normalized, plan.Schema));
            //}
        }

        var fields = projectedExpressions.ExpressionListToFields(plan);
        return new Projection(plan, expressions, new Schema(fields));

        static ILogicalExpression ToColumnExpression(ILogicalExpression expression, Schema schema)
        {
            switch (expression)
            {
                case Column:
                    return expression;

                case Alias alias:
                    return alias with { Expression = ToColumnExpression(alias.Expression, schema) };

                // case Cast
                // case TryCast
                // case ScalarSubQuery

                default:
                    var name = expression.CreateLogicalName();
                    var field = schema.GetField(name);
                    return field?.QualifiedColumn() ?? expression;
            }
        }
    }
    /// <summary>
    /// Adds columns that may be missing from a projection.  If a plan's schema
    /// does not have a given column, then missing columns are appended and
    /// the plan is rebuild with the full schema
    /// </summary>
    /// <param name="plan">Plan to check for missing columns</param>
    /// <param name="missingColumns">Possible missing column values</param>
    /// <param name="isDistinct">Signals if the plan is distinct</param>
    /// <returns>Rebuilt plan with all columns in the plan's schema</returns>
    internal static ILogicalPlan AddMissingColumns(this ILogicalPlan plan, HashSet<Column> missingColumns, bool isDistinct)
    {
        if (plan is Projection projection)
        {
            if (missingColumns.All(projection.Plan.Schema.HasColumn))
            {
                var missingExpressions = missingColumns.Select(c => new Column(c.Name, c.Relation).NormalizeColumn(plan)).ToList();

                // Do not let duplicate columns to be added, some of the missing columns
                // may be already present but without the new projected alias.
                missingExpressions = missingExpressions.Where(c => !projection.Expression.Contains(c)).ToList();

                if (isDistinct)
                {
                    //TODO distinct check
                }

                projection.Expression.AddRange(missingExpressions);
                return projection.Plan.PlanProjection(projection.Expression);
            }
        }

        var distinct = isDistinct || plan is Distinct;

        var newInputs = plan.GetInputs()
            .Select(p => p.AddMissingColumns(missingColumns, distinct))
            .ToList();

        return plan.FromPlan(plan.GetExpressions(), newInputs);
    }
    #endregion

    #region Order By Plan
    /// <summary>
    /// Creates an order by plan to sort values as part of the physical
    /// execution.  If no sort expressions are provided, the input
    /// plan is returned.
    /// </summary>
    /// <param name="plan">Plan to sort during execution</param>
    /// <param name="orderByExpressions">Expressions for ordering data</param>
    /// <param name="context">Planner context</param>
    /// <returns>Sort plan</returns>
    internal static ILogicalPlan OrderBy(
        this ILogicalPlan plan,
        Sequence<OrderByExpression>? orderByExpressions,
        PlannerContext context)
    {
        if (orderByExpressions == null || !orderByExpressions.Any())
        {
            return plan;
        }

        var orderByRelation = orderByExpressions.Select(e => e.OrderByToSortExpression(plan.Schema, context)).ToList();

        return Sort.TryNew(plan, orderByRelation);
    }
    /// <summary>
    /// Converts an AST OrderByExpression into an OrderBy logical expression
    /// </summary>
    /// <param name="orderByExpression">AST order by expression</param>
    /// <param name="schema">Plan schema</param>
    /// <param name="context">Planner context</param>
    /// <returns>OrderBy logical expression</returns>
    internal static ILogicalExpression OrderByToSortExpression(
        this OrderByExpression orderByExpression,
        Schema schema,
        PlannerContext context)
    {
        ILogicalExpression orderExpression;
        if (orderByExpression.Expression is LiteralValue { Value: Value.Number n })
        {
            var fieldIndex = int.Parse(n.Value);
            var field = schema.Fields[fieldIndex - 1];
            orderExpression = field.QualifiedColumn();
        }
        else
        {
            orderExpression = SqlExpressionToLogicalExpression(orderByExpression.Expression, schema, context);
        }

        return new Expressions.OrderBy(orderExpression, orderByExpression.Asc ?? true);
    }

    #endregion

    #region Limit Plan
    /// <summary>
    /// Creates a plan that limits the output data during execution
    /// </summary>
    /// <param name="plan">Plan to wrap in a limit</param>
    /// <param name="skip">Number of records to skip</param>
    /// <param name="fetch">Number of records to collect</param>
    /// <returns>Limit logical plan</returns>
    public static ILogicalPlan Limit(this ILogicalPlan plan, Offset? skip, Expression? fetch)
    {
        if (skip == null && fetch == null)
        {
            return plan;
        }

        var skipCount = 0;
        var fetchCount = int.MaxValue;

        if (skip != null)
        {
            if (skip.Value is LiteralValue slv)
            {
                if (slv.Value is Value.Number skipNumber)
                {
                    _ = int.TryParse(skipNumber.Value, out skipCount);
                }
                else
                {
                    throw new InvalidOperationException("Invalid offset");
                }
            }
            else
            {
                throw new InvalidOperationException("Invalid offset");
            }
        }

        if (fetch != null)
        {
            if (fetch is LiteralValue flv)
            {
                if (flv.Value is Value.Number fetchNumber)
                {
                    _ = int.TryParse(fetchNumber.Value, out fetchCount);
                }
                else
                {
                    throw new InvalidOperationException("Invalid offset");
                }
            }
            else
            {
                throw new InvalidOperationException("Invalid offset");
            }
        }

        return new Limit(plan, skipCount, fetchCount);
    }
    #endregion

    #region Join Plan
    /// <summary>
    /// Creates a join plan for merging data from a pair of related plans
    /// </summary>
    /// <param name="left">Left side join plan</param>
    /// <param name="right">Right side join plan</param>
    /// <param name="constraint">Constraint on how the join is evaluated</param>
    /// <param name="joinType">Type of join (e.g. outer)</param>
    /// <param name="context">Planner context</param>
    /// <returns>Join logical plan</returns>
    internal static ILogicalPlan ParseJoin(
        ILogicalPlan left,
        ILogicalPlan right,
        JoinConstraint constraint,
        JoinType joinType,
        PlannerContext context)
    {
        if (constraint is JoinConstraint.On on)
        {
            var joinSchema = left.Schema.Join(right.Schema);
            var expression = on.Expression.SqlToExpression(joinSchema, context);

            return Join.TryNew(left, right, joinType, new([], []), expression);
        }

        if (constraint is JoinConstraint.Using @using)
        {
            var keys = @using.Idents.Select(i => new Column(i)).ToList();
            return LogicalPlanBuilder.JoinUsing(left, right, joinType, keys);
        }

        throw new NotImplementedException("ParseJoin: Join constraint not implemented");
    }
    /// <summary>
    /// Normalizes columns in a join plan with fallback schema
    /// </summary>
    /// <param name="joinPlan">Join plan to normalize</param>
    /// <param name="column">Column to normalize with schemas</param>
    /// <returns></returns>
    internal static Column NormalizeJoin(ILogicalPlan joinPlan, Column column)
    {
        var schema = new List<Schema> { joinPlan.Schema };
        var fallbackSchemas = joinPlan.FallbackNormalizeSchemas();
        var usingColumns = joinPlan.UsingColumns;

        var schemaList = new List<List<Schema>> { schema, fallbackSchemas };

        return column.NormalizeColumnWithSchemas(schemaList, usingColumns);
    }
    /// <summary>
    /// Builds a join plan schema selecting fields from a left plan schema
    /// and right plan schema.  Schemas are combined using a strategy
    /// dependent on the type of join being performed.
    /// </summary>
    /// <param name="left">Left join plan schema</param>
    /// <param name="right">Right join plan schema</param>
    /// <param name="joinType">Plan jon type</param>
    /// <returns>Combined schema and join field indices</returns>
    internal static (Schema Schema, List<JoinColumnIndex> Indices) BuildJoinSchema(Schema left, Schema right, JoinType joinType)
    {
        List<QualifiedField> fields;
        List<JoinColumnIndex> columnIndices;

        switch (joinType)
        {
            case JoinType.Inner:
            case JoinType.Left:
            case JoinType.Full:
            case JoinType.Right:
                {
                    var leftFields = left.Fields
                        //.Select(f => OutputJoinField(f, joinType, true))
                        .Select((f, i) => (Field: f, ColumnIndex: new JoinColumnIndex(i, JoinSide.Left)))
                        .ToList();

                    var rightFields = right.Fields
                        //.Select(f => OutputJoinField(f, joinType, false))
                        .Select((f, i) => (Field: f, ColumnIndex: new JoinColumnIndex(i, JoinSide.Right)))
                        .ToList();

                    fields = leftFields
                        .Select(f => f.Field).Concat(rightFields.Select(f => f.Field))
                        .ToList();

                    columnIndices = leftFields
                        .Select(f => f.ColumnIndex).Concat(rightFields.Select(f => f.ColumnIndex))
                        .ToList();
                    break;
                }
            case JoinType.LeftSemi:
            case JoinType.LeftAnti:
                {
                    var allFields = left.Fields
                        .Select((f, i) => (Field: f, ColumnIndex: new JoinColumnIndex(i, JoinSide.Left)))
                        .ToList();

                    fields = allFields.Select(f => f.Field).ToList();
                    columnIndices = allFields.Select(f => f.ColumnIndex).ToList();
                    break;
                }
            case JoinType.RightSemi:
            case JoinType.RightAnti:
                {
                    var allFields = left.Fields
                        .Select((f, i) => (Field: f, ColumnIndex: new JoinColumnIndex(i, JoinSide.Right)))
                        .ToList();

                    fields = allFields.Select(f => f.Field).ToList();
                    columnIndices = allFields.Select(f => f.ColumnIndex).ToList();
                    break;
                }

            default:
                throw new NotImplementedException("BuildJoinSchema join type not implemented yet");
        }

        return (new Schema(fields), columnIndices);
    }

    #endregion

    #region Union Plan
    /// <summary>
    /// Creates a new projection using aliased columns at each expression's index
    /// </summary>
    /// <param name="expressions">Expressions to convert to alias values</param>
    /// <param name="input">Input plan</param>
    /// <param name="schema">Schema containing field definitions for column creation</param>
    /// <returns>Converted logical plan</returns>
    internal static ILogicalPlan ProjectWithColumnIndex(this ILogicalPlan input, IEnumerable<ILogicalExpression> expressions, Schema schema)
    {
        var aliasExpression = expressions.Select((e, i) =>
        {
            return e switch
            {
                Alias a when a.Name != schema.Fields[i].Name => new Alias(e.Unalias(), schema.Fields[i].Name),
                Column c when c.Name != schema.Fields[i].Name => new Alias(e, schema.Fields[i].Name),
                _ => new Alias(e, schema.Fields[i].Name)
            } as ILogicalExpression;
        }).ToList();

        return new Projection(input, aliasExpression, schema);
    }
    /// <summary>
    /// Creates a projection ensuring the expressions are compatible between plans
    /// </summary>
    /// <param name="plan">Plan to evaluate and use in the new plan creation</param>
    /// <param name="schema">Schema containing fields for all plan columns</param>
    /// <returns>New Projection plan</returns>
    internal static ILogicalPlan CoercePlanExpressionsForSchema(this ILogicalPlan plan, Schema schema)
    {
        if (plan is Projection projection)
        {
            var newExpressions = CoerceExpressionsForSchema(projection.Expression, projection.Plan.Schema, schema);
            return Projection.TryNew(projection.Plan, newExpressions);
        }
        else
        {
            var expressions = plan.Schema.Fields.Select(f => f.QualifiedColumn()).ToList();
            var newExpressions = CoerceExpressionsForSchema(expressions, plan.Schema, schema);
            var addProject = newExpressions.Any(e => e is not Column);

            return addProject ? Projection.TryNew(plan, newExpressions) : plan;
        }
    }
    /// <summary>
    /// Convert expressions into compatible expressions between the source
    /// and destination schemas
    /// </summary>
    /// <param name="expressions">Expressions to coerce</param>
    /// <param name="sourceSchema">Source schema</param>
    /// <param name="destSchema">Destination schema</param>
    /// <returns>List of coerced expressions</returns>
    private static List<ILogicalExpression> CoerceExpressionsForSchema(IEnumerable<ILogicalExpression> expressions,
        Schema sourceSchema, Schema destSchema)
    {
        return expressions.Select((expr, index) =>
        {
            var newType = destSchema.Fields[index].DataType;

            if (newType == expr.GetDataType(sourceSchema))
            {
                return expr;
            }

            var cast = expr.CastTo(newType, sourceSchema);

            if (expr is Alias alias)
            {
                return alias with { Expression = cast };
                //return new Alias(cast, alias.Name);
            }

            return cast;

        }).ToList();
    }

    #endregion

    #region Rules
    /// <summary>
    /// Splits an expression into a list of associated logical
    /// expressions if the outer expression 
    /// </summary>
    /// <param name="expression">Expression to split</param>
    /// <returns>List of logical expressions</returns>
    internal static List<ILogicalExpression> SplitConjunction(this ILogicalExpression expression)
    {
        return SplitConjunctionInternal(expression, []);
    }
    /// <summary>
    /// Splits an expression into a list of associated logical
    /// expressions if the outer expression 
    /// </summary>
    /// <param name="expression">Expression to split</param>
    /// <param name="expressions">Expression list to recursively split</param>
    /// <returns>List of logical expressions</returns>
    internal static List<ILogicalExpression> SplitConjunctionInternal(ILogicalExpression expression, List<ILogicalExpression> expressions)
    {
        while (true)
        {
            switch (expression)
            {
                case Binary { Op: BinaryOperator.And } binary:
                    {
                        var conjunction = SplitConjunctionInternal(binary.Left, expressions);
                        expression = binary.Right;
                        expressions = conjunction;
                        continue;
                    }

                case Alias a:
                    expression = a.Expression;
                    continue;
            }

            expressions.Add(expression);
            return expressions;
        }
    }

    #endregion

    #region Physical Expression
    /// <summary>
    /// Creates a physical expression from a logical expression
    /// </summary>
    /// <param name="expression">Expression to parse</param>
    /// <param name="inputDfSchema">Data frame schema</param>
    /// <param name="inputSchema">Input schema</param>
    /// <returns>Physical expression</returns>
    internal static IPhysicalExpression CreatePhysicalExpression(this ILogicalExpression expression, Schema inputDfSchema, Schema inputSchema)
    {
        while (true)
        {
            switch (expression)
            {
                case Column column:
                    var index = inputDfSchema.IndexOfColumn(column);
                    return new Physical.Expressions.Column(column.Name, index!.Value);

                case Literal literal:
                    return new Physical.Expressions.Literal(literal.Value);

                case Alias alias:
                    expression = alias.Expression;
                    continue;

                case Binary binary:
                    var left = binary.Left.CreatePhysicalExpression(inputDfSchema, inputSchema);
                    var right = binary.Right.CreatePhysicalExpression(inputDfSchema, inputSchema);

                    return new Physical.Expressions.Binary(left, binary.Op, right);

                case InList inList:
                    {
                        var valueExpression = inList.Expression.CreatePhysicalExpression(inputDfSchema, inputSchema);
                        var listExpressions = inList.List.Select(e => e.CreatePhysicalExpression(inputDfSchema, inputSchema)).ToList();
                        var dataType = inList.Expression.GetDataType(inputSchema);

                        foreach (var listExpr in inList.List)
                        {
                            var listDataType = listExpr.GetDataType(inputSchema);
                            if (dataType != listDataType)
                            {
                                throw new InvalidOperationException("The values in the 'IN' statement must be of the same type.");
                            }
                        }

                        var filter = MakeSet(EvaluateList(listExpressions, new RecordBatch(inputSchema)));

                        return new Physical.Expressions.InList(valueExpression, listExpressions, inList.Negated, filter);

                        static List<ColumnValue> EvaluateList(IEnumerable<IPhysicalExpression> list, RecordBatch batch)
                        {
                            var scalars = list.Select(e =>
                            {
                                var columnValue = e.Evaluate(batch);
                                return columnValue;
                            }).ToList();

                            return scalars;
                        }
                    }

                case Between between:
                    {
                        var valueExpression = between.Expression.CreatePhysicalExpression(inputDfSchema, inputSchema);
                        var lowExpression = between.Low.CreatePhysicalExpression(inputDfSchema, inputSchema);
                        var highExpression = between.High.CreatePhysicalExpression(inputDfSchema, inputSchema);

                        // Rewrite between as two binary operators
                        var leftBinary = new Physical.Expressions.Binary(valueExpression, BinaryOperator.GtEq, lowExpression);
                        var rightBinary = new Physical.Expressions.Binary(valueExpression, BinaryOperator.LtEq, highExpression);
                        var binaryExpression = new Physical.Expressions.Binary(leftBinary, BinaryOperator.And, rightBinary);

                        if (!between.Negated)
                        {
                            return binaryExpression;
                        }

                        return new NotExpression(binaryExpression);
                    }

                case Like like:
                    {
                        if (like.EscapeCharacter != null)
                        {
                            throw new InvalidOperationException("LIKE does not support escape characters");
                        }

                        var physicalExpression = like.Expression.CreatePhysicalExpression(inputDfSchema, inputSchema);
                        var physicalPattern = like.Pattern.CreatePhysicalExpression(inputDfSchema, inputSchema);
                        var expressionType = physicalPattern.GetDataType(inputSchema);
                        var patternType = physicalPattern.GetDataType(inputSchema);

                        if (expressionType != patternType)
                        {
                            throw new InvalidOperationException("Type LIKE expression data types should be the same");
                        }

                        return new Physical.Expressions.Like(like.Negated, like.CaseSensitive, physicalExpression, physicalPattern);
                    }

                case Case @case:
                    {
                        IPhysicalExpression? expr = null;
                        if (@case.Expression != null)
                        {
                            expr = @case.Expression.CreatePhysicalExpression(inputDfSchema, inputSchema);
                        }

                        var whenExpression = @case.WhenThenExpression.Select(e => e.When.CreatePhysicalExpression(inputDfSchema, inputSchema)).ToList();
                        var thenExpression = @case.WhenThenExpression.Select(e => e.Then.CreatePhysicalExpression(inputDfSchema, inputSchema)).ToList();
                        var whenThenExpressions = whenExpression.Zip(thenExpression).ToList();

                        IPhysicalExpression? elseExpression = null;
                        if (@case.ElseExpression != null)
                        {
                            elseExpression = @case.ElseExpression.CreatePhysicalExpression(inputDfSchema, inputSchema);
                        }

                        return new Physical.Expressions.Case(expr, whenThenExpressions, elseExpression);
                    }

                default:
                    throw new NotImplementedException($"Expression type {expression.GetType().Name} is not yet supported.");
            }
        }
    }
    /// <summary>
    /// Converts a list of column values into a distinct set of raw
    /// values used to create physical literal expressions.
    /// </summary>
    /// <param name="values">Values to convert</param>
    /// <returns>List of literal expressions</returns>
    internal static List<Physical.Expressions.Literal> MakeSet(List<ColumnValue> values)
    {
        return values.Cast<ScalarColumnValue>()
            .Select(v => v.Value)
            .DistinctBy(v => v.RawValue)
            .Select(v => new Physical.Expressions.Literal(v))
            .ToList();
    }
    /// <summary>
    /// Creates a physical name for a function and adjusts
    /// for whether the function contains a distinct value
    /// </summary>
    /// <param name="fn">Aggregate function</param>
    /// <param name="distinct">True if distinct; otherwise false</param>
    /// <param name="args">Function arguments</param>
    /// <returns></returns>
    internal static string CreateFunctionPhysicalName(this AggregateFunction fn, bool distinct, List<ILogicalExpression> args)
    {
        var names = args.Select(e => e.CreatePhysicalName(false)).ToList();

        var distinctText = distinct ? "DISTINCT " : "";

        return $"{fn.FunctionType}({distinctText}{string.Join(",", names)})";
    }
    /// <summary>
    /// Creates a physical for a logical expression
    /// </summary>
    /// <param name="expression">Logical expression</param>
    /// <param name="isFirstExpression">True if first; otherwise false</param>
    /// <returns>Expression name</returns>
    internal static string CreatePhysicalName(this ILogicalExpression expression, bool isFirstExpression)
    {
        return expression switch
        {
            Column column => isFirstExpression ? column.Name : column.FlatName,
            Alias alias => alias.Name,
            Binary binary => $"{binary.Left.CreatePhysicalName(false)} {binary.Op.GetDisplayText()} {binary.Left.CreatePhysicalName(false)}",
            AggregateFunction fn => fn.CreateFunctionPhysicalName(fn.Distinct, fn.Args),
            Literal literal => CreateLiteralName(literal),
            Case @case => CreateCaseName(@case),

            _ => throw new NotImplementedException()
        };

        static string CreateCaseName(Case @case)
        {
            var name = "CASE ";

            if (@case.Expression != null)
            {
                var exprName = @case.Expression.CreateLogicalName();
                name += $"{exprName} ";
            }

            foreach (var (w, t) in @case.WhenThenExpression)
            {
                var when = w.CreateLogicalName();
                var then = t.CreateLogicalName();
                name += $"WHEN {when} THEN {then} ";
            }

            if (@case.ElseExpression != null)
            {
                var elseName = @case.ElseExpression.CreateLogicalName();
                name += $"ELSE {elseName} ";
            }

            name += "END";
            return name;
        }

        static string CreateLiteralName(Literal lit)
        {
            var name = lit.Value.RawValue?.ToString() ?? string.Empty;
            var typeName = lit.Value.RawValue?.GetType().Name ?? "null";

            return $"{typeName}({name})";
        }
    }
    #endregion

    /// <summary>
    /// Rewrites an expression and all child expressions based on the logic
    /// supposed by the input expression rewriter.  
    /// </summary>
    /// <param name="expression">Expression to rewrite</param>
    /// <param name="rewriter">Expression rewriter instance</param>
    /// <returns></returns>
    internal static ILogicalExpression Rewrite(this ILogicalExpression expression, ITreeNodeRewriter<ILogicalExpression> rewriter)
    {
        var recursion = rewriter.PreVisit(expression);
        bool needMutate;

        switch (recursion)
        {
            case RewriterRecursion.Mutate: return rewriter.Mutate(expression);
            case RewriterRecursion.Stop: return expression;
            case RewriterRecursion.Continue:
                needMutate = true;
                break;

            default: // 
                needMutate = false;
                break;
        }

        var afterOpChildren = expression.MapChildren(expression, node => node.Rewrite(rewriter));

        return needMutate ? rewriter.Mutate(afterOpChildren) : afterOpChildren;
    }
}