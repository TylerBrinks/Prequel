using SqlParser.Ast;

namespace Prequel.Engine.Core.Logical;

internal interface IStackEntry;

internal record ExpressionStackEntry(Expression? Expression) : IStackEntry;

internal record OperatorStackEntry(BinaryOperator Operator) : IStackEntry;