﻿using Prequel.Physical.Aggregation;
using Prequel.Physical.Expressions;
using Prequel.Data;

namespace Prequel.Physical.Functions;

/// <summary>
/// Physical max function
/// </summary>
/// <param name="Expression">Function input expression </param>
/// <param name="Name">Function name</param>
/// <param name="DataType">Function data type</param>
internal record MaxFunction(IPhysicalExpression Expression, string Name, ColumnDataType DataType)
    : Aggregate(Expression), IAggregation
{
    /// <summary>
    /// Max state to use between aggregation operations or calculation steps
    /// </summary>
    internal override List<QualifiedField> StateFields => [QualifiedField.Unqualified($"{Name}", DataType)];

    /// <summary>
    /// Gets the field using the unqualified format
    /// </summary>
    internal override QualifiedField NamedQualifiedField => QualifiedField.Unqualified(Name, DataType);

    /// <summary>
    /// Gets the expressions used as inputs for the function
    /// </summary>
    internal override List<IPhysicalExpression> Expressions => [Expression];

    /// <summary>
    /// Creates an accumulator for the function's data type
    /// </summary>
    /// <returns>Max accumulator</returns>
    public Accumulator CreateAccumulator()
    {
        return new MaxAccumulator(DataType);
    }

    public override string ToString()
    {
        return $"max({Name}):{DataType}";
    }
}