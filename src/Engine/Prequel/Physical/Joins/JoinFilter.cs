using Prequel.Data;
using Prequel.Physical.Expressions;

namespace Prequel.Physical.Joins;

/// <summary>
/// Join condition filter
/// </summary>
/// <param name="FilterExpression">Join filter physical expression</param>
/// <param name="ColumnIndices">Filter column indices</param>
/// <param name="Schema">Schema containing join field definitions</param>
internal record JoinFilter(IPhysicalExpression FilterExpression, List<JoinColumnIndex> ColumnIndices, Schema Schema);