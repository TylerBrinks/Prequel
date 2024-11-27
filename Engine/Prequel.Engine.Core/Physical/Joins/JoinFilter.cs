using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Physical.Expressions;

namespace Prequel.Engine.Core.Physical.Joins;

/// <summary>
/// Join condition filter
/// </summary>
/// <param name="FilterExpression">Join filter physical expression</param>
/// <param name="ColumnIndices">Filter column indices</param>
/// <param name="Schema">Schema containing join field definitions</param>
internal record JoinFilter(IPhysicalExpression FilterExpression, List<JoinColumnIndex> ColumnIndices, Schema Schema);