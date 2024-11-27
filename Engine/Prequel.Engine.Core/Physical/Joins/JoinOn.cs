using Prequel.Engine.Core.Physical.Expressions;

namespace Prequel.Engine.Core.Physical.Joins;

/// <summary>
/// Join operation type to identify left and right side join columns
/// </summary>
/// <param name="Left">Join left side column</param>
/// <param name="Right">Join right side column</param>
internal record JoinOn(Column Left, Column Right);