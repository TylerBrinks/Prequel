namespace Prequel.Physical.Joins;

/// <summary>
/// Physical join column index
/// </summary>
/// <param name="Index">Column index</param>
/// <param name="JoinSide">Identifies which side of the join the column is on</param>
internal record JoinColumnIndex(int Index, JoinSide JoinSide);