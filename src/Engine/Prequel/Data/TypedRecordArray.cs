using System.Collections;

namespace Prequel.Data;

/// <summary>
/// Strongly typed record array
/// </summary>
/// <typeparam name="T">Record array value type</typeparam>
public abstract class TypedRecordArray<T> : RecordArray
{
    /// <summary>
    /// The record array's value generic list
    /// </summary>
    internal List<T> List { get; private set; } = [];
    /// <summary>
    /// Defines an operation to sort the record in ascending or descending order
    /// and return the index of the values in their sorted order
    /// </summary>
    /// <param name="ascending">True if sorting in ascending order; otherwise false</param>
    /// <param name="start">Start of the sort operation</param>
    /// <param name="take">Number of items to take in the sort operation after the start index</param>
    /// <returns>Integer list of index values of the sorted list</returns>
    public override List<int> GetSortIndices(bool ascending, int? start = null, int? take = null)
    {
        var skip = start ?? 0;
        var count = take ?? List.Count;

        // Only sort on the relevant fields within a group.  For a single
        // sort, the entire column is the group.  For subsequent columns
        // each sort is limited to the items in each parent's distinct 
        // list of sorted values.  
        var groupSubset = List.Skip(skip).Take(count);
        // Get the original indexed position of the items in the list
        var indexMap = groupSubset.Select((value, index) => new KeyValuePair<T, int>(value, index));
        // Apply ascending or descending sort ordering
        var sorted = ascending ? indexMap.OrderBy(i => i.Key) : indexMap.OrderByDescending(i => i.Key);
        // Get the index values as they should appear once rearranged in the sort operation
        var indices = sorted.Select(s => s.Value);
        // Order the indices by their position in the sort and return the index at that position
        // e.g. as list with values
        // c, b, d, e, a has indices
        // 0, 1, 2, 3, 4 and should be finally sorted
        //
        // a, b, c, d, e
        // 4, 1, 0, 2, 3
        // This is the index order that needs to be applied to the array 
        // segment that will be sorted.  
        return indices.Select((p, i) => (Index: i, Position: p)).OrderBy(i => i.Position).Select(i => i.Index).ToList();
    }
    /// <summary>
    /// Reorders the current list using the indices provided
    /// </summary>
    /// <param name="indices">Index order to sort the list</param>
    public override void Reorder(List<int> indices)
    {
        // Clone the list since it will be reordered while
        // other arrays need the original list to reorder.
        var order = indices.ToList();

        var temp = new T[List.Count];

        for (var i = 0; i < List.Count; i++)
        {
            temp[order[i]] = List[i];
        }

        for (var i = 0; i < List.Count; i++)
        {
            List[i] = temp[i];
            order[i] = i;
        }
    }
    /// <summary>
    /// Returns a segment of the values in the list
    /// </summary>
    /// <param name="offset">Index to start slicing values</param>
    /// <param name="count">Number of values to take in the slice</param>
    public override void Slice(int offset, int count)
    {
        List = List.Skip(offset).Take(count).ToList();
    }
    /// <summary>
    /// Copies the values in the list as a new enumerable list
    /// </summary>
    /// <returns>IEnumerable copy of the typed list values</returns>
    public override IEnumerable CopyValues()
    {
        return List.ToList();
    }
    /// <summary>
    /// Method for efficient cache deserialization of values that
    /// already occupy memory
    /// </summary>
    /// <param name="list">List to set</param>
    // ReSharper disable once UnusedMember.Global
    internal void SetList(List<T> list)
    {
        List = list;
    }
}