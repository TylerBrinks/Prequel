using System.Collections;

namespace Prequel.Data;

/// <summary>
/// Record array containing a list of value used during physical operations
/// </summary>
public abstract class RecordArray
{
    public abstract IList Values { get; }

    /// <summary>
    /// Adds a new value to the array
    /// </summary>
    /// <param name="value">Value to add</param>
    internal abstract bool Add(object? value);
    /// <summary>
    /// Gets indices for sorted values
    /// </summary>
    /// <param name="ascending">True if sorted ascending; otherwise false;</param>
    /// <param name="start">Sort range start index</param>
    /// <param name="take">Sort range end index</param>
    /// <returns></returns>
    public abstract List<int> GetSortIndices(bool ascending, int? start = null, int? take = null);
    /// <summary>
    /// Reorders a list using a list of index values;
    /// </summary>
    /// <param name="indices">Index reorder values</param>
    public abstract void Reorder(List<int> indices);
    /// <summary>
    /// Gets a subset of values from the array list.
    /// </summary>
    /// <param name="offset">Slice offset (skip)</param>
    /// <param name="count">Slice count (take)</param>
    public abstract void Slice(int offset, int count);
    /// <summary>
    /// Creates a new record array of a given size
    /// </summary>
    /// <param name="size">Array size</param>
    /// <returns>New RecordArray</returns>
    public abstract RecordArray NewEmpty(int size);
    /// <summary>
    /// Fills the array with a number of null values
    /// </summary>
    /// <param name="count">null value count</param>
    /// <returns>Current array instance</returns>
    public virtual RecordArray FillWithNull(int count)
    {
        for (var i = 0; i < count; i++)
        {
            Add(null);
        }

        return this;
    }
    /// <summary>
    /// Creates a new list of a given size form a value at a specific index
    /// </summary>
    /// <param name="size">New array size</param>
    /// <param name="index">Index of the value to repeat</param>
    /// <returns></returns>
    public IList ToArrayOfSize(int size, int index)
    {
        var value = Values[index];

        var array = new List<object?>(size);

        for (var i = 0; i < size; i++)
        {
            array.Add(value);
        }

        return array;
    }
    /// <summary>
    /// Copy all array values
    /// </summary>
    /// <returns>IEnumerable instance</returns>
    public abstract IEnumerable CopyValues();
    /// <summary>
    /// Gets a string representation of the underlying value
    /// </summary>
    /// <param name="ordinal">Index of the value to serialize</param>
    /// <returns>Value string representation</returns>
    public virtual string GetStringValue(int ordinal)
    {
        return Values[ordinal]?.ToString() ?? "";
    }
}