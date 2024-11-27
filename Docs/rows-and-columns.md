# Rows vs. Columns

## Data Orientation
Many, if not most, RDBMS platforms preserve data using row-based storage.  Likewise, many read data in the same way.  These database engines are typically used for OLTP purposes.  Postgres MySql, and MS SQL are examples of row-based OLTP databases.

Other database platforms use column-based storage and and are suited for analytical workloads (OLAP).  Redshift (based on Postgres), BigQuery, and ClickHouse.

The difference may seem trivial, but it has large impacts on how data is managed and queried.

For write operations in particular, row-oriented OLTP databases often have an advantage.  Each write operation stores the related data in the same general area on disk

```
Row1: ["London", 55, "Apples"]
Row2: ["Rome",   21, "Oranges"]
Row3: ["Berlin", 347, "Peaches"]
```

In a column-oriented database, the same data would be stored as if the table above were turned 90 degrees clockwise, on its side, and columns become rows

```
city: ["London", "Rome", "Berlin"]
totals: [55, 21, 347]
fruit: ["Apples", "Oranges", "Peaches"]
```

This orientation makes it easier and faster to retrieve data.  If you wanted to calculate statistics on the "totals" column, all data from the column can be queried in a single pass instead of having to read through all rows extracting 1 value at a time from each row.

# Column Record Batches
The Prequel engine reads data in rows, but the storage mechanism in memory stores the data in a column-like orientation.  Each `RecordBatch` object contains one or more arrays of data, and the data in each array is of the same underlying type.  This approach makes computational operations much easier since all operations can operate on a single enumerable list of values at a time.

Data stored in Prequel `RecordBatch` objects looks much like the column-oriented example as far as C# is concerned.

The screenshot below shows a record batch from one of the project's sample queries.  Notice how the batch contains numerous (indexed) arrays.  2 byte arrays and 4 string arrays, to be specific.  Each of those arrays contain 19 values.  One of those arrays is expanded to show all the data in the “column” of the same type, and available in the same enumerable object.



The `RecordBatch` class is then responsible for keeping the various arrays synchronized.  For example, if one column is reordered, all other columns need to be reordered.

```c#
public void Reorder(List<int> indices, List<int>? columnsToIgnore = null)
{
    for (var i = 0; i < Results.Count; i++)
    {
        var array = Results[i];


        if (columnsToIgnore != null && columnsToIgnore.Contains(i))
        {
            // Column is already sorted.
            continue;
        }


        array.Reorder(indices);
    }
}
```

Filtering data is similar; simple and efficient

```c#
public void Filter(bool[] filter)
{
    var filterIndices = new List<int>();


    for (var i = filter.Length - 1; i >= 0; i--)
    {
        if (filter[i])
        {
            continue;
        }


        filterIndices.Add(i);
    }


    foreach (var column in Results)
    {
        foreach (var i in filterIndices)
        {
            column.Values.RemoveAt(i);
        }
    }
}
```

The underlying array implementations each have their own reorder logic which takes input from the parent record batch.  Reordering a list is as simple as taking a list of index changes and applying it to the data in the array

```c#
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
```

There is no right or wrong answer in terms of how a query engine should work.  This project uses a column-based approach since most operations are intended to run in memory across multiple data sources.  Nonetheless, you can plug in virtually any source from which you can read data.