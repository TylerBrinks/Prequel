using System.Collections;

namespace Prequel.Data;

/// <summary>
/// A RecordBatch combines schema and data during physical plan execution.
/// Schemas are used to coordinate how expressions are handed when data is
/// read from a source or a physical execution plan.  Results are stored in
/// a list of RecordArray objects which contain a list containing any of the
/// supported data type values.
/// </summary>
public record RecordBatch
{
    private readonly List<RecordArray> _results = [];

    /// <summary>
    /// Creates a new Record Batch instance
    /// </summary>
    /// <param name="schema">Schema defining each field in the record batch</param>
    public RecordBatch(Schema schema)
    {
        Schema = schema;

        foreach (var field in schema.Fields)
        {
            AddResultArray(GetArrayType(field));
        }
    }

    /// <summary>
    /// Schema of the data held in the result arrays
    /// </summary>
    public Schema Schema { get; }
    /// <summary>
    /// Number of rows in each of the result arrays
    /// </summary>
    public int RowCount => _results.Count > 0 ? _results.First().Values.Count : 0;

    /// <summary>
    /// Result lists
    /// </summary>
    public IReadOnlyList<RecordArray> Results => _results.AsReadOnly();
    /// <summary>
    /// Adds a new result to the result array at the given index
    /// </summary>
    /// <param name="ordinal">Index of the array</param>
    /// <param name="value">Value to store</param>
    public void AddResult(int ordinal, object? value)
    {
        // Attempt to add the result
        var added = Results[ordinal].Add(value);

        // Numeric results may exceed the boundary of the current array 
        // at which point the numeric array will be up-cast to an 
        // instance that can handle the data.
        if (!added && Results[ordinal] is IUpcastNumericArray numeric)
        {
            _results[ordinal] = numeric.Upcast(value);
        }
    }
    /// <summary>
    /// Adds a new record array to the list of results
    /// </summary>
    /// <param name="array">Result array to add</param>
    internal void AddResultArray(RecordArray array)
    {
        _results.Add(array);
    }
    /// <summary>
    /// Gets a record array for a given field
    /// </summary>
    /// <param name="field">Field to build the array around</param>
    /// <returns>RecordArray instance</returns>
    internal static RecordArray GetArrayType(Field? field)
    {
        if (field != null)
        {
            return field.NumericType.HasValue
                ? CreateNumericField(field.NumericType.Value)
                : CreateRecordArray(field.DataType);
        }

        return new StringArray();
    }
    /// <summary>
    /// Creates a type record array based on the supplied column data type.
    /// </summary>
    /// <param name="dataType">Column data type</param>
    /// <returns>Typed RecordArray instance</returns>
    internal static RecordArray CreateRecordArray(ColumnDataType dataType)
    {
        return dataType switch
        {
            ColumnDataType.Utf8 => new StringArray(),
            ColumnDataType.Integer => new ByteArray(),
            ColumnDataType.Boolean => new BooleanArray(),
            ColumnDataType.Double => new DoubleArray(),
            ColumnDataType.Date32 or
                ColumnDataType.TimestampSecond => new DateTimeArray(),
            //or ColumnDataType.TimestampMillisecond
            //or ColumnDataType.TimestampMicrosecond
            ColumnDataType.TimestampNanosecond => new TimeStampArray(),
            _ => new StringArray()
        };
    }
    /// <summary>
    /// Creates a type record array based on the supplied integer type.
    /// </summary>
    /// <param name="integerType">Integer data type</param>
    /// <returns>Typed RecordArray instance</returns>
    internal static RecordArray CreateNumericField(IntegerDataType integerType)
    {
        return integerType switch
        {
            IntegerDataType.Byte => new ByteArray(),
            IntegerDataType.Short => new ShortArray(),
            IntegerDataType.Integer => new IntegerArray(),
            IntegerDataType.Long => new LongArray(),
            _ => new StringArray()
        };
    }
    /// <summary>
    /// Reorders the values of each column in the record batch by the specified index values.
    /// Optionally ignores specific column indices
    /// </summary>
    /// <param name="indices">Column index values by which the existing batch will be reordered</param>
    /// <param name="columnsToIgnore">Optional columns to ignore when reordering</param>
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
    /// <summary>
    /// Prunes the record batch results by removing records prior to or 
    /// subsequent to the offset and count values
    /// </summary>
    /// <param name="offset">Number of records to offset</param>
    /// <param name="count">Number of records to preserve after the offset</param>
    /// <exception cref="IndexOutOfRangeException"></exception>
    public void Slice(int offset, int count)
    {
        if (offset + count > RowCount)
        {
            throw new IndexOutOfRangeException();
        }

        foreach (var array in Results)
        {
            array.Slice(offset, count);
        }
    }
    /// <summary>
    /// RecordBatch instances may grow beyond the configured batch size during
    /// physical execution steps such as aggregations.  This method splits
    /// an oversized batch into an enumerable list of batches that meet
    /// the configured batch size criteria
    /// </summary>
    /// <param name="batchSize">Record count used to resize the batch into smaller slices</param>
    /// <returns>IEnumerable list of RecordBatch instances</returns>
    public IEnumerable<RecordBatch> Repartition(int batchSize)
    {
        var rowCount = RowCount;
        var count = 0;

        var partition = new RecordBatch(Schema);

        for (var i = 0; i < rowCount; i++)
        {
            for (var j = 0; j < Results.Count; j++)
            {
                partition.AddResult(j, Results[j].Values[i]);
            }

            count++;

            if (count != batchSize) { continue; }

            yield return partition;

            partition = new RecordBatch(Schema);
            count = 0;
        }

        if (count > 0)
        {
            yield return partition;
        }
    }
    /// <summary>
    /// Attempts to create a new record batch with a given schema and "row" of object values
    /// </summary>
    /// <param name="schema">Batch schema</param>
    /// <param name="columns">Values for each column in the schema</param>
    /// <returns>New RecordBatch instance</returns>
    /// <exception cref="InvalidOperationException">Thrown if the number of columns does not match the number of schema fields.</exception>
    public static RecordBatch TryNew(Schema schema, List<object?> columns)
    {
        if (schema.Fields.Count != columns.Count)
        {
            throw new InvalidOperationException("Number of columns must match the number of fields");
        }

        var batch = new RecordBatch(schema);

        for (var i = 0; i < columns.Count; i++)
        {
            batch.AddResult(i, columns[i]);
        }

        return batch;
    }
    /// <summary>
    /// Attempts to create a new record batch with a given schema and a list of "rows" of object values
    /// </summary>
    /// <param name="schema">Batch schema</param>
    /// <param name="columns">List of values for each column in the schema</param>
    /// <returns></returns>
    public static RecordBatch TryNewWithLists(Schema schema, List<IList> columns)
    {
        var batch = new RecordBatch(schema);

        for (var i = 0; i < columns.Count; i++)
        {
            foreach (var value in columns[i])
            {
                batch.AddResult(i, value);
            }
        }

        return batch;
    }
    /// <summary>
    /// Combines the current batch with another batch merging the results
    /// </summary>
    /// <param name="leftBatch">Batch with values to merge into the current batch</param>
    public void Concat(RecordBatch leftBatch)
    {
        for (var i = 0; i < Results.Count; i++)
        {
            foreach (var value in leftBatch.Results[i].Values)
            {
                AddResult(i, value);
            }
        }
    }
    /// <summary>
    /// Uses a bit mask to filter out results in the current batch
    /// </summary>
    /// <param name="filter">Bitmask filter.  Values at indices with True value are preserved; False are filtered out.</param>
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
    /// <summary>
    /// Truncates columns from a record batch
    /// </summary>
    /// <param name="projection">Projection containing indices of columns to preserve</param>
    public void Project(List<int> projection)
    {
        for (var i = Results.Count - 1; i >= 0; i--)
        {
            if (projection.Contains(i))
            {
                continue;
            }

            _results.RemoveAt(i);
            Schema.Fields.RemoveAt(i);
        }
    }
    /// <summary>
    /// Makes a copy of the current record batch.  
    /// </summary>
    /// <returns>New RecordBatch with a copy of the current batch schema and data</returns>
    public RecordBatch Copy()
    {
        var batch = new RecordBatch(Schema);

        for (var i = 0; i < Results.Count; i++)
        {
            var result = Results[i];
            foreach (var value in result.CopyValues())
            {
                batch.AddResult(i, value);
            }
        }

        return batch;
    }
    /// <summary>
    /// Creates a copy of the current batch and schema
    /// </summary>
    /// <returns>Cloned batch</returns>
    public RecordBatch CloneBatch()
    {
        var fields = Schema.Fields.Select(f => f).ToList();
        var clonedSchema = new Schema(fields);

        var batch = new RecordBatch(clonedSchema);
        batch._results.Clear();

        foreach (var row in Results.Select(CloneArray))
        {
            batch._results.Add(row);
        }

        return batch;

        static RecordArray CloneArray(RecordArray source)
        {
            var target = source.NewEmpty(source.Values.Count);
            foreach (var value in source.CopyValues())
            {
                target.Add(value);
            }
            return target;
        }
    }
}
