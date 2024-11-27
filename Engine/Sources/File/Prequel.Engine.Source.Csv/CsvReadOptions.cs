namespace Prequel.Engine.Source.Csv;

/// <summary>
/// CSV read execution context
/// </summary>
public class CsvReadOptions
{
    /// <summary>
    /// CSV file delimiter.  Default is a comma.
    /// </summary>
    public string Delimiter { get; set; } = ",";
    /// <summary>
    /// True if the CSV file has a header; otherwise false.
    /// </summary>
    public bool HasHeader { get; set; } = true;
    /// <summary>
    /// Number of records to read when inferring the file's schema
    /// </summary>
    public int InferMax { get; set; } = 100;
}