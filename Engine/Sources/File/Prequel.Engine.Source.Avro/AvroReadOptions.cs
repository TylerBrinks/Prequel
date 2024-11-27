namespace Prequel.Engine.Source.Avro;

public class AvroReadOptions
{
    /// <summary>
    /// Number of records to read when inferring the file's schema
    /// </summary>
    public int InferMax { get; set; } = 100;
}