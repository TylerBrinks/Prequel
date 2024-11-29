namespace Prequel.Engine.Source.Json;

public class JsonOptions
{
    /// <summary>
    /// Number of records to read when inferring the file's schema
    /// </summary>
    public int InferMax { get; set; } = 100;
}