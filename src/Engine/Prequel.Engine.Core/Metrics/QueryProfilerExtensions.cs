namespace Prequel.Engine.Core.Metrics;

public static class QueryProfilerExtensions
{
    /// <summary>
    /// Returns a Timing that will measure execution between the time
    /// it was created and when it is disposed
    /// </summary>
    /// <param name="profiler">Current profiler instance</param>
    /// <param name="name">Step name identifier</param>
    /// <returns>Timing step</returns>
    public static Timing Step(this QueryProfiler profiler, string? name) => profiler.StepInner(name);
}