using System.Diagnostics;

namespace Prequel.Engine.Core.Metrics;

/// <summary>
/// Profiler used for measuring discrete and overall executing metrics
/// </summary>
public class QueryProfiler
{
    private Timing? _lastSetHead;

    // Allows async to properly track the attachment point
    private readonly AsyncLocal<Timing?> _head = new();
    /// <summary>
    /// Creates a new profiler instance with an optional root
    /// timing instance name
    /// </summary>
    /// <param name="rootStepName"></param>
    internal QueryProfiler(string? rootStepName = null)
    {
        Started = DateTime.UtcNow;

        // Stopwatch must start before any child Timings are instantiated
        Stopwatch = Stopwatch.StartNew();
        Root = new Timing(this, null, rootStepName);

    }
    /// <summary>
    /// 
    /// </summary>
    /// <returns></returns>
    public static QueryProfiler Start(string? rootStepName)
    {
        return new QueryProfiler(rootStepName);
    }
    /// <summary>
    /// 
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    internal Timing StepInner(string? name) => new(this, Head, name);
    /// <summary>
    /// Returns how many milliseconds have elapsed since <paramref name="startTicks"/> was recorded.
    /// </summary>
    /// <param name="startTicks">The start tick count.</param>
    internal decimal GetDurationMilliseconds(long startTicks) => GetRoundedMilliseconds(ElapsedTicks - startTicks);
    /// <summary>
    /// Returns how many milliseconds have elapsed since <paramref name="ticks"/> was recorded.
    /// </summary>
    internal static decimal GetRoundedMilliseconds(long ticks)
    {
        var times100 = ticks * 100000 / Stopwatch.Frequency;
        return times100 / 100m;
    }
    /// <summary>
    /// Walks the <see cref="Timing"/> hierarchy contained in this profiler, starting with <see cref="Root"/>, and returns each Timing found.
    /// </summary>
    public IEnumerable<Timing> GetTimingHierarchy()
    {
        return Root.GetTimingHierarchy();
    }
    /// <summary>
    /// Stops the profiler all running timings
    /// </summary>
    /// <returns>True if stopping closed active timings; false if the profiler
    /// was already in a stopped state</returns>
    public bool Stop()
    {
        if (!Stopwatch.IsRunning)
        {
            return false;
        }

        Stopwatch.Stop();
        DurationMilliseconds = GetRoundedMilliseconds(ElapsedTicks);

        foreach (var timing in GetTimingHierarchy())
        {
            timing.Stop();
        }

        return true;
    }

    public Guid Id { get; set; } = Guid.NewGuid();
    public Timing Root { get; set; }
    public DateTime Started { get; set; }
    internal Stopwatch Stopwatch { get; set; }
    public decimal DurationMilliseconds { get; set; }
    internal long ElapsedTicks => Stopwatch.ElapsedTicks;
    /// <summary>
    /// Gets or sets points to the currently executing Timing.
    /// </summary>
    public Timing? Head
    {
        get => _head.Value ?? _lastSetHead;
        set => _head.Value = _lastSetHead = value;
    }

}