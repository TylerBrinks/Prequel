using System.Text.Json.Serialization;
// ReSharper disable UnusedMember.Global

namespace Prequel.Metrics;

/// <summary>
/// A class for measuring an individual query engine operation
/// </summary>
public class Timing : IDisposable
{
    private readonly long _startTicks;
    private readonly object _syncRoot = new();
    private List<Timing>? _children;
    private readonly List<int> _batchRowCount = [];

    public Timing(QueryProfiler profiler, Timing? parent, string? name)
    {
        Id = Guid.NewGuid();
        Name = name;
        Profiler = profiler;
        Profiler.Head = this;

        while (parent?.DurationMilliseconds.HasValue == true)
        {
            parent = parent.ParentTiming;
        }

        parent?.AddChild(this);

        _startTicks = profiler.ElapsedTicks;
        StartMilliseconds = QueryProfiler.GetRoundedMilliseconds(_startTicks);
    }

    public override string? ToString() => Name;

    /// <summary>
    /// Adds a child timer to the hierarchy with this
    /// instance as the parent.
    /// </summary>
    /// <param name="timing">Child timing</param>
    public void AddChild(Timing timing)
    {
        lock (_syncRoot)
        {
            Children ??= [];
            Children.Add(timing);
        }
        timing.Profiler ??= Profiler;
        timing.ParentTimingId = Id;
        timing.ParentTiming = this;
    }
    /// <summary>
    /// Completes this Timing's duration and sets the profiler's Head up one level.
    /// </summary>
    public void Stop()
    {
        if (DurationMilliseconds != null)
        {
            return;
        }

        if (Profiler is null) { return; }

        DurationMilliseconds = Profiler.GetDurationMilliseconds(_startTicks);
        Profiler.Head = ParentTiming;
    }
    /// <summary>
    /// Stops profiling, allowing the <c>using</c> construct to neatly encapsulate a region to be profiled.
    /// </summary>
    void IDisposable.Dispose() => Stop();
    /// <summary>
    /// Aggregates batch metrics to the overall timing calculations
    /// </summary>
    /// <param name="batchRowCount">Number of rows in the most recent batch</param>
    public void IncrementBatch(int batchRowCount)
    {
        _batchRowCount.Add(batchRowCount);
    }
    /// <summary>
    /// Increments the number of rows read by the current step by 1
    /// </summary>
    public void IncrementRowCount()
    {
        IncrementRowCount(1);
    }
    /// <summary>
    /// Increments the number of rows read by the current step 
    /// by the row count value
    /// <param name="rowCount">Value to increment the current timing row count</param>
    /// </summary>
    public void IncrementRowCount(int rowCount) => RowCount += rowCount;
    /// <summary>
    /// Walks the <see cref="Timing"/> hierarchy contained in this profiler,
    /// starting with Root, and returns each Timing found.
    /// </summary>
    public IEnumerable<Timing> GetTimingHierarchy()
    {
        var timings = new Stack<Timing>();

        timings.Push(this);

        while (timings.Count > 0)
        {
            var timing = timings.Pop();

            yield return timing;

            if (!timing.HasChildren) { continue; }

            var children = timing.Children;
            for (var i = children!.Count - 1; i >= 0; i--)
            {
                timings.Push(children[i]);
            }
        }
    }

    public Guid Id { get; }
    public Guid ParentTimingId { get; set; }
    public string? Name { get; }
    [JsonIgnore]
    public Timing? ParentTiming { get; set; }
    [JsonIgnore]
    internal QueryProfiler? Profiler { get; private set; }
    public decimal StartMilliseconds { get; set; }
    public decimal? DurationMilliseconds { get; set; }
    public long RowCount { get; private set; }
    public bool IsRoot => Equals(Profiler?.Root);
    public int BatchCount => _batchRowCount.Count;
    public long BatchRowCount => _batchRowCount.Sum();
    public double BatchRowCountAverage => _batchRowCount.Count != 0 ? _batchRowCount.Average() : 0;
    [JsonIgnore]
    public bool HasChildren => Children?.Count > 0;
    public decimal? DurationIsolatedMilliseconds
    {
        get
        {
            if (DurationMilliseconds is null)
            {
                return 0;
            }

            if (Children == null)
            {
                return DurationMilliseconds;
            }

            var childExecution = Children!.Sum(c => c.DurationMilliseconds);

            return Math.Abs(DurationMilliseconds.Value - childExecution!.Value);
        }
    }
    public List<Timing>? Children
    {
        get => _children;
        set
        {
            if (value?.Count > 0)
            {
                lock (value)
                {
                    foreach (var t in value)
                    {
                        t.ParentTiming = this;
                    }
                }
            }
            _children = value;
        }
    }
}