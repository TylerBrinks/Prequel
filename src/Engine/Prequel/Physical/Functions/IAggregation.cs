using Prequel.Physical.Aggregation;

namespace Prequel.Physical.Functions;

internal interface IAggregation
{
    Accumulator CreateAccumulator();
}