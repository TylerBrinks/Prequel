using Prequel.Engine.Core.Physical.Aggregation;

namespace Prequel.Engine.Core.Physical.Functions;

internal interface IAggregation
{
    Accumulator CreateAccumulator();
}