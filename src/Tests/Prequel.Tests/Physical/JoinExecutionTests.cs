using Prequel.Engine.Core.Execution;

namespace Prequel.Tests.Physical
{
    public class JoinExecutionTests
    {
        [Fact]
        public void JoinExecution_Appends_Left_Indices()
        {
            var (left, right) = JoinExecution.AppendLeftIndices([ 1, 2, 3 ], [ 2, 3, 4 ], [ 4 ]);
            Assert.Equal([ 1, 2, 3, 4 ], left);
            Assert.Equal([ 2, 3, 4, null ], right);
        }

        [Fact]
        public void JoinExecution_Does_Not_Append_Left_Unmatched()
        {
            var (left, right) = JoinExecution.AppendLeftIndices([1, 2, 3], [2, 3, 4], new long[] { });
            Assert.Equal([1, 2, 3], left);
            Assert.Equal([2, 3, 4], right);
        }

        [Fact]
        public void JoinExecution_Appends_Right_Indices()
        {
            var (left, right) = JoinExecution.AppendRightIndices([1, 2, 3], [2, 3, 4], [4]);
            Assert.Equal([1, 2, 3, null], left);
            Assert.Equal([2, 3, 4, 4], right);
        }

        [Fact]
        public void JoinExecution_Does_Not_Append_Right_Unmatched()
        {
            var (left, right) = JoinExecution.AppendRightIndices([1, 2, 3], [2, 3, 4], new long[] { });
            Assert.Equal([1, 2, 3], left);
            Assert.Equal([2, 3, 4], right);
        }
    }
}
