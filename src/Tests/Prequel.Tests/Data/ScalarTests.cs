using Prequel.Logical.Values;

namespace Prequel.Tests.Data
{
    public class ScalarTests
    {
        [Fact]
        public void BooleanScalar_Checks_Equality()
        {
            var scalar = new BooleanScalar(true);

            Assert.True(scalar.IsEqualTo(true));
            Assert.False(scalar.IsEqualTo("true"));
            Assert.False(scalar.IsEqualTo(null));

            scalar = new BooleanScalar(false);
            Assert.True(scalar.IsEqualTo(false));
            Assert.False(scalar.IsEqualTo("false"));
            Assert.False(scalar.IsEqualTo("abc"));
        }

        [Fact]
        public void DoubleScalar_Checks_Equality()
        {
            var scalar = new DoubleScalar(12.34);

            Assert.True(scalar.IsEqualTo(12.34));
            Assert.True(scalar.IsEqualTo(12.34M));
            Assert.True(scalar.IsEqualTo("12.34"));
            Assert.False(scalar.IsEqualTo("123.4"));
            Assert.False(scalar.IsEqualTo(null));
            Assert.False(scalar.IsEqualTo("abc"));
        }

        [Fact]
        public void IntegerScalar_Checks_Equality()
        {
            var scalar = new IntegerScalar(1234);

            Assert.True(scalar.IsEqualTo(1234));
            Assert.True(scalar.IsEqualTo(1234M));
            Assert.True(scalar.IsEqualTo(1234D));
            Assert.True(scalar.IsEqualTo(1234L));
            Assert.True(scalar.IsEqualTo("1234"));
            Assert.False(scalar.IsEqualTo("2345"));
            Assert.False(scalar.IsEqualTo(null));
            Assert.False(scalar.IsEqualTo("abc"));
        }

        [Fact]
        public void StringScalar_Checks_Equality()
        {
            var scalar = new StringScalar("1234");

            Assert.True(scalar.IsEqualTo("1234"));
            Assert.True(scalar.IsEqualTo(1234));
            Assert.True(scalar.IsEqualTo(1234M));
            Assert.True(scalar.IsEqualTo(1234D));
            Assert.True(scalar.IsEqualTo(1234L));
            Assert.False(scalar.IsEqualTo("2345"));
            Assert.False(scalar.IsEqualTo(null));

            scalar = new StringScalar(null);
            Assert.True(scalar.IsEqualTo(null));
        }
    }
}
