using Prequel.Logical.Values;
using Prequel.Values;
using Prequel.Data;

namespace Prequel.Tests.Data
{
    public class ValueTests
    {
        [Fact]
        public void BooleanColumnValue_Returns_Boolean_Values()
        {
            var values = new BooleanColumnValue(new []{true, false, true});

            Assert.True((bool)values.GetValue(0)!);
            Assert.False((bool)values.GetValue(1)!);
            Assert.True((bool)values.GetValue(2)!);
        }

        [Fact]
        public void BooleanColumnValue_Inverts_Values()
        {
            var values = new BooleanColumnValue(new[] { true, false, true });

            Assert.True((bool)values.GetValue(0)!);
            Assert.False((bool)values.GetValue(1)!);
            Assert.True((bool)values.GetValue(2)!);

            values.Invert();

            Assert.False((bool)values.GetValue(0)!);
            Assert.True((bool)values.GetValue(1)!);
            Assert.False((bool)values.GetValue(2)!);
        }

        [Fact]
        public void ScalarColumnValue_Returns_Value()
        {
            var value = new ScalarColumnValue(new IntegerScalar(123), 1, ColumnDataType.Integer);
            Assert.Equal(123L, value.Value.RawValue);

            value = new ScalarColumnValue(new BooleanScalar(true), 1, ColumnDataType.Boolean);
            Assert.Equal(true, value.Value.RawValue);

            value = new ScalarColumnValue(new DoubleScalar(123), 1, ColumnDataType.Double);
            Assert.Equal(123D, value.Value.RawValue);

            value = new ScalarColumnValue(new StringScalar("abc"), 1, ColumnDataType.Utf8);
            Assert.Equal("abc", value.Value.RawValue);
        }
    }
}
