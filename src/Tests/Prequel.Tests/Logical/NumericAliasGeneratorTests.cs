using Prequel.Logical.Rules;

namespace Prequel.Tests.Logical
{
    public class NumericAliasGeneratorTests
    {
        [Fact]
        public void NumericAliasGenerator_Generates_Sequential_Values()
        {
            var generator = new NumericAliasGenerator();

            for (var i = 1; i < 11; i++)
            {
                var id = generator.Next();
                Assert.Equal(i.ToString(), id);
            }
        }
    }
}
