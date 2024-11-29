namespace Prequel.Engine.Core.Logical.Rules;

/// <summary>
/// Generates unique numbers sequentially starting at 1.
/// </summary>
public class NumericAliasGenerator : IAliasGenerator
{
    private int _aliasIndex;

    /// <summary>
    /// Generates a new alias index and returns it as a name in string format
    /// </summary>
    /// <returns>Unique alias name</returns>
    public string Next()
    {
        return (++_aliasIndex).ToString();
    }
}