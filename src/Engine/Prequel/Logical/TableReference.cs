namespace Prequel.Logical;

public record TableReference(string Name, string? Alias = null)
{
    public override string ToString()
    {
        return Name + (Alias != null ? $" AS {Alias}" : "");
    }

    public virtual bool Equals(TableReference? other)
    {
        if (other == null)
        {
            return false;
        }

        var equal = Name.Equals(other.Name, StringComparison.InvariantCultureIgnoreCase);

        if (Alias != null)
        {
            equal &= Alias.Equals(other.Alias, StringComparison.InvariantCultureIgnoreCase);
        }

        return equal;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Name, Alias);
    }
}