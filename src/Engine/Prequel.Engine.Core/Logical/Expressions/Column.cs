using SqlParser.Ast;
using SqlParser.Dialects;

namespace Prequel.Engine.Core.Logical.Expressions;

/// <summary>
/// Defines a column with an optional table reference
/// </summary>
/// <param name="Name"></param>
/// <param name="Relation"></param>
public record Column(string Name, TableReference? Relation = null) : ILogicalExpression
{
    public override string ToString()
    {
        var relation = Relation != null ? Relation.Alias ?? Relation.Name + "." : string.Empty;
        return $"{relation}{Name}";
    }

    /// <summary>
    /// Gets the column's flattened name by combining the table relation
    /// with a '.' delimiter and the column's name
    /// </summary>
    public string FlatName => Relation != null ? $"{Relation.Name}.{Name}" : Name;

    public virtual bool Equals(Column? other)
    {
        if (other == null)
        {
            return false;
        }

        var equal = Name.Equals(other.Name, StringComparison.InvariantCultureIgnoreCase);

        if (equal && Relation != null)
        {
            equal &= Relation == other.Relation;
        }

        return equal;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Name, Relation);
    }

    /// <summary>
    /// Creates a column from a flat name. A table relation will be
    /// inferred if one exists
    /// </summary>
    /// <param name="flatName">Full field name</param>
    /// <returns>Column, qualified if tne name contains multiple identifiers</returns>
    public static Column FromQualifiedName(string flatName)
    {
        var idents = ParseIdentifiersNormalized(flatName).ToList();

        var (relation, name) = idents.Count switch
        {
            1 => (null, idents.First()),
            2 => (new TableReference(idents[0]), idents[1]),
            3 => (new TableReference(idents[0], idents[1]), idents[2]),
            //4 => ()
            _ => (null, flatName)
        };

        return new Column(name, relation);
    }
    /// <summary>
    /// Parses a string name into a list of string identifier values
    /// </summary>
    /// <param name="name">Name to parse</param>
    /// <returns>Enumerable list of strings</returns>
    private static IEnumerable<string> ParseIdentifiersNormalized(string name)
    {
        return ParseIdentifiers(name).Select(ident => ident.QuoteStyle != null
            ? ident.Value
            : ident.Value.ToLowerInvariant())
            .ToList();
    }
    /// <summary>
    /// Parses a strings into a list of SQL identifiers
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    private static IEnumerable<Ident> ParseIdentifiers(string name)
    {
        var parser = new Parser().TryWithSql(name, new GenericDialect());
        return parser.ParseIdentifiers();
    }
}