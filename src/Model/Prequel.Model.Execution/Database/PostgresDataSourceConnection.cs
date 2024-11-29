using Prequel.Engine.Source.Database;
using Npgsql;

namespace Prequel.Model.Execution.Database;

/// <summary>
/// Postgres database connection data table
/// </summary>
public class PostgresDataSourceConnection : DatabaseDataSourceConnection
{
    public PostgresDataSourceConnection()
    {
        Port = "5432";
    }
    private string GetPort()
    {
        return $",{Port}";
    }

    public override string ConnectionString => $"Host={Server}{GetPort()};Username={UserId};Password={Password};Database={Catalog}";
    /// <summary>
    /// Creates a Postgres DatabaseReader instance
    /// </summary>
    /// <param name="query">SQL query</param>
    /// <returns>DatabaseReader instance</returns>
    public override DatabaseDataSourceReader CreateReader(string query)
    {
        return new LimitDatabaseReader(query, () => new NpgsqlConnection(ConnectionString));
    }
}