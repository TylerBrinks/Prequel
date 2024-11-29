using MySql.Data.MySqlClient;
using Prequel.Engine.Source.Database;

namespace Prequel.Model.Execution.Database;

/// <summary>
/// MySql database connection data table
/// </summary>
public class MySqlDataSourceConnection : DatabaseDataSourceConnection
{
    public MySqlDataSourceConnection()
    {
        Port = "3306";
    }
    public override string ConnectionString => $"server{Server}{GetPort()};uid={UserId};pwd={Password};database={Catalog}";

    private string GetPort()
    {
        return $",{Port}";
    }

    /// <summary>
    /// Creates a MySql DatabaseReader instance
    /// </summary>
    /// <param name="query">SQL query</param>
    /// <returns>DatabaseReader instance</returns>
    public override DatabaseDataSourceReader CreateReader(string query)
    {
        return new LimitDatabaseReader(query, () => new MySqlConnection(ConnectionString));
    }
}