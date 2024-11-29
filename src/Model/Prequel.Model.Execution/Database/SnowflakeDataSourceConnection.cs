using Prequel.Engine.Source.Database;
using Snowflake.Data.Client;

namespace Prequel.Model.Execution.Database;

/// <summary>
/// Snowflake database connection data table
/// </summary>
public class SnowflakeDataSourceConnection : DatabaseDataSourceConnection
{
    public required string Schema { get; init; }
    public required string Role { get; init; }

    public override string ConnectionString => $"account={Server};user={UserId};password={Password};ROLE={Role};db={Catalog};schema={Schema}";
    /// <summary>
    /// Creates a Snowflake DatabaseReader instance
    /// </summary>
    /// <param name="query">SQL query</param>
    /// <returns>DatabaseReader instance</returns>
    public override DatabaseDataSourceReader CreateReader(string query)
    {
        return new LimitDatabaseReader(query, () => new SnowflakeDbConnection(ConnectionString));
    }
}