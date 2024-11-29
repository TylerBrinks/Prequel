using Microsoft.Data.SqlClient;
using Prequel.Engine.Source.MsSql;
using Prequel.Engine.Source.Database;

namespace Prequel.Model.Execution.Database;

/// <summary>
/// Sql database connection data table
/// </summary>
public class MsSqlDataSourceConnection : DatabaseDataSourceConnection
{
    public MsSqlDataSourceConnection()
    {
        Port = "1433";
    }

    private string GetPort()
    {
        return $",{Port}";
    }

    public override string ConnectionString => $"Server=tcp:{Server}{GetPort()};Initial Catalog={Catalog};User ID={UserId};Password={Password};Encrypt=True;TrustServerCertificate=True";
    /// <summary>
    /// Creates a MS Sql DatabaseReader instance
    /// </summary>
    /// <param name="query">SQL query</param>
    /// <returns>DatabaseReader instance</returns>
    public override DatabaseDataSourceReader CreateReader(string query)
    {
        return new MsSqlDatabaseReader(query, () => new SqlConnection(ConnectionString));
    }
}