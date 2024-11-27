using System.Data;
using System.Data.Common;

namespace Prequel.Tests.Database;

public class TestDbConnection : DbConnection
{
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        throw new NotImplementedException();
    }

    public override void ChangeDatabase(string databaseName)
    {
        throw new NotImplementedException();
    }

    public override void Close()
    {
        Console.WriteLine("Test connection closed.");
    }

    public override void Open()
    {
        Console.WriteLine("Test connection opened.");
    }

    public override string ConnectionString { get; set; }
    public override string Database { get; }
    public override ConnectionState State { get; }
    public override string DataSource { get; }
    public override string ServerVersion { get; }

    protected override DbCommand CreateDbCommand()
    {
        return new TestDbCommand();
    }
}