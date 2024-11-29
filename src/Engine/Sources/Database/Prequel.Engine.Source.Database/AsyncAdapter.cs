using System.Collections.Concurrent;
using System.Data.Common;
using System.Data;
using System.Reflection;
using System.Diagnostics.CodeAnalysis;
// ReSharper disable UnusedMember.Global
// ReSharper disable InconsistentNaming
// ReSharper disable MemberHidesStaticFromOuterClass

namespace Prequel.Engine.Source.Database;

[ExcludeFromCodeCoverage]
public static class AsyncAdapter
{
    private static readonly ConcurrentDictionary<Type, ConnectionAdapter> ConnectionAdapters = new();
    private static readonly ConcurrentDictionary<Type, CommandAdapter> CommandAdapters = new();
    private static readonly ConcurrentDictionary<Type, DataReaderAdapter> DataReaderAdapters = new();

    private class ConnectionAdapter
    {
        internal readonly Func<IDbConnection, Task> OpenAsync;
        internal readonly Func<IDbConnection, CancellationToken, Task> OpenAsyncToken;

        internal ConnectionAdapter(Type type)
        {
            if (type.GetRuntimeMethod("OpenAsync", []) != null)
            {
                OpenAsync = async connection =>
                {
                    dynamic cmd = connection;
                    await cmd.OpenAsync();
                };
            }
            else
            {
                OpenAsync = async connection => await Task.Run(() =>
                {
                    connection.Open();
                });
            }

            if (type.GetRuntimeMethod("OpenAsync", [typeof(CancellationToken)]) != null)
            {
                OpenAsyncToken = async (connection, token) =>
                {
                    dynamic cmd = connection;
                    await cmd.OpenAsync(token);
                };
            }
            else
            {
                OpenAsyncToken = async (connection, _) => await Task.Run(() =>
                {
                    connection.Open();
                });
            }
        }
    }
    private class CommandAdapter
    {
        internal readonly Func<IDbCommand, Task<int>> ExecuteNonQueryAsync;
        internal readonly Func<IDbCommand, CancellationToken, Task<int>> ExecuteNonQueryAsyncToken;

        internal readonly Func<IDbCommand, Task<IDataReader>> ExecuteReaderAsync;
        internal readonly Func<IDbCommand, CancellationToken, Task<IDataReader>> ExecuteReaderAsyncToken;
        internal readonly Func<IDbCommand, CommandBehavior, Task<IDataReader>> ExecuteReaderAsyncBehavior;
        internal readonly Func<IDbCommand, CommandBehavior, CancellationToken, Task<IDataReader>> ExecuteReaderAsyncBehaviorToken;

        internal readonly Func<IDbCommand, Task<object>> ExecuteScalarAsync;
        internal readonly Func<IDbCommand, CancellationToken, Task<object>> ExecuteScalarAsyncToken;

        internal CommandAdapter(Type type)
        {
            #region ExecuteNonQueryAsync
            if (type.GetRuntimeMethod("ExecuteNonQueryAsync", []) != null)
            {
                ExecuteNonQueryAsync = async command =>
                {
                    dynamic cmd = command;
                    return await cmd.ExecuteNonQueryAsync();
                };
            }
            else
            {
                ExecuteNonQueryAsync = async command => await Task.FromResult(command.ExecuteNonQuery());
            }

            if (type.GetRuntimeMethod("ExecuteNonQueryAsync", [typeof(CancellationToken)]) != null)
            {
                ExecuteNonQueryAsyncToken = async (command, token) =>
                {
                    dynamic cmd = command;
                    return await cmd.ExecuteNonQueryAsync(token);
                };
            }
            else
            {
                ExecuteNonQueryAsyncToken = async (command, _) => await Task.FromResult(command.ExecuteNonQuery());
            }
            #endregion ExecuteNonQueryAsync

            #region ExecuteReaderAsync

            if (type.GetRuntimeMethod("ExecuteReaderAsync", []) != null)
            {
                ExecuteReaderAsync = async command =>
                {
                    dynamic cmd = command;
                    return await cmd.ExecuteReaderAsync();
                };
            }
            else
            {
                ExecuteReaderAsync = async command => await Task.FromResult(command.ExecuteReader());
            }

            if (type.GetRuntimeMethod("ExecuteReaderAsync", [typeof(CancellationToken)]) != null)
            {
                ExecuteReaderAsyncToken = async (command, token) =>
                {
                    dynamic cmd = command;
                    return await cmd.ExecuteReaderAsync(token);
                };
            }
            else
            {
                ExecuteReaderAsyncToken = async (command, _) => await Task.FromResult(command.ExecuteReader());
            }

            if (type.GetRuntimeMethod("ExecuteReaderAsync", [typeof(CommandBehavior)]) != null)
            {
                ExecuteReaderAsyncBehavior = async (command, behavior) =>
                {
                    dynamic cmd = command;
                    return await cmd.ExecuteReaderAsync(behavior);
                };
            }
            else
            {
                ExecuteReaderAsyncBehavior = async (command, _) => await Task.FromResult(command.ExecuteReader());
            }

            if (type.GetRuntimeMethod("ExecuteReaderAsync", [typeof(CommandBehavior), typeof(CancellationToken)]) != null)
            {
                ExecuteReaderAsyncBehaviorToken = async (command, behavior, token) =>
                {
                    dynamic cmd = command;
                    return await cmd.ExecuteReaderAsync(behavior, token);
                };
            }
            else
            {
                ExecuteReaderAsyncBehaviorToken = async (command, _, _) => await Task.FromResult(command.ExecuteReader());
            }

            #endregion ExecuteReaderAsync

            #region ExecuteScalarAsync

            if (type.GetRuntimeMethod("ExecuteScalarAsync", []) != null)
            {
                ExecuteScalarAsync = async command =>
                {
                    dynamic cmd = command;
                    return await cmd.ExecuteScalarAsync();
                };
            }
            else
            {
                ExecuteScalarAsync = async command => await Task.FromResult(command.ExecuteScalarAsync());
            }

            if (type.GetRuntimeMethod("ExecuteScalarAsync", [typeof(CancellationToken)]) != null)
            {
                ExecuteScalarAsyncToken = async (command, _) =>
                {
                    dynamic cmd = command;
                    return await cmd.ExecuteScalarAsync();
                };
            }
            else
            {
                ExecuteScalarAsyncToken = async (command, _) => await Task.FromResult(command.ExecuteScalarAsync());
            }

            #endregion ExecuteScalarAsync
        }
    }
    private class DataReaderAdapter
    {
        internal readonly Func<IDataReader, int, Task<bool>> IsDBNullAsync;
        internal readonly Func<IDataReader, int, CancellationToken, Task<bool>> IsDBNullAsyncToken;
        internal readonly Func<IDataReader, Task<bool>> NextResultAsync;
        internal readonly Func<IDataReader, CancellationToken, Task<bool>> NextResultAsyncToken;
        internal readonly Func<IDataReader, Task<bool>> ReadAsync;
        internal readonly Func<IDataReader, CancellationToken, Task<bool>> ReadAsyncToken;
        private readonly MethodInfo? _getFieldValueAsync;
        private readonly MethodInfo? _getFieldValueAsyncToken;

        internal DataReaderAdapter(Type type)
        {
            if (type.GetRuntimeMethod("IsDBNullAsync", [typeof(int)]) != null)
            {
                IsDBNullAsync = async (reader, ordinal) =>
                {
                    dynamic cmd = reader;
                    return await cmd.IsDBNullAsync(ordinal);
                };
            }
            else
            {
                IsDBNullAsync = async (reader, ordinal) => await Task.FromResult(reader.IsDBNull(ordinal));
            }

            if (type.GetRuntimeMethod("IsDBNullAsync", [typeof(int), typeof(CancellationToken)]) != null)
            {
                IsDBNullAsyncToken = async (reader, ordinal, token) =>
                {
                    dynamic cmd = reader;
                    return await cmd.IsDBNullAsync(ordinal, token);
                };
            }
            else
            {
                IsDBNullAsyncToken = async (reader, ordinal, _) => await Task.FromResult(reader.IsDBNull(ordinal));
            }

            if (type.GetRuntimeMethod("NextResultAsync", []) != null)
            {
                NextResultAsync = async reader =>
                {
                    dynamic cmd = reader;
                    return await cmd.NextResultAsync();
                };
            }
            else
            {
                NextResultAsync = async reader => await Task.FromResult(reader.NextResult());
            }

            if (type.GetRuntimeMethod("NextResultAsync", [typeof(int), typeof(CancellationToken)]) != null)
            {
                NextResultAsyncToken = async (reader, token) =>
                {
                    dynamic cmd = reader;
                    return await cmd.NextResultAsync(token);
                };
            }
            else
            {
                NextResultAsyncToken = async (reader, _) => await Task.FromResult(reader.NextResult());
            }

            if (type.GetRuntimeMethod("ReadAsync", []) != null)
            {
                ReadAsync = async reader =>
                {
                    dynamic cmd = reader;
                    return await cmd.ReadAsync();
                };
            }
            else
            {
                ReadAsync = async reader => await Task.FromResult(reader.Read());
            }

            if (type.GetRuntimeMethod("ReadAsync", [typeof(int), typeof(CancellationToken)]) != null)
            {
                ReadAsyncToken = async (reader, token) =>
                {
                    dynamic cmd = reader;
                    return await cmd.ReadAsync(token);
                };
            }
            else
            {
                ReadAsyncToken = async (reader, _) => await Task.FromResult(reader.Read());
            }

            // for template function we have to defer checks
            _getFieldValueAsync = type.GetRuntimeMethod("GetFieldValueAsync", [typeof(int)]);
            _getFieldValueAsyncToken = type.GetRuntimeMethod("GetFieldValueAsync", [typeof(int), typeof(CancellationToken)]);
        }


        internal Task<T> DoGetFieldValueAsync<T>(IDataReader reader, int ordinal)
        {
            if (_getFieldValueAsync != null)
            {
                var method = _getFieldValueAsync.MakeGenericMethod(typeof(T));
                return (Task<T>)method.Invoke(reader, [ordinal]);
            }
            return Task.FromResult((T)reader.GetValue(ordinal));
        }
        internal Task<T> DoGetFieldValueAsync<T>(IDataReader reader, int ordinal, CancellationToken token)
        {
            if (_getFieldValueAsyncToken != null)
            {
                var method = _getFieldValueAsyncToken.MakeGenericMethod(typeof(T));
                return (Task<T>)method.Invoke(reader, [ordinal, token]);
            }
            return Task.FromResult((T)reader.GetValue(ordinal));
        }
    }


    public static Task OpenAsync(this IDbConnection connection)
    {
        return connection switch
        {
            null => throw new ArgumentNullException(nameof(connection)),
            DbConnection dbConnection => dbConnection.OpenAsync(),
            _ => ConnectionAdapters.GetOrAdd(connection.GetType(), type => new ConnectionAdapter(type))
                .OpenAsync(connection)
        };
    }

    public static Task OpenAsync(this IDbConnection connection, CancellationToken token)
    {
        return connection switch
        {
            null => throw new ArgumentNullException(nameof(connection)),
            DbConnection dbConnection => dbConnection.OpenAsync(token),
            _ => ConnectionAdapters.GetOrAdd(connection.GetType(), type => new ConnectionAdapter(type))
                .OpenAsyncToken(connection, token)
        };
    }


    public static Task<int> ExecuteNonQueryAsync(this IDbCommand command)
    {
        return command switch
        {
            null => throw new ArgumentNullException(nameof(command)),
            DbCommand dbCommand => dbCommand.ExecuteNonQueryAsync(),
            _ => CommandAdapters.GetOrAdd(command.GetType(), type => new CommandAdapter(type))
                .ExecuteNonQueryAsync(command)
        };
    }

    public static Task<int> ExecuteNonQueryAsync(this IDbCommand command, CancellationToken token)
    {
        return command switch
        {
            null => throw new ArgumentNullException(nameof(command)),
            DbCommand dbCommand => dbCommand.ExecuteNonQueryAsync(token),
            _ => CommandAdapters.GetOrAdd(command.GetType(), type => new CommandAdapter(type))
                .ExecuteNonQueryAsyncToken(command, token)
        };
    }

    public static Task<IDataReader> ExecuteReaderAsync(this IDbCommand command)
    {
        return command switch
        {
            null => throw new ArgumentNullException(nameof(command)),
            DbCommand dbCommand => dbCommand.ExecuteReaderAsync().ContinueWith<IDataReader>(t => t.Result),
            _ => CommandAdapters.GetOrAdd(command.GetType(), type => new CommandAdapter(type))
                .ExecuteReaderAsync(command)
        };
    }

    public static Task<IDataReader> ExecuteReaderAsync(this IDbCommand command, CancellationToken token)
    {
        switch (command)
        {
            case null:
                throw new ArgumentNullException(nameof(command));
            case DbCommand dbCommand:
                return dbCommand.ExecuteReaderAsync(token).ContinueWith<IDataReader>(t => t.Result, token);
            default:
                return CommandAdapters.GetOrAdd(command.GetType(), type => new CommandAdapter(type)).ExecuteReaderAsyncToken(command, token);
        }
    }

    public static Task<IDataReader> ExecuteReaderAsync(this IDbCommand command, CommandBehavior commandBehavior)
    {
        return command switch
        {
            null => throw new ArgumentNullException(nameof(command)),
            DbCommand dbCommand => dbCommand.ExecuteReaderAsync(commandBehavior)
                .ContinueWith<IDataReader>(t => t.Result),
            _ => CommandAdapters.GetOrAdd(command.GetType(), type => new CommandAdapter(type))
                .ExecuteReaderAsyncBehavior(command, commandBehavior)
        };
    }

    public static Task<IDataReader> ExecuteReaderAsync(this IDbCommand command, CommandBehavior commandBehavior,
        CancellationToken token)
    {
        return command switch
        {
            null => throw new ArgumentNullException(nameof(command)),
            DbCommand dbCommand => dbCommand.ExecuteReaderAsync(commandBehavior, token)
                .ContinueWith<IDataReader>(t => t.Result, token),
            _ => CommandAdapters.GetOrAdd(command.GetType(), type => new CommandAdapter(type))
                .ExecuteReaderAsyncBehaviorToken(command, commandBehavior, token)
        };
    }

    public static Task<object> ExecuteScalarAsync(this IDbCommand command)
    {
        return command switch
        {
            null => throw new ArgumentNullException(nameof(command)),
            DbCommand dbCommand => dbCommand.ExecuteScalarAsync()!,
            _ => CommandAdapters.GetOrAdd(command.GetType(), type => new CommandAdapter(type))
                .ExecuteScalarAsync(command)
        };
    }

    public static Task<object> ExecuteScalarAsync(this IDbCommand command, CancellationToken token)
    {
        return command switch
        {
            null => throw new ArgumentNullException(nameof(command)),
            DbCommand dbCommand => dbCommand.ExecuteScalarAsync(token)!,
            _ => CommandAdapters.GetOrAdd(command.GetType(), type => new CommandAdapter(type))
                .ExecuteScalarAsyncToken(command, token)
        };
    }



    public static Task<T> GetFieldValueAsync<T>(this IDataReader reader, int ordinal)
    {
        return reader switch
        {
            null => throw new ArgumentNullException(nameof(reader)),
            DbDataReader dataReader => dataReader.GetFieldValueAsync<T>(ordinal),
            _ => DataReaderAdapters.GetOrAdd(reader.GetType(), type => new DataReaderAdapter(type))
                .DoGetFieldValueAsync<T>(reader, ordinal)
        };
    }
    public static Task<T> GetFieldValueAsync<T>(this IDataReader reader, int ordinal, CancellationToken token)
    {
        return reader switch
        {
            null => throw new ArgumentNullException(nameof(reader)),
            DbDataReader dataReader => dataReader.GetFieldValueAsync<T>(ordinal, token),
            _ => DataReaderAdapters.GetOrAdd(reader.GetType(), type => new DataReaderAdapter(type))
                .DoGetFieldValueAsync<T>(reader, ordinal, token)
        };
    }
    public static Task<bool> IsDBNullAsync(this IDataReader reader, int ordinal)
    {
        return reader switch
        {
            null => throw new ArgumentNullException(nameof(reader)),
            DbDataReader dataReader => dataReader.IsDBNullAsync(ordinal),
            _ => DataReaderAdapters.GetOrAdd(reader.GetType(), type => new DataReaderAdapter(type))
                .IsDBNullAsync(reader, ordinal)
        };
    }
    public static Task<bool> IsDBNullAsync(this IDataReader reader, int ordinal, CancellationToken token)
    {
        return reader switch
        {
            null => throw new ArgumentNullException(nameof(reader)),
            DbDataReader dataReader => dataReader.IsDBNullAsync(ordinal, token),
            _ => DataReaderAdapters.GetOrAdd(reader.GetType(), type => new DataReaderAdapter(type))
                .IsDBNullAsyncToken(reader, ordinal, token)
        };
    }
    public static Task<bool> NextResultAsync(this IDataReader reader)
    {
        return reader switch
        {
            null => throw new ArgumentNullException(nameof(reader)),
            DbDataReader dataReader => dataReader.NextResultAsync(),
            _ => DataReaderAdapters.GetOrAdd(reader.GetType(), type => new DataReaderAdapter(type))
                .NextResultAsync(reader)
        };
    }
    public static Task<bool> NextResultAsync(this IDataReader reader, CancellationToken token)
    {
        return reader switch
        {
            null => throw new ArgumentNullException(nameof(reader)),
            DbDataReader dataReader => dataReader.NextResultAsync(token),
            _ => DataReaderAdapters.GetOrAdd(reader.GetType(), type => new DataReaderAdapter(type))
                .NextResultAsyncToken(reader, token)
        };
    }
    public static Task<bool> ReadAsync(this IDataReader reader)
    {
        return reader switch
        {
            null => throw new ArgumentNullException(nameof(reader)),
            DbDataReader dataReader => dataReader.ReadAsync(),
            _ => DataReaderAdapters.GetOrAdd(reader.GetType(), type => new DataReaderAdapter(type)).ReadAsync(reader)
        };
    }
    public static Task<bool> ReadAsync(this IDataReader reader, CancellationToken token)
    {
        return reader switch
        {
            null => throw new ArgumentNullException(nameof(reader)),
            DbDataReader dataReader => dataReader.ReadAsync(token),
            _ => DataReaderAdapters.GetOrAdd(reader.GetType(), type => new DataReaderAdapter(type))
                .ReadAsyncToken(reader, token)
        };
    }
}