using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace Yinyang.Utilities.SqlServer
{
    public class SqlServer : IDisposable
    {
        private readonly string _connectionString;
        private readonly SqlConnection _sqlConnection;
        private SqlCommand _sqlCommand;
        private SqlTransaction _sqlTransaction;

        /// <summary>
        /// Gets or sets the string used to open a SQL Server database.
        /// </summary>
        public static string ConnectionString { get; set; }

        /// <summary>
        ///     CommandText
        /// </summary>
        public string CommandText
        {
            get => _sqlCommand.CommandText;
            set => _sqlCommand.CommandText = value;
        }

        /// <summary>
        ///     Constructor (Use ConnectionString property)
        /// </summary>
        public SqlServer()
        {
            if (string.IsNullOrEmpty(ConnectionString))
            {
                throw new NoNullAllowedException("ConnectionString Null or Empty");
            }

            _connectionString = ConnectionString;

            _sqlConnection = new SqlConnection(_connectionString);

            _sqlCommand = new SqlCommand { Connection = _sqlConnection };
        }

        /// <summary>
        ///     Constructor
        /// </summary>
        /// <param name="connectionString">The connection string that includes the source database name, and other parameters needed to establish the initial connection. The default value is an empty string.</param>
        public SqlServer(string connectionString)
        {
            _connectionString = connectionString;

            _sqlConnection = new SqlConnection(_connectionString);

            _sqlCommand = new SqlCommand { Connection = _sqlConnection };
        }

        /// <summary>
        ///     Dispose
        /// </summary>
        public void Dispose()
        {
            try
            {
                Close();
            }
            catch
            {
                // ignored
            }

            _sqlTransaction?.Dispose();
            _sqlConnection?.Dispose();
            _sqlCommand?.Dispose();
        }

        /// <summary>
        ///     Copy
        /// </summary>
        /// <returns></returns>
        public SqlServer Copy()
        {
            return new SqlServer(_connectionString);
        }

        /// <summary>
        ///     Opens a database connection with the property settings specified by the ConnectionString.
        /// </summary>
        public void Open()
        {
            _sqlConnection.Open();
        }

        /// <summary>
        ///     Closes the connection to the database. This is the preferred method of closing any open connection.
        /// </summary>
        public void Close()
        {
            _sqlCommand.Dispose();
            _sqlConnection.Close();
        }

        /// <summary>
        ///     Change command type
        /// </summary>
        /// <param name="type"></param>
        public void ChangeCommandType(CommandType type)
        {
            _sqlCommand.CommandType = type;
        }

        /// <summary>
        ///     AddParameter
        /// </summary>
        /// <param name="key"></param>
        /// <param name="obj"></param>
        public void AddParameter(string key, object obj)
        {
            _sqlCommand.Parameters.Add(obj == null
                ? new SqlParameter(key, DBNull.Value)
                : new SqlParameter(key, obj));
        }

        /// <summary>
        ///     AddWithValue
        /// </summary>
        /// <param name="key"></param>
        /// <param name="obj"></param>
        public void AddWithValue(string key, object obj)
        {
            _sqlCommand.Parameters.AddWithValue(key, obj);
        }

        /// <summary>
        ///     Clear parameters
        /// </summary>
        public void ClearParameter()
        {
            _sqlCommand.CommandType = CommandType.Text;
            _sqlCommand.Parameters.Clear();
        }

        /// <summary>
        ///     Regenerate SqlCommand
        /// </summary>
        public void Refresh()
        {
            _sqlCommand.Dispose();
            _sqlCommand = null;
            _sqlCommand = new SqlCommand { Connection = _sqlConnection };
            if (_sqlTransaction != null)
            {
                _sqlCommand.Transaction = _sqlTransaction;
            }
        }

        /// <summary>
        ///     ExecuteNonQuery
        /// </summary>
        /// <returns></returns>
        public int ExecuteNonQuery()
        {
            return _sqlCommand.ExecuteNonQuery();
        }

        /// <summary>
        ///     ExecuteScalar
        /// </summary>
        /// <returns></returns>
        public object ExecuteScalar()
        {
            return _sqlCommand.ExecuteScalar();
        }

        /// <summary>
        ///     ExecuteScalar
        /// </summary>
        /// <returns></returns>
        public int ExecuteScalarToInt()
        {
            var r = _sqlCommand.ExecuteScalar();
            if (r != null)
            {
                return int.Parse(r.ToString());
            }

            return -1;
        }


        /// <summary>
        /// EasySelect (Auto Open and Close)
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <param name="sql">SQL Query (SELECT)</param>
        /// <returns></returns>
        public List<T> EasySelect<T>(string sql) where T : new()
        {
            if (string.IsNullOrEmpty(ConnectionString))
            {
                throw new NoNullAllowedException("ConnectionString Null or Empty");
            }

            Open();
            CommandText = sql;
            var result = ExecuteReader<T>();
            Close();
            return result;
        }

        /// <summary>
        /// EasySelect (Auto Open and Close. used TryExecuteReader)
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <param name="sql">SQL Query (SELECT)</param>
        /// <returns></returns>
        public List<T> EasySelectTry<T>(string sql) where T : new()
        {
            if (string.IsNullOrEmpty(ConnectionString))
            {
                throw new NoNullAllowedException("ConnectionString Null or Empty");
            }

            Open();
            CommandText = sql;
            var result = TryExecuteReader<T>();
            Close();
            return result;
        }

        /// <summary>
        /// ExecuteReaderFirst
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <returns>First Record</returns>
        public T ExecuteReaderFirst<T>() where T : new()
        {
            T t = default;
            using (var reader = _sqlCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    t = new T();

                    for (var inc = 0; inc < reader.FieldCount; inc++)
                    {
                        var type = t.GetType();
                        var prop = type.GetProperty(reader.GetName(inc));
                        if (prop != null)
                        {
                            var val = reader.GetValue(inc);
                            if (val == DBNull.Value)
                            {
                                continue;
                            }
                            prop.SetValue(t, val, null);
                        }
                    }
                    break;
                }
                reader.Close();
            }
            return t;
        }

        /// <summary>
        ///     ExecuteReaderFirst (ignore error)
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <returns>First Record</returns>
        public T TryExecuteFirst<T>() where T : new()
        {
            T t = default;

            using (var reader = _sqlCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    t = new T();

                    for (var inc = 0; inc < reader.FieldCount; inc++)
                    {
                        var type = t.GetType();
                        var prop = type.GetProperty(reader.GetName(inc));
                        if (prop != null)
                        {
                            try
                            {
                                var val = reader.GetValue(inc);
                                if (val == DBNull.Value)
                                {
                                    continue;
                                }
                                prop.SetValue(t, val, null);
                                prop.SetValue(t, reader.GetValue(inc), null);
                            }
                            catch
                            {
                                // ignored
                            }
                        }
                    }
                    break;
                }
                reader.Close();
            }
            return t;
        }

        /// <summary>
        ///     ExecuteReader To List
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public List<T> ExecuteReader<T>() where T : new()
        {
            var res = new List<T>();
            using (var reader = _sqlCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    var t = new T();

                    for (var inc = 0; inc < reader.FieldCount; inc++)
                    {
                        var type = t.GetType();
                        var prop = type.GetProperty(reader.GetName(inc));
                        if (prop != null)
                        {
                            var val = reader.GetValue(inc);
                            if (val == DBNull.Value)
                            {
                                continue;
                            }
                            prop.SetValue(t, val, null);
                        }
                    }
                    res.Add(t);
                }
                reader.Close();
            }
            return res;
        }

        /// <summary>
        ///     ExecuteReader To List (ignore error)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public List<T> TryExecuteReader<T>() where T : new()
        {
            var res = new List<T>();
            using (var reader = _sqlCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    var t = new T();

                    for (var inc = 0; inc < reader.FieldCount; inc++)
                    {
                        var type = t.GetType();
                        var prop = type.GetProperty(reader.GetName(inc));
                        if (prop != null)
                        {
                            try
                            {
                                var val = reader.GetValue(inc);
                                if (val == DBNull.Value)
                                {
                                    continue;
                                }
                                prop.SetValue(t, val, null);
                                prop.SetValue(t, reader.GetValue(inc), null);
                            }
                            catch
                            {
                                // ignored
                            }
                        }
                    }
                    res.Add(t);
                }
                reader.Close();
            }

            return res;
        }

        /// <summary>
        ///     SQL IN
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="partialClause"></param>
        /// <param name="paramPrefix"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public string BuildWhereInClause<T>(string partialClause, string paramPrefix, IEnumerable<T> parameters)
        {
            var parameterNames = parameters.Select(
                (paramText, paramNumber) => "@" + paramPrefix + paramNumber)
              .ToArray();

            var inClause = string.Join(",", parameterNames);
            var whereInClause = string.Format(partialClause.Trim(), inClause);

            return whereInClause;
        }

        /// <summary>
        ///     SQL Parameter IN
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="paramPrefix"></param>
        /// <param name="parameters"></param>
        public void AddParamsToCommand<T>(string paramPrefix, IEnumerable<T> parameters)
        {
            var parameterValues = parameters.Select(paramText => paramText.ToString()).ToArray();

            var parameterNames = parameterValues.Select(
              (paramText, paramNumber) => "@" + paramPrefix + paramNumber
            ).ToArray();

            for (var i = 0; i < parameterNames.Length; i++)
            {
                _sqlCommand.Parameters.AddWithValue(parameterNames[i], parameterValues[i]);
            }
        }

        /// <summary>
        ///     Table Rows Count
        /// </summary>
        /// <param name="tableName">Table Name</param>
        /// <returns></returns>
        public int TableRowsCount(string tableName)
        {
            ClearParameter();
            CommandText = $"SELECT COUNT(*) FROM {tableName};";
            var r = ExecuteScalarToInt();
            return r;
        }

        #region Transaction

        /// <summary>
        ///     BeginTransaction
        /// </summary>
        public void BeginTransaction()
        {
            _sqlTransaction = _sqlConnection.BeginTransaction();
            _sqlCommand.Transaction = _sqlTransaction;
        }

        /// <summary>
        ///     BeginTransaction
        /// </summary>
        /// <param name="iso"></param>
        public void BeginTransaction(IsolationLevel iso)
        {
            _sqlTransaction = _sqlConnection.BeginTransaction(iso);
            _sqlCommand.Transaction = _sqlTransaction;
        }

        /// <summary>
        ///     Commit
        /// </summary>
        public void Commit()
        {
            _sqlTransaction.Commit();
            TransactionClean();
        }

        /// <summary>
        ///     Rollback
        /// </summary>
        public void Rollback()
        {
            _sqlTransaction.Rollback();
            TransactionClean();
        }

        /// <summary>
        ///     Transaction Reset
        /// </summary>
        private void TransactionClean()
        {
            _sqlTransaction.Dispose();
            _sqlTransaction = null;
            _sqlCommand.Transaction = null;
        }

        #endregion
    }
}
