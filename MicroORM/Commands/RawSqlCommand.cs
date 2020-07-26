using MicroORM.Results;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MicroORM.Internal;

namespace MicroORM.Commands
{
    public class RawSqlCommand : AdoCommand
    {

        List<Microsoft.Data.SqlClient.SqlParameter> _parameters = new List<Microsoft.Data.SqlClient.SqlParameter>(10);
        string _sqlCommand;
        internal RawSqlCommand(string sqlCommand, string customConnectionString, SQLDatabaseConnection existentConnection, Factory factory) : base(customConnectionString, existentConnection, factory)
        {
            if (string.IsNullOrWhiteSpace(sqlCommand))
                throw new ArgumentNullException(nameof(sqlCommand));

            _sqlCommand = sqlCommand;
        }

        public RawSqlCommand AddParameters(Dictionary<string, object> parameters)
        {
            foreach (var p in parameters)
            {
                AddParameter(p.Key, p.Value);
            }

            return this;
        }

        public RawSqlCommand AddParameter(string name, object value)
        {
            var p = new Microsoft.Data.SqlClient.SqlParameter(name, value ?? DBNull.Value);
            if (value != null)
            {
                p.SqlDbType = MicroORM.Core.SqlCommandBuilder.GetSqlFieldType(value.GetType());

                if (p.SqlDbType == System.Data.SqlDbType.Char)
                    p.Size = Math.Max(1, Math.Min(6000, ((int?)value.ToString()?.Length) ?? 1));
            }

            _parameters.Add(p);

            return this;
        }

        public GenericResult<System.Data.IDataReader> ExecuteQueryAsDataReader()
        {
            var conn = getConnection();
            if (conn.Connection.State != System.Data.ConnectionState.Open)
                conn.Connection.Open();

            var result = executeInternal((connection) =>
            {
                var reader = connection.GetDataReader(_sqlCommand, base.getTimeout(), _parameters.ToArray());

                return reader;

            }, true);

            return result;
        }

        public GenericResult<System.Data.DataTable> ExecuteQueryAsDataTable(bool keepConnectionOpen = false)
        {
            var result = executeInternal((connection) =>
            {
                var oprDataTable = connection.FillDataTable(_sqlCommand, base.getTimeout(), _parameters.ToArray());

                if (!oprDataTable.Success)
                    throw oprDataTable.Exception;

                return oprDataTable.Result;
            }, keepConnectionOpen);

            return result;
        }

        public SelectResult<T> ExecuteQuery<T>(bool keepConnectionOpen = false)
            where T : new()
        {
            var result = executeInternal((connection) =>
            {
                var properties = TypeDescriptor.GetProperties(typeof(T));

                List<T> resultList = new List<T>();

                Dictionary<int, PropertyDescriptor> mappings = new Dictionary<int, PropertyDescriptor>();

                var reader = connection.GetDataReader(_sqlCommand, base.getTimeout(), _parameters.ToArray());
                while (reader.Read())
                {
                    T newItem = new T();

                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        PropertyDescriptor destinyProp = null;

                        if (mappings.ContainsKey(i))
                            destinyProp = mappings[i];
                        else
                        {
                            string fieldName = reader.GetName(i).ToUpper();

                            destinyProp = properties.Cast<PropertyDescriptor>().FirstOrDefault(p => p.Name.ToUpper() == fieldName);

                            mappings.Add(i, destinyProp);
                        }

                        if (destinyProp == null)
                            continue;

                        destinyProp.SetValue(newItem, getConvertedValue(reader[i], destinyProp.PropertyType));
                    }

                    resultList.Add(newItem);
                }

                reader.Close();

                return resultList;

            }, keepConnectionOpen);

            return new SelectResult<T>() { Exception = result.Exception, DataList = result.Result };
        }

        public EmptyResult ExecuteCommand(bool keepConnectionOpen = false)
        {
            var result = executeInternal<object>((connection) =>
            {
                connection.ExecuteCommand(_sqlCommand, base.getTimeout(), _parameters?.ToArray());

                return null;
            }, keepConnectionOpen);

            return new EmptyResult() { Exception = result.Exception };
        }

        public TemporaryTableResult ToTemporaryTable(string tableName = "")
        {
            try
            {
                if (String.IsNullOrWhiteSpace(_sqlCommand))
                    throw new ArgumentNullException(nameof(_sqlCommand));

                var firstSelectIndex = _sqlCommand.IndexOf("SELECT", StringComparison.OrdinalIgnoreCase);
                var fromIndex = _sqlCommand.IndexOf("FROM", firstSelectIndex, StringComparison.OrdinalIgnoreCase);

                if (firstSelectIndex < 0 || fromIndex < 0)
                    throw new Exception("Select/From statement not found.");

                if (_sqlCommand.Contains(" INTO ", StringComparison.OrdinalIgnoreCase))
                    throw new Exception("Invalid command for a temporary table creation.");

                if (String.IsNullOrWhiteSpace(tableName))
                    tableName = $"##temp_{MicroORM.Internal.Utils.GetUniqueId()}";
                else if (!tableName.StartsWith("#"))
                    tableName = tableName.Insert(0, "#");

                var selectIntoCommand = _sqlCommand.Insert(fromIndex, $" INTO {tableName} ");

                var genericResult = executeInternal<object>((connection) =>
                {
                    connection.ExecuteCommand(selectIntoCommand, base.getTimeout(), _parameters?.ToArray());

                    return null;
                }, true);

                var result = new TemporaryTableResult() { TableName = tableName, Exception = genericResult.Exception };

                return result;
            }
            catch (Exception ex)
            {
                var fullException = new Exception("An error has occurred while executing the command.", new Exception($"Error while creating the temporary table with the command: {((_sqlCommand ?? string.Empty))}", ex));
                _factory._logger.LogError(fullException);
                return new TemporaryTableResult() { Exception = fullException };
            }
        }

        private GenericResult<T> executeInternal<T>(Func<SQLDatabaseConnection, T> action, bool keepConnectionOpen = false)
        {
            try
            {
                if (_parameters != null)
                {
                    for (int i = 0; i < _parameters.Count; i++)
                    {
                        if (_parameters[i].SqlDbType.In(System.Data.SqlDbType.NVarChar, System.Data.SqlDbType.NChar, System.Data.SqlDbType.NText, System.Data.SqlDbType.Text))
                            _parameters[i].SqlDbType = System.Data.SqlDbType.Char;
                    }
                }

                SQLDatabaseConnection connection = base.getConnection();
                var originalConnectionState = connection.Connection.State;
                var hadTransaction = connection.HasTransaction();

                T result;

                try
                {
                    if (originalConnectionState != System.Data.ConnectionState.Open) connection.Connection.Open();

                    result = action(connection);
                }
                finally
                {
                    if ((!hadTransaction) && (!keepConnectionOpen || originalConnectionState != System.Data.ConnectionState.Open))
                        connection.Dispose();
                }

                return new GenericResult<T>() { Result = result };
            }
            catch (Exception ex)
            {
                var fullException = new Exception("An error has occurred while executing the command.", new Exception($"Error in the execution of the command: {(_sqlCommand ?? string.Empty)}", ex));
                _factory._logger.LogError(fullException);
                return new GenericResult<T>() { Exception = fullException };
            }
        }

    }
}
