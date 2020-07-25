using MicroORM.Results;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MicroORM.Internal;

namespace MicroORM.Commands
{
    public class AdoCommand : IDisposable
    {

        protected string _customConnectionString;
        private SQLDatabaseConnection _connection;
        private bool _hadTransaction = false;
        protected Factory _factory;

        public AdoCommand(string customConnectionString, SQLDatabaseConnection existentConnection, Factory factory)
        {
            _customConnectionString = customConnectionString;
            _connection = existentConnection;
            _factory = factory;

            _hadTransaction = existentConnection != null && existentConnection.Connection.State == ConnectionState.Open && existentConnection.HasTransaction();
        }

        public SQLDatabaseConnection CurrentConnection
        {
            get
            {
                return _connection;
            }
        }

        public GenericResult<DateTime> GetServerDateTime()
        {
            var connection = getConnection();

            var originalConnectionState = connection.Connection.State;
            var hadTransaction = connection.HasTransaction();

            try
            {
                if (originalConnectionState != ConnectionState.Open)
                    connection.Connection.Open();

                var result = _factory.PrepareSql("SELECT GetDate() AS CurrentDateTime", existentConnection: connection).ExecuteQueryAsDataReader();
                if (!result.Success)
                    return new GenericResult<DateTime>() { Exception = result.Exception };

                var reader = result.Result;

                DateTime dataHoraServidor;

                if (reader == null || !reader.Read() || reader[0] == null || reader[0] == DBNull.Value || !DateTime.TryParse(reader[0].ToString(), out dataHoraServidor))
                    return new GenericResult<DateTime>() { Exception = new Exception("It wasn't possible to obtain the current database time.") };

                reader.Close();

                dataHoraServidor = dataHoraServidor.AddTicks(-(dataHoraServidor.Ticks % TimeSpan.TicksPerSecond));

                return new GenericResult<DateTime>() { Result = dataHoraServidor };
            }
            catch (Exception ex)
            {
                var fullException = new Exception("An error has occurred while getting the current database time.", ex);
                _factory._logger.LogError(fullException);

                return new GenericResult<DateTime>() { Exception = fullException };
            }
            finally
            {
                if (originalConnectionState != System.Data.ConnectionState.Open && !hadTransaction)
                    connection.Dispose();
            }
        }

        public GenericResult<bool> IsConnectionAvailable()
        {
            var result = GetServerDateTime();
            return new GenericResult<bool>() { Result = result.Success };
        }


        public GenericResult<List<string>> GetTablesNames()
        {
            var connection = getConnection();

            var originalConnectionState = connection.Connection.State;
            var hadTransaction = connection.HasTransaction();

            try
            {
                if (originalConnectionState != ConnectionState.Open)
                    connection.Connection.Open();

                var result = _factory.PrepareSql("SELECT table_name FROM INFORMATION_SCHEMA.TABLES", existentConnection: connection).ExecuteQueryAsDataReader();
                if (!result.Success)
                    return new GenericResult<List<string>>() { Exception = result.Exception };

                var reader = result.Result;

                List<string> tables = new List<string>();

                if (reader != null)
                {
                    while (reader.Read())
                    {
                        var tableName = reader[0];
                        if (tableName != null && tableName != DBNull.Value && !String.IsNullOrWhiteSpace(tableName.ToString()))
                            tables.Add(tableName.ToString());
                    }
                }

                return new GenericResult<List<string>>() { Result = tables };
            }
            catch (Exception ex)
            {
                var fullException = new Exception("An error has occurred while getting the tables' name from the database.", ex);
                _factory._logger.LogError(fullException);
                return new GenericResult<List<string>>() { Exception = fullException };
            }
            finally
            {
                if (originalConnectionState != System.Data.ConnectionState.Open && !hadTransaction)
                    connection.Dispose();
            }
        }

        protected SQLDatabaseConnection getConnection()
        {
            if (_connection == null)
                _connection = _factory.GetNewConnection(_customConnectionString);

            return _connection;
        }

        protected void disposeConnection()
        {
            if (_connection != null && !_hadTransaction)
                _connection.Dispose();

            _connection = null;
        }

        protected virtual int getTimeout()
        {
            return 0;
        }

        protected object getConvertedValue(object rawValue, Type targetType)
        {
            if (rawValue == null || rawValue == DBNull.Value)
                return targetType.IsValueType ? Activator.CreateInstance(targetType) : null;

            var convertedValue = Convert.ChangeType(rawValue, targetType.FullName.StartsWith("System.Nullable") && targetType.GenericTypeArguments != null && targetType.GenericTypeArguments.Length == 1 ? targetType.GenericTypeArguments[0] : targetType);
            return convertedValue;
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                    if (_connection != null && !_hadTransaction)
                        _connection.Dispose();
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.
                _connection = null;

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~AdoCommand() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion

    }
}
