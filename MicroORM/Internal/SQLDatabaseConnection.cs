using MicroORM.Middleware;
using MicroORM.Results;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;

namespace MicroORM.Internal
{
    public class SQLDatabaseConnection : IDisposable
    {
        internal SqlConnection Connection { get; private set; }

        internal SQLDatabaseConnection(SQLDatabaseOptions options) : this(options.ConnectionString)
        {
        }

        private void createConnection()
        {
            Connection = new SqlConnection(_connectionString);
            Connection.StateChange += (sender, e) =>
            {
                if (e.CurrentState == ConnectionState.Open)
                    SetTransactionIsolationLevelReadUncommitted();
            };
        }

        private string _connectionString;
        internal SQLDatabaseConnection(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException(nameof(connectionString), "No connection string has been setted");

            _connectionString = connectionString;

            createConnection();
        }

        private SqlTransaction _transaction;
        internal SqlTransaction GetTransaction()
        {
            return _transaction;
        }

        internal bool HasTransaction()
        {
            return _transaction != null;
        }

        private SqlCommand createCommand(string sqlCommand, int timeout, params SqlParameter[] parameters)
        {
            var command = Connection.CreateCommand();
            command.CommandTimeout = timeout;
            command.CommandText = sqlCommand;

            if (parameters != null && parameters.Length > 0)
                foreach (var p in parameters)
                {
                    command.Parameters.Add(p);
                }

            if (HasTransaction())
                command.Transaction = GetTransaction();

            return command;
        }

        internal int ExecuteCommand(string sqlCommand, int timeout, params SqlParameter[] parameters)
        {
            using (var command = createCommand(sqlCommand, timeout, parameters))
            {
                var oldStateConnection = Connection.State;

                try
                {
                    if (oldStateConnection == ConnectionState.Closed)
                        Connection.Open();

                    return command.ExecuteNonQuery();
                }
                finally
                {
                    if (oldStateConnection == ConnectionState.Closed)
                        Connection.Close();
                }
            }
        }

        internal void SetTransactionIsolationLevelReadUncommitted()
        {
            ExecuteCommand("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED", 5);
        }

        internal object ExecuteScalarCommand(string sqlCommand, int timeout, params SqlParameter[] parameters)
        {
            using (var command = createCommand(sqlCommand, timeout, parameters))
            {
                var oldStateConnection = Connection.State;

                try
                {
                    if (oldStateConnection == ConnectionState.Closed)
                        Connection.Open();

                    return command.ExecuteScalar();
                }
                finally
                {
                    if (oldStateConnection == ConnectionState.Closed)
                        Connection.Close();
                }
            }
        }

        public Microsoft.Data.SqlClient.SqlTransaction BeginTransaction()
        {
            if (Connection.State != ConnectionState.Open) Connection.Open();

            if (_transaction == null)
                _transaction = Connection.BeginTransaction();

            return _transaction;
        }

        internal GenericResult<System.Data.DataTable> FillDataTable(string sqlCommand, int timeout, SqlParameter[] parameters)
        {
            try
            {
                using (var adapter = new SqlDataAdapter(createCommand(sqlCommand, timeout, parameters)))
                {
                    var table = new DataTable();
                    adapter.Fill(table);

                    return new GenericResult<DataTable>() { Result = table };
                }
            }
            catch (Exception ex)
            {
                return new GenericResult<DataTable>() { Exception = ex };
            }
        }

        public EmptyResult Commit()
        {
            try
            {
                if (_transaction != null)
                {
                    _transaction.Commit();
                    _transaction = null;
                }

                return new EmptyResult();
            }
            catch (Exception ex)
            {
                return new EmptyResult() { Exception = ex };
            }
        }

        public EmptyResult Rollback()
        {
            try
            {
                if (_transaction != null && _transaction.Connection != null)
                {
                    _transaction.Rollback();
                    _transaction.Dispose();
                    _transaction = null;
                }

                return new EmptyResult();
            }
            catch (Exception ex)
            {
                return new EmptyResult() { Exception = ex };
            }
        }

        internal IDataReader GetDataReader(string sqlCommand, int timeout, params SqlParameter[] parameters)
        {
            if (Connection.State != ConnectionState.Open) Connection.Open();

            return createCommand(sqlCommand, timeout, parameters).ExecuteReader();
        }

        internal void Close()
        {
            if (Connection.State != ConnectionState.Closed)
            {
                Connection.Close();
                Connection.Dispose();
            }

            createConnection();
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {

                    Rollback();

                    if (Connection != null)
                    {
                        if (Connection.State != ConnectionState.Closed) Connection.Close();
                        Connection.Dispose();
                    }

                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~SQLDatabaseConnection()
        // {
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
