using MicroORM.Core;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MicroORM.Internal;
using MicroORM.Results;

namespace MicroORM.Commands
{

    public abstract class ModelCommand<TResult, TModel> : AdoCommand
          where TResult : AdoResult, new()
    {

        private const int _INTERVAL_BETWEEN_TRY_MILLISECONDS = 1000;
        private const int _TIMEOUT_COMMIT_MILLISECONDS = 30000;
        private const int _RETRY_COUNT = 2;

        internal ModelCommand(string customConnectionString, SQLDatabaseConnection existentConnection, Factory factory) : base(customConnectionString, existentConnection, factory)
        {
        }

        protected EmptyResult checkBlockerOfLockTimeoutException(string connectionString)
        {
            try
            {
                string sqlTextCommand = @"
        ;WITH SESSOES(SESSAO_BLOQUEADORA, LOGIN_APP, LAST_REQUEST_START_TIME, LAST_REQUEST_END_TIME, BLOQUEADORA, APP, APP_BLOQUEADO, CMD_BLOQUEADO) AS(
           SELECT SESSION_ID, '','', '', BLOCKING_SESSION_ID, '', '', ''

           FROM SYS.DM_EXEC_REQUESTS AS R JOIN SYS.SYSPROCESSES P ON P.SPID = R.SESSION_ID

           WHERE BLOCKING_SESSION_ID > 0

           UNION ALL

           SELECT SESSION_ID, S.LOGIN_NAME , S.LAST_REQUEST_START_TIME, S.LAST_REQUEST_END_TIME , CAST(0 AS SMALLINT), S.PROGRAM_NAME, '', ''
            FROM SYS.DM_EXEC_SESSIONS AS S
            WHERE EXISTS (
                SELECT* FROM SYS.DM_EXEC_REQUESTS AS R
                WHERE S.SESSION_ID = R.BLOCKING_SESSION_ID)
            AND NOT EXISTS(
                SELECT* FROM SYS.DM_EXEC_REQUESTS AS R
                WHERE S.SESSION_ID = R.SESSION_ID)
        ), 
        BLOQUEIOS AS(
            SELECT
                SESSAO_BLOQUEADORA, BLOQUEADORA, SESSAO_BLOQUEADORA AS REF, 1 AS NIVEL
            FROM SESSOES
            UNION ALL
            SELECT S.SESSAO_BLOQUEADORA, B.SESSAO_BLOQUEADORA, B.REF, NIVEL + 1
            FROM BLOQUEIOS AS B
            INNER JOIN SESSOES AS S ON B.SESSAO_BLOQUEADORA = S.BLOQUEADORA)

        --INSERT INTO DBDBA..TB_MON_LOCKS
        SELECT REF AS SESSAO_BLOQUEADORA, 
        (SELECT LOGIN_NAME+ '_' + HOST_NAME FROM SYS.DM_EXEC_SESSIONS
                WHERE SESSION_ID = B.REF) AS LOGIN_APP_HOSTNAME, --BLOQUEADOR
               (SELECT LAST_REQUEST_START_TIME FROM SYS.DM_EXEC_SESSIONS
                WHERE SESSION_ID = B.REF) AS LAST_REQUEST_START_TIME, --BLOQUEADOR
            COUNT(DISTINCT R.SESSION_ID) AS 'BLOQUEIOSDIRETOS', --BLOQUEADOR
            COUNT(DISTINCT B.SESSAO_BLOQUEADORA) - 1 AS BLOQUEIOSTOTAL, --BLOQUEADOR
            COUNT(DISTINCT B.SESSAO_BLOQUEADORA) - COUNT(DISTINCT R.SESSION_ID) - 1 AS BLOQUEIOSINDIRETOS, --BLOQUEADOR
            (SELECT TEXT FROM SYS.DM_EXEC_SQL_TEXT(
                (SELECT MOST_RECENT_SQL_HANDLE FROM SYS.DM_EXEC_CONNECTIONS
                WHERE SESSION_ID = B.REF))) AS COMANDO, --BLOQUEADOR
               (SELECT PROGRAM_NAME FROM SYS.DM_EXEC_SESSIONS
                WHERE SESSION_ID = B.REF) AS APP, --BLOQUEADOR
               P.SPID AS SESSAO_BLOQUEADA,
               P.PROGRAM_NAME AS APP_BLOQUEADO,
               S.TEXT AS CMD_BLOQUEADO,
               P.HOSTNAME AS HOSTNAME_BLOQUEADO
        FROM BLOQUEIOS AS B
            INNER JOIN SYS.DM_EXEC_REQUESTS AS R ON B.REF = R.BLOCKING_SESSION_ID
               INNER JOIN SYS.SYSPROCESSES AS P ON P.SPID = R.SESSION_ID
               CROSS APPLY SYS.DM_EXEC_SQL_TEXT (P.SQL_HANDLE ) AS S
        GROUP BY REF, R.WAIT_RESOURCE, P.PROGRAM_NAME, S.TEXT, P.HOSTNAME, P.SPID, DATEDIFF(SECOND, R.START_TIME, GETDATE()), R.START_TIME
         HAVING COUNT(DISTINCT R.SESSION_ID) = 1 ";

                var t = new Thread(new ParameterizedThreadStart((connStringObj) =>
                {
                    try
                    {
                        Task.Delay(2000);

                        using (var conn = new SQLDatabaseConnection(connStringObj?.ToString()))
                        {
                            using (var reader = conn.GetDataReader(sqlTextCommand, 3))
                            {
                                if (reader == null || reader.IsClosed) return;

                                var schema = reader.GetSchemaTable();
                                if (schema == null) return;

                                StringBuilder sb = new StringBuilder("* Locking details:");
                                sb.AppendLine();

                                bool lockerFound = false;

                                while (reader.Read())
                                {
                                    for (int ic = 0; ic < schema.Rows.Count; ic++)
                                    {
                                        string columnName = schema.Rows[ic]["ColumnName"]?.ToString();
                                        sb.Append($"{(ic == 0 ? "-" : " /")} {columnName.ToUpper()}: {reader[columnName]?.ToString()?.Trim() ?? "NULL"}");
                                    }
                                    sb.AppendLine();

                                    lockerFound = true;
                                }

                                if (lockerFound)
                                    _factory._logger.LogError(new Exception($"Lock Timeout Exception detected.{sb.ToString()}"));

                                conn.Close();
                            }
                        }

                    }
                    catch
                    {//empty catch}
                    }
                }));
                t.Start(connectionString);

                return new EmptyResult();
            }
            catch (Exception ex)
            {
                return new EmptyResult() { Exception = ex };
            }
        }

        protected abstract TResult executeInternal(SQLDatabaseConnection connection);

        protected bool _cancelRetry = false;
        protected bool _hasLockTimeoutExceptionOccurred = false;
        public TResult Execute(bool keepConnectionOpen = false, bool retryOnError = true)
        {
            try
            {
                var connection = getConnection();
                var originalConnectionState = connection.Connection.State;
                var hadTransaction = connection.HasTransaction();
                _hasLockTimeoutExceptionOccurred = false;

                int tryCount = 0;
                var startTime = DateTime.Now;
                Exception lastException = null;

                var currentCulture = System.Threading.Thread.CurrentThread.CurrentCulture;
                var currentUICulture = System.Threading.Thread.CurrentThread.CurrentUICulture;

                if (!retryOnError)
                    tryCount = _RETRY_COUNT - 1;

                while ((_RETRY_COUNT <= 0 || (_RETRY_COUNT + (_hasLockTimeoutExceptionOccurred ? 1 : 0)) >= ++tryCount) && ((DateTime.Now - startTime).TotalMilliseconds <= _TIMEOUT_COMMIT_MILLISECONDS + 10000) && (!_cancelRetry))
                {
                    try
                    {
                        System.Threading.Thread.CurrentThread.CurrentCulture = new System.Globalization.CultureInfo("en-US");
                        System.Threading.Thread.CurrentThread.CurrentUICulture = new System.Globalization.CultureInfo("en-US");

                        if (connection.Connection.State != ConnectionState.Open)
                            connection.Connection.Open();
                        //else if (!hadTransaction)

                        connection.SetTransactionIsolationLevelReadUncommitted();

                        return executeInternal(connection);
                    }
                    catch (Exception ex)
                    {
                        _hasLockTimeoutExceptionOccurred = _hasLockTimeoutExceptionOccurred || ex.IsLockRequestTimeoutException();

                        var thisChangeCommand = this as ChangeCommand<TModel>;

                        var beforeLastException = lastException?.InnerException ?? lastException;

                        if (thisChangeCommand == null)
                            lastException = ex;
                        else
                            lastException = new DatabaseAdoException(thisChangeCommand.ListCurrentChanges(), ex);

                        if (beforeLastException != null && lastException.Message != beforeLastException.Message)
                        {
                            lastException.Data.Add("BeforeLastException", beforeLastException);
                        }

                        System.Threading.Thread.Sleep(_INTERVAL_BETWEEN_TRY_MILLISECONDS);
                    }
                    finally
                    {
                        if ((!keepConnectionOpen || originalConnectionState != System.Data.ConnectionState.Open) && !hadTransaction)
                            connection.Dispose();

                        System.Threading.Thread.CurrentThread.CurrentCulture = currentCulture;
                        System.Threading.Thread.CurrentThread.CurrentUICulture = currentUICulture;
                    }
                }

                throw lastException;

            }
            catch (Exception ex)
            {
                var fullException = new Exception("An error has occurred in the communication with the database.", ex);
                _factory._logger.LogError(fullException);
                return new TResult() { Exception = fullException };
            }
        }

    }
}
