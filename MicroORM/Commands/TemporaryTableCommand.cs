using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MicroORM.Results;
using MicroORM.Internal;

namespace MicroORM.Commands
{
    public class TemporaryTableCommand<T> : AdoCommand
    {

        private string _tableName;
        PropertyDescriptorCollection _properties;
        internal TemporaryTableCommand(string customConnectionString, SQLDatabaseConnection existentConnection, Factory factory) : base(customConnectionString, existentConnection, factory)
        {
            _properties = TypeDescriptor.GetProperties(typeof(T));
            if (_properties == null || _properties.Count <= 0)
                throw new Exception("Invalid class type.");
        }

        public string TableName
        {
            get
            {
                return _tableName;
            }
        }

        public TemporaryTableCommand<T> SetTableName(string tableName)
        {
            if (!String.IsNullOrWhiteSpace(tableName))
            {
                tableName = tableName.Trim();
                if (!tableName.StartsWith("#"))
                    tableName = $"#{tableName}";
            }

            _tableName = tableName;

            return this;
        }

        public TemporaryTableResult Create(int timeout = 10)
        {
            var result = executeInternal((tableName, conn) =>
            {
                string columnsStatement = String.Join(", ", (from p in _properties.Cast<PropertyDescriptor>()
                                                             select $"[{p.Name}] {getSqlFieldTypeCreateTable(p.PropertyType)} NULL"));

                string createTableStatement = $" CREATE TABLE {tableName} ({columnsStatement}) ";

                conn.ExecuteCommand(createTableStatement, timeout);

                return new TemporaryTableResult() { TableName = tableName };
            });

            return result;
        }

        public TemporaryTableResult BulkInsert(params T[] collection)
        {
            if (collection == null || collection.Length <= 0)
                return new TemporaryTableResult() { TableName = _tableName };

            var result = executeInternal((tableName, conn) =>
            {
                var bulkResult = _factory.Bulk<T>(base._customConnectionString, conn).SetTableName(TableName).SetIsTempTable(true).Insert(collection).Execute(true, false);

                if (bulkResult.Success)
                    return new TemporaryTableResult() { TableName = TableName };
                else
                    return new TemporaryTableResult() { Exception = bulkResult.Exception };
            });

            return result;
        }

        public TemporaryTableResult Drop()
        {
            var result = executeInternal((tableName, conn) =>
            {
                string dropTableStatement = $" IF NOT OBJECT_ID('TEMPDB..{tableName}') IS NULL DROP TABLE {tableName} ";

                conn.ExecuteCommand(dropTableStatement, 10);

                return new TemporaryTableResult() { TableName = tableName };
            });

            return result;
        }


        private string getSqlFieldTypeCreateTable(Type type)
        {
            if (type.IsIntegerType())
                return "[int] default 0";
            else if (type.IsLongType())
                return "[bigint] default 0";
            else if (type.IsDecimalType())
                return "[numeric](20, 3) default 0";
            else if (type.IsDateTimeType())
                return "[datetime]";
            else if (type.IsBooleanType())
                return "[bit] default 0";
            else
                return "[varchar](max) COLLATE SQL_Latin1_General_CP1_CI_AS default ''";
        }

        private TemporaryTableResult executeInternal(Func<string, SQLDatabaseConnection, TemporaryTableResult> action)
        {
            try
            {
                var connection = getConnection();
                var originalConnectionState = connection.Connection.State;
                var hadTransaction = connection.HasTransaction();

                if (originalConnectionState != ConnectionState.Open)
                    connection.Connection.Open();
                else if (!hadTransaction)
                    connection.SetTransactionIsolationLevelReadUncommitted();

                string tableName = this._tableName;
                if (String.IsNullOrWhiteSpace(tableName))
                    tableName = $"##temp_{MicroORM.Internal.Utils.GetUniqueId()}";

                if (!tableName.StartsWith("#"))
                    throw new Exception("The temporary table's name must starts with #.");

                var result = action(tableName, connection);

                if (result.Success && !String.IsNullOrWhiteSpace(result.TableName))
                    _tableName = result.TableName;

                return result;
            }
            catch (Exception ex)
            {
                var fullException = new Exception("There was an error in database communication.", ex);
                _factory._logger.Log(Middleware.ELogType.Error, fullException.Message, fullException);
                return new TemporaryTableResult() { Exception = fullException };
            }
        }

    }
}
