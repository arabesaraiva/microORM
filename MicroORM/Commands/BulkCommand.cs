using MicroORM.Core;
using MicroORM.Results;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MicroORM.Internal;

namespace MicroORM.Commands
{
    public class BulkCommand<T> : ModelCommand<ChangeResult, T>
    {

        DataTable _insertDataTable;

        PropertyDescriptorCollection _properties;
        PropertyDescriptor _pkProperty;
        internal BulkCommand(string customConnectionString, SQLDatabaseConnection existentConnection, Factory factory) : base(customConnectionString, existentConnection, factory)
        {
            _properties = TypeDescriptor.GetProperties(typeof(T));
            if (_properties == null || _properties.Count <= 0)
                throw new Exception("Invalid class type.");

            _pkProperty = _properties.Cast<PropertyDescriptor>().FirstOrDefault(p => (p.Attributes?.Cast<Attribute>()?.Any(a => a.GetType().Equals(typeof(System.ComponentModel.DataAnnotations.KeyAttribute))) ?? false));

        }

        private string _customTableName;
        private string getTableName()
        {
            if (!String.IsNullOrWhiteSpace(_customTableName))
                return _customTableName;

            return typeof(T).Name;
        }

        internal BulkCommand<T> SetTableName(string tableName)
        {
            _customTableName = tableName;
            return this;
        }

        private bool _isTempTable = false;
        internal BulkCommand<T> SetIsTempTable(bool value)
        {
            _isTempTable = value;
            return this;
        }

        private void buildInsertDataTable()
        {
            _insertDataTable = new DataTable(getTableName()) { CaseSensitive = false, MinimumCapacity = 1000, RemotingFormat = SerializationFormat.Binary };

            for (int i = 0; i < _properties.Count; i++)
            {
                var prop = _properties[i];

                var dataColumn = new DataColumn(prop.Name, prop.PropertyType.IsEnum ? typeof(string) : prop.PropertyType.GetNonNullableType());
                dataColumn.AllowDBNull = _isTempTable || prop.PropertyType.IsDateTimeType();

                _insertDataTable.Columns.Add(dataColumn);

                if (_pkProperty != null && prop == _pkProperty)
                    _insertDataTable.PrimaryKey = new DataColumn[1] { dataColumn };
            }
        }

        public BulkCommand<T> Insert(params T[] models)
        {
            if (models == null)
                throw new ArgumentNullException(nameof(models));

            if (_insertDataTable == null)
                buildInsertDataTable();

            _insertDataTable.BeginLoadData();

            for (int i = 0; i < models.Length; i++)
            {
                var newItem = models[i];

                var newRow = _insertDataTable.NewRow();

                for (int j = 0; j < _properties.Count; j++)
                {
                    var property = _properties[j];

                    var value = property.GetValue(newItem);

                    newRow.SetField(property.Name, value ?? DBNull.Value);
                }

                _insertDataTable.Rows.Add(newRow);
            }

            _insertDataTable.EndLoadData();

            return this;
        }

        protected override ChangeResult executeInternal(SQLDatabaseConnection conn)
        {
            int insertedCount = _insertDataTable.Rows.Count;

            var sqlConn = conn.Connection as SqlConnection;
            var sqlTran = conn.GetTransaction() as SqlTransaction;

            int batchSize = _properties.Count > 64 ? 5000 : 10000;
            
            var bulkCopy = sqlTran != null ? new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.Default, sqlTran) : new Microsoft.Data.SqlClient.SqlBulkCopy(sqlConn);

            for (int i = 0; i < _properties.Count; i++)
            {
                var prop = _properties[i];

                bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(prop.Name, prop.Name));
            }

            bulkCopy.DestinationTableName = getTableName();
            bulkCopy.BulkCopyTimeout = 120;

            int currentRow = 0;
            while (currentRow < insertedCount)
            {
                var batchRows = _insertDataTable.Rows.Cast<DataRow>().Skip(currentRow).Take(batchSize).ToArray();

                bulkCopy.WriteToServer(batchRows);
                _cancelRetry = true;

                currentRow += batchSize;
            }

            bulkCopy.Close();

            _insertDataTable.Clear();

            return new ChangeResult() { InsertedCount = insertedCount };
        }

    }
}
