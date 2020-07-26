using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using MicroORM.Core;
using MicroORM.Internal;
using MicroORM.Results;

namespace MicroORM.Commands
{
    public class ChangeCommand<T> : ModelCommand<ChangeResult, T>
    {

        private const int _BULK_MIN_COUNT = 1000;
        private const int _COMMANDS_COUNT_EXECUTE = 10;
        private const int _MULTIPLE_INSERT_VALUES_COUNT = 20;
        private const int _MAX_PARAMETERS_COUNT = 2000;// para evitar o seguinte erro do SQL Server: The incoming request has too many parameters. The server supports a maximum of 2100 parameters. Reduce the number of parameters and resend the request.

        protected Dictionary<object, Change<T>> _changes = new Dictionary<object, Change<T>>(100);
        PropertyDescriptorCollection _properties;
        PropertyDescriptor _pkProperty;
        private bool _saveLog = false;
        private PropertyDescriptor _logField = null;
        private int? _bulkMinCount = null;

        internal ChangeCommand(string customConnectionString, SQLDatabaseConnection existentConnection, Factory factory) : base(customConnectionString, existentConnection, factory)
        {
            _properties = TypeDescriptor.GetProperties(typeof(T));
            if (_properties == null || _properties.Count <= 0)
                throw new Exception("Invalid class type.");

            _pkProperty = _properties.Cast<PropertyDescriptor>().FirstOrDefault(p => (p.Attributes?.Cast<Attribute>()?.Any(a => a.GetType().Equals(typeof(System.ComponentModel.DataAnnotations.KeyAttribute))) ?? false));
            if (_pkProperty == null)
                throw new Exception("It is not possible to update a table without a defined primary key.");

            _saveLog = _factory._logger.IsModelSavingLog(typeof(T));

            string logFieldName = _factory._logger.GetModelLogField(typeof(T));
            if (string.IsNullOrWhiteSpace(logFieldName))
                logFieldName = _pkProperty.Name;

            _logField = _properties.Cast<PropertyDescriptor>().FirstOrDefault(p => p.Name.ToUpper() == logFieldName.ToUpper());

            if (_logField == null)
                _logField = _pkProperty;
        }

        public ChangeCommand<T> InsertOrUpdate(params T[] models)
        {
            return addChanges(ChangeType.InsertOrUpdate, models);
        }

        public ChangeCommand<T> Insert(params T[] models)
        {
            return addChanges(ChangeType.Insert, models);
        }

        public ChangeCommand<T> Update(params T[] models)
        {
            return addChanges(ChangeType.Update, models);
        }

        public ChangeCommand<T> Delete(params T[] models)
        {
            return addChanges(ChangeType.Delete, models);
        }


        public ChangeCommand<T> SetMinCountToBulk(int? bulkMinCount)
        {
            _bulkMinCount = bulkMinCount;
            return this;
        }

        private ChangeCommand<T> addChanges(ChangeType type, params T[] models)
        {
            if (models == null)
                throw new ArgumentNullException(nameof(models));

            for (int i = 0; i < models.Length; i++)
            {
                var currentModel = models[i];
                var currentPkValue = _pkProperty.GetValue(currentModel);
                if (currentPkValue == null)
                    throw new Exception("Empty primary key.");

                if (_changes.ContainsKey(currentPkValue))
                {
                    _changes[currentPkValue].Type = type;
                    _changes[currentPkValue].Model = currentModel;
                    _changes[currentPkValue].SpecificFields = null;
                }
                else
                {
                    _changes.Add(currentPkValue, new Change<T>(currentPkValue) { Type = type, Model = currentModel });
                }
            }

            return this;
        }

        public SpecificField<T> Set<TProperty>(Expression<Func<T, TProperty>> propertySelector, TProperty value)
        {
            return SpecificField<T>.Create(propertySelector, value);
        }

        public ChangeCommand<T> InsertOrUpdateSpecificFields(object pkValue, params SpecificField<T>[] specificFields)
        {
            return addChangesSpecificFields(ChangeType.InsertOrUpdate, pkValue, specificFields);
        }

        public ChangeCommand<T> InsertSpecificFields(object pkValue, params SpecificField<T>[] specificFields)
        {
            return addChangesSpecificFields(ChangeType.Insert, pkValue, specificFields);
        }

        public ChangeCommand<T> UpdateSpecificFields(object pkValue, params SpecificField<T>[] specificFields)
        {
            return addChangesSpecificFields(ChangeType.Update, pkValue, specificFields);
        }

        private ChangeCommand<T> addChangesSpecificFields(ChangeType type, object pkValue, params SpecificField<T>[] specificFields)
        {
            if (pkValue == null)
                throw new Exception("Empty primary key.");

            if (specificFields == null || specificFields.Length <= 0)
                throw new Exception("At least one field has to be changed.");

            if (type == ChangeType.Delete)
                throw new ArgumentException("Invalid ChangeType.", nameof(type));

            if (_changes.ContainsKey(pkValue))
            {
                _changes[pkValue].Type = type;
                _changes[pkValue].SpecificFields = specificFields;
                _changes[pkValue].Model = default(T);
            }
            else
            {
                _changes.Add(pkValue, new Change<T>(pkValue) { Type = type, SpecificFields = specificFields });
            }

            return this;
        }

        public ChangeCommand<T> DeleteByPk(params object[] pks)
        {
            if (pks == null)
                throw new Exception("Empty primary key.");

            if (pks == null || pks.Length <= 0)
                throw new Exception("At least one primary key has to be informed to be deleted.");

            for (int i = 0; i < pks.Length; i++)
            {
                var pkValue = pks[i];

                if (_changes.ContainsKey(pkValue))
                {
                    _changes[pkValue].Type = ChangeType.Delete;
                    _changes[pkValue].SpecificFields = null;
                    _changes[pkValue].Model = default(T);
                }
                else
                {
                    _changes.Add(pkValue, new Change<T>(pkValue) { Type = ChangeType.Delete });
                }
            }

            return this;
        }

        private bool isNew(string tableName, object pkValue)
        {
            string selectCommand = SqlCommandBuilder.GetExistsByPkCommand<T>(_pkProperty);
            var result = getConnection().ExecuteScalarCommand(selectCommand, 3, new Microsoft.Data.SqlClient.SqlParameter(_pkProperty.Name, pkValue) { SqlDbType = MicroORM.Core.SqlCommandBuilder.GetSqlFieldType(_pkProperty.PropertyType) });

            return result == null || !result.Equals(1);
        }

        public void ResetChanges()
        {
            _changes.Clear();
        }

        private int? _auxRowsCountToMultipleInsert;
        private int getRowsCountToMultipleInsert()
        {
            if (_auxRowsCountToMultipleInsert == null)
                _auxRowsCountToMultipleInsert = Math.Min(Math.Abs(_MAX_PARAMETERS_COUNT / _properties.Count), _MULTIPLE_INSERT_VALUES_COUNT);

            return _auxRowsCountToMultipleInsert.Value;
        }

        protected override ChangeResult executeInternal(SQLDatabaseConnection conn)
        {
            if (_changes == null || _changes.Count == 0)
                return new ChangeResult();

            var tableName = typeof(T).Name;

            var insertList = new List<Change<T>>();
            var updateList = new Dictionary<object, Change<T>>();
            var deleteList = new List<object>();
            var bulkList = new List<Change<T>>();

            bool useBulk = (_changes.Count >= (_bulkMinCount.HasValue && _bulkMinCount.Value >= 0 ? _bulkMinCount.Value : _BULK_MIN_COUNT));

            foreach (var currentChange in _changes)
            {
                if (currentChange.Value.Type == ChangeType.InsertOrUpdate)
                {
                    if (isNew(tableName, currentChange.Key))
                        currentChange.Value.Type = ChangeType.Insert;
                    else
                        currentChange.Value.Type = ChangeType.Update;
                }

                switch (currentChange.Value.Type)
                {
                    case ChangeType.Insert:
                        if (useBulk && (currentChange.Value.SpecificFields == null || currentChange.Value.SpecificFields.Length <= 0))
                            bulkList.Add(currentChange.Value);
                        else
                            insertList.Add(currentChange.Value);

                        break;
                    case ChangeType.Update:
                        if (useBulk && (currentChange.Value.SpecificFields == null || currentChange.Value.SpecificFields.Length <= 0))
                            bulkList.Add(currentChange.Value);
                        else
                            updateList.Add(currentChange.Key, currentChange.Value);

                        break;
                    case ChangeType.Delete:
                        if (useBulk)
                            bulkList.Add(currentChange.Value);
                        else
                            deleteList.Add(currentChange.Key);

                        break;
                }

            }

            var hadTransaction = conn.HasTransaction();

            int changedCount = 0;

            try
            {
                if (!hadTransaction)
                    conn.BeginTransaction();

                executeBulkCommands(bulkList, conn);

                List<ParameterCommand> commands = new List<ParameterCommand>(1000);

                if (insertList.Count > 0)
                {

                    string insertHeaderCommandText = SqlCommandBuilder.GetInsertHeaderCommand(typeof(T).Name, _properties) + " VALUES ";

                    int multipleInsertCommandCount = 0;
                    List<Tuple<string, object, Type>> parameters = new List<Tuple<string, object, Type>>(_MAX_PARAMETERS_COUNT);
                    StringBuilder multipleInsertCommandBuilder = new StringBuilder(insertHeaderCommandText);

                    for (int i = 0; i < insertList.Count; i++)
                    {
                        var change = insertList[i];
                        if (change.SpecificFields != null && change.SpecificFields.Length > 0)
                            commands.Add(SqlCommandBuilder.GetInsertCommand(change.PKValue, _pkProperty, _properties, change.SpecificFields));
                        else
                        {
                            if (multipleInsertCommandCount > 0)
                                multipleInsertCommandBuilder.Append(",");

                            parameters.AddRange(SqlCommandBuilder.GetInsertValuesCommand<T>(ref multipleInsertCommandBuilder, change.Model, _properties));

                            multipleInsertCommandCount++;

                            if (multipleInsertCommandCount >= getRowsCountToMultipleInsert())
                            {
                                commands.Add(new ParameterCommand(multipleInsertCommandBuilder.ToString(), parameters));
                                multipleInsertCommandBuilder.Clear();
                                multipleInsertCommandBuilder.Append(insertHeaderCommandText);
                                multipleInsertCommandCount = 0;
                                parameters = new List<Tuple<string, object, Type>>(_MAX_PARAMETERS_COUNT);
                            }
                        }
                    }

                    if (multipleInsertCommandCount > 0)
                    {
                        commands.Add(new ParameterCommand(multipleInsertCommandBuilder.ToString(), parameters));
                        multipleInsertCommandCount = 0;
                        parameters = new List<Tuple<string, object, Type>>(_MAX_PARAMETERS_COUNT);
                    }

                }

                if (updateList.Count > 0)
                {

                    foreach (var u in updateList)
                    {
                        var change = u.Value;
                        if (change.SpecificFields != null && change.SpecificFields.Length > 0)
                        {
                            var updateCommand = SqlCommandBuilder.GetUpdateCommand(_pkProperty, _properties, u.Key, change.SpecificFields);
                            if (updateCommand != null)
                                commands.Add(updateCommand);
                        }
                        else
                        {
                            var updateCommand = SqlCommandBuilder.GetUpdateCommand(change.Model, _pkProperty, _properties, u.Key);
                            if (updateCommand != null)
                                commands.Add(updateCommand);
                        }
                    }

                }

                if (deleteList.Count > 0)
                {

                    for (int i = 0; i < deleteList.Count; i++)
                    {
                        var pkValue = deleteList[i];
                        commands.Add(SqlCommandBuilder.GetDeleteCommand<T>(pkValue, _pkProperty));
                    }

                }

                changedCount = executeCommands(commands, conn);

                if (!hadTransaction)
                {
                    var oprCommit = conn.Commit();
                    if (!oprCommit.Success)
                        throw oprCommit.Exception;
                }

            }
            catch
            {
                if (!hadTransaction)
                    conn.Rollback();

                throw;
            }

            ResetChanges();

            return new ChangeResult()
            {
                InsertedCount = insertList.Count,
                DeletedCount = deleteList.Count,
                UpdatedCount = updateList.Count,
                AffectedCount = changedCount
            };

        }

        private class tempChangeTableDTO
        {
            public object PK { get; set; }
            public char ChangeType { get; set; }
        }

        private void executeBulkCommands(List<Change<T>> commands, SQLDatabaseConnection conn)
        {
            if (commands.Count <= 0)
                return;

            saveLog();

            var changes = (from c in commands
                           select new tempChangeTableDTO()
                           {
                               ChangeType = c.Type == ChangeType.Insert ? 'I' : c.Type == ChangeType.Update ? 'U' : c.Type == ChangeType.Delete ? 'D' : ' ',
                               PK = c.PKValue
                           }).ToArray();

            string changesTypeTempTableName = $"##tbChangesType_{MicroORM.Internal.Utils.GetUniqueId()}";
            string changesModelTempTableName = $"##tbChangesModel_{MicroORM.Internal.Utils.GetUniqueId()}";

            using (var changesTypeTempTableCommand = _factory.TemporaryTable<tempChangeTableDTO>().SetTableName(changesTypeTempTableName))
            {
                using (var changesModelTempTableCommand = _factory.TemporaryTable<T>(existentConnection: changesTypeTempTableCommand.CurrentConnection).SetTableName(changesModelTempTableName))
                {
                    var res = changesTypeTempTableCommand.Create();
                    if (!res.Success) throw res.Exception;

                    res = changesModelTempTableCommand.Create();
                    if (!res.Success) throw res.Exception;

                    res = changesTypeTempTableCommand.BulkInsert(changes);
                    if (!res.Success) throw res.Exception;

                    res = changesModelTempTableCommand.BulkInsert(commands.Select(c => c.Model).Where(c => c != null).ToArray());
                    if (!res.Success) throw res.Exception;

                    StringBuilder commandTextBuilder = new StringBuilder();

                    //insert
                    commandTextBuilder.AppendLine("--INSERTS:");
                    commandTextBuilder.AppendLine(SqlCommandBuilder.GetInsertHeaderCommand(typeof(T).Name, _properties));
                    commandTextBuilder.AppendLine(SqlCommandBuilder.GetSelectHeaderCommand(changesModelTempTableName, _properties) + " AS M ");
                    commandTextBuilder.AppendLine($" INNER JOIN [{changesTypeTempTableName}] AS C ON M.[{_pkProperty.Name}] = C.PK ");
                    commandTextBuilder.AppendLine($" WHERE C.ChangeType = 'I' ");

                    //update
                    commandTextBuilder.AppendLine("--UPDATES:");
                    commandTextBuilder.AppendLine($" UPDATE T SET ");
                    for (int i = 0; i < _properties.Count; i++)
                    {
                        var currentProperty = _properties[i];

                        if (i > 0)
                            commandTextBuilder.Append(", ");

                        commandTextBuilder.Append($"T.[{currentProperty.Name}] = M.[{currentProperty.Name}]");
                    }
                    commandTextBuilder.AppendLine($" FROM [{typeof(T).Name}] AS T ");
                    commandTextBuilder.AppendLine($" INNER JOIN [{changesModelTempTableName}] AS M ON T.[{_pkProperty.Name}] = M.[{_pkProperty.Name}] ");
                    commandTextBuilder.AppendLine($" INNER JOIN [{changesTypeTempTableName}] AS C ON M.[{_pkProperty.Name}] = C.PK ");
                    commandTextBuilder.AppendLine($" WHERE C.ChangeType = 'U' ");

                    //delete
                    commandTextBuilder.AppendLine("--DELETES:");
                    commandTextBuilder.AppendLine($" DELETE FROM [{typeof(T).Name}] WHERE [{_pkProperty.Name}] IN ( ");
                    commandTextBuilder.AppendLine($" SELECT C.PK FROM [{changesTypeTempTableName}] AS C ");
                    commandTextBuilder.AppendLine($" WHERE C.ChangeType = 'D') ");

                    var exeRes = _factory.PrepareSql(commandTextBuilder.ToString(), existentConnection: conn).ExecuteCommand(true);
                    if (!exeRes.Success)
                        throw exeRes.Exception;

                    res = changesTypeTempTableCommand.Drop();
                    res = changesModelTempTableCommand.Drop();
                }
            }

        }

        private int executeCommands(List<ParameterCommand> commands, SQLDatabaseConnection conn)
        {
            if (commands.Count <= 0)
                return 0;

            int changedCount = 0;

            saveLog();

            int executed = 0;

            while (executed < commands.Count)
            {
                var auxNextCommands = commands.Skip(executed).Take(_COMMANDS_COUNT_EXECUTE).ToList();
                List<ParameterCommand> nextCommands = new List<ParameterCommand>(_COMMANDS_COUNT_EXECUTE);
                int currentParametersCount = 0;

                foreach (var comm in auxNextCommands)
                {
                    if (comm.Parameters.Count + currentParametersCount > _MAX_PARAMETERS_COUNT)
                        if (nextCommands.Count <= 0)
                            throw new Exception($"It is not allowed to execute a command with more than {_MAX_PARAMETERS_COUNT} parameters.");
                        else
                            break;

                    currentParametersCount += comm.Parameters.Count;
                    nextCommands.Add(comm);
                }

                string nextCommandsSql = String.Join(Environment.NewLine, nextCommands.Select(c => c.SqlStatement));
                var nextParameters = (from c in nextCommands
                                      from p in c.Parameters
                                      select new Microsoft.Data.SqlClient.SqlParameter(p.Item1, SqlCommandBuilder.GetSqlRawValue(p.Item3, p.Item2)) { SqlDbType = MicroORM.Core.SqlCommandBuilder.GetSqlFieldType(p.Item3) }).ToArray();

                if (_hasLockTimeoutExceptionOccurred)
                    checkBlockerOfLockTimeoutException(conn.Connection?.ConnectionString);

                try
                {
                    changedCount += conn.ExecuteCommand(nextCommandsSql, 60, nextParameters);
                }
                catch (Exception ex)
                {
                    var fullException = new Exception("Something went wrong with in the execution of a database command.", ex);
                    fullException.Data.Add(nameof(nextCommandsSql), nextCommandsSql);
                    throw fullException;
                }

                executed += nextCommands.Count;
            }

            return changedCount;
        }

        public List<ChangeLogItemDTO> ListCurrentChanges()
        {
            if (_changes == null || !_changes.Any()) return new List<ChangeLogItemDTO>();

            var result = (from c in _changes
                          select new ChangeLogItemDTO()
                          {
                              State = c.Value.Type.ToString(),
                              Model = c.Value.Model,
                              TableName = typeof(T).Name,
                              PKName = _pkProperty.Name,
                              PKValue = c.Value.PKValue?.ToString()
                          }).ToList();

            return result;
        }

        private void saveLog()
        {
            if (!_saveLog || _logField == null || _changes == null || _changes.Count <= 0)
                return;

            foreach (var c in _changes)
            {
                _factory._logger.Log(Middleware.ELogType.Debug, $"{c.Value.Type.ToString()} On {typeof(T).Name}.{_logField.Name}? {(c.Value.Model == null ? c.Key : (_logField.GetValue(c.Value.Model) ?? c.Key)) ?? string.Empty}");
            }
        }

    }

}
