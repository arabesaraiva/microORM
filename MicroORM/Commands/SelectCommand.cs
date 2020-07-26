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
    public class SelectCommand<T, TResult> : ConditionalCommand<SelectResult<TResult>, T>
    {

        PropertyDescriptorCollection _sourceProperties;
        PropertyDescriptor[] _destinyProperties;
        private List<System.Reflection.PropertyInfo> _customSourcePropertyInfos = new List<System.Reflection.PropertyInfo>();
        private PageRequest _pageRequest;

        private Func<T, TResult> _targetTypeInitializer;
        internal SelectCommand(string customConnectionString, Expression<Func<T, TResult>> targetTypeInitializer, SQLDatabaseConnection existentConnection, Factory factory) : base(customConnectionString, existentConnection, factory)
        {
            _destinyProperties = TypeDescriptor.GetProperties(typeof(TResult)).Cast<PropertyDescriptor>().Where(p => !p.IsReadOnly && (p.Attributes == null || p.Attributes[typeof(System.ComponentModel.DataAnnotations.Schema.NotMappedAttribute)] == null)).ToArray();

            if (targetTypeInitializer != null)
            {
                _targetTypeInitializer = targetTypeInitializer.Compile();

                if (!typeof(TResult).Equals(typeof(T)))
                {
                    fillCustomSourcePropertyInfos(targetTypeInitializer);

                    _sourceProperties = TypeDescriptor.GetProperties(typeof(T));
                    if (!_customSourcePropertyInfos.Any() && _destinyProperties.Cast<PropertyDescriptor>().Any(p => !_sourceProperties.Cast<PropertyDescriptor>().Any(s => s.Name.ToLower() == p.Name.ToLower())))
                        throw new Exception("All model's properties have to be the same names as the database columns names.");
                }
            }


        }

        private void fillCustomSourcePropertyInfos(Expression<Func<T, TResult>> targetTypeInitializer)
        {
            if (targetTypeInitializer?.Body == null || !(targetTypeInitializer.Body is MemberInitExpression))
                return;

            var memberInit = (MemberInitExpression)targetTypeInitializer.Body;

            if (memberInit.Bindings == null || !memberInit.Bindings.Any())
                return;

            var bindings = memberInit.Bindings.OfType<MemberAssignment>().ToArray();

            for (int i = 0; i < bindings.Length; i++)
            {
                var b = bindings[i];

                var memberExpression = b.Expression as MemberExpression;
                if (memberExpression == null)
                {
                    var unaryExpression = b.Expression as UnaryExpression;
                    if (unaryExpression != null)
                        memberExpression = unaryExpression.Operand as MemberExpression;
                }

                if (memberExpression != null)
                {
                    var member = memberExpression.Member as System.Reflection.PropertyInfo;
                    if (member != null)
                        _customSourcePropertyInfos.Add(member);
                }
                else if (b.Expression is ConditionalExpression)
                {
                    var testExpression = ((ConditionalExpression)b.Expression).Test;
                    if (testExpression is BinaryExpression)
                    {
                        var member = (((BinaryExpression)testExpression).Left as MemberExpression)?.Member as System.Reflection.PropertyInfo;
                        if (member != null)
                            _customSourcePropertyInfos.Add(member);
                    }
                }
            }
        }

        private Expression<Func<T, object>>[] _orderByFields;
        private int? _topCount = null;
        public SelectCommand<T, TResult> Top(int topCount, params Expression<Func<T, dynamic>>[] orderByFields)
        {
            if (topCount < 0)
                _topCount = null;

            _topCount = topCount;
            _orderByFields = orderByFields;

            return this;
        }

        private string _selectFieldCommand;
        private string getSelectFieldsCommand()
        {
            if (String.IsNullOrWhiteSpace(_selectFieldCommand))
            {
                if (_destinyProperties == null || _destinyProperties.Length <= 0)
                    throw new Exception("Invalid type.");

                string[] selectColumnsNames;

                if (_customSourcePropertyInfos != null && _customSourcePropertyInfos.Any())
                    selectColumnsNames = (from p in _customSourcePropertyInfos select p.Name).ToArray();
                else
                    selectColumnsNames = (from p in _destinyProperties.Cast<PropertyDescriptor>() select p.Name).ToArray();

                if (_ignoreColumnsNames != null && _ignoreColumnsNames.Length > 0)
                    selectColumnsNames = selectColumnsNames.Where(f => !_ignoreColumnsNames.Any(i => i?.Trim().ToUpper() == f.Trim().ToUpper())).ToArray();

                if (selectColumnsNames.Length <= 0)
                    throw new Exception("No fields to select.");

                _selectFieldCommand = String.Join(", ", selectColumnsNames);

            }

            return _selectFieldCommand;
        }

        public SelectCommand<T, TResult> ClearPaging()
        {
            this._pageRequest = null;

            return this;
        }

        public SelectCommand<T, TResult> Page(int skip, int take, params Expression<Func<T, dynamic>>[] orderByFields)
        {
            if (take <= 0)
                throw new ArgumentException("The records count must be greather than 0.", nameof(take));

            if (orderByFields == null || orderByFields.Length <= 0)
                throw new ArgumentNullException(nameof(orderByFields), "There must be at least one field to order by.");

            List<string> orderByFieldList = new List<string>(orderByFields.Length);
            foreach (var e in orderByFields)
            {
                string fieldName = ((e.Body as MemberExpression) ?? ((e.Body as UnaryExpression)?.Operand as MemberExpression))?.Member.Name;
                if (!String.IsNullOrWhiteSpace(fieldName))
                    orderByFieldList.Add(fieldName);
            }

            if (orderByFieldList == null || orderByFieldList.Count <= 0)
                throw new ArgumentNullException(nameof(orderByFields), "There must be at least one field to order by.");

            _pageRequest = new PageRequest() { Skip = Math.Max(0, skip), Take = take, OrderByFields = orderByFieldList.ToArray() };

            return this;
        }

        private string _temporaryTableName = "";

        public SelectCommand<T, TResult> ToTemporaryTable(string tempTableName)
        {
            if (String.IsNullOrWhiteSpace(tempTableName))
                tempTableName = $"##temp_{MicroORM.Internal.Utils.GetUniqueId()}";
            else if (!tempTableName.StartsWith("#"))
                tempTableName = tempTableName.Insert(0, "#");

            _temporaryTableName = tempTableName;

            return this;
        }

        private string[] _ignoreColumnsNames;
        public SelectCommand<T, TResult> IgnoreColumns(params string[] columnsNames)
        {
            if (columnsNames == null)
            {
                _ignoreColumnsNames = new string[0];
                return this;
            }

            _ignoreColumnsNames = columnsNames;

            return this;
        }

        public SelectCommand<T, TResult> IgnoreColumns(params Expression<Func<T, dynamic>>[] columnsNames)
        {
            if (columnsNames == null)
            {
                _ignoreColumnsNames = new string[0];
                return this;
            }

            List<string> ignoreFieldList = new List<string>(columnsNames.Length);
            foreach (var e in columnsNames)
            {
                string fieldName = ((e.Body as MemberExpression) ?? ((e.Body as UnaryExpression)?.Operand as MemberExpression))?.Member.Name;
                if (!String.IsNullOrWhiteSpace(fieldName))
                    ignoreFieldList.Add(fieldName);
            }

            return IgnoreColumns(ignoreFieldList.ToArray());
        }

        protected override SelectResult<TResult> executeInternal(SQLDatabaseConnection conn)
        {
            var tableName = typeof(T).Name;

            bool isPaging = _pageRequest != null && _pageRequest.OrderByFields.Length > 0 && _pageRequest.Take > 0;

            StringBuilder commandBuilder = new StringBuilder($" SELECT");

            if (isPaging)
                commandBuilder.Append($" ROW_NUMBER() OVER ( ORDER BY { String.Join(", ", _pageRequest.OrderByFields)} ) AS [RowNum], ");
            else if (_topCount.HasValue && _topCount.Value >= 0)
                commandBuilder.Append($" TOP {_topCount.Value.ToString()}");

            commandBuilder.Append($" {getSelectFieldsCommand()} ");

            if (!isPaging && !String.IsNullOrWhiteSpace(_temporaryTableName))
                commandBuilder.Append($" INTO {_temporaryTableName} ");

            commandBuilder.Append($" FROM {tableName} {base.getWhereClause()} ");

            if (!isPaging && _topCount.HasValue && _topCount.Value > 0 && _orderByFields != null && _orderByFields.Length > 0)
            {
                commandBuilder.Append(" ORDER BY ");

                for (int i = 0; i < _orderByFields.Length; i++)
                {
                    string fieldName = ((_orderByFields[i].Body as MemberExpression) ?? ((_orderByFields[i].Body as UnaryExpression)?.Operand as MemberExpression))?.Member.Name;
                    if (String.IsNullOrWhiteSpace(fieldName))
                        break;

                    if (i > 0)
                        commandBuilder.Append(", ");

                    commandBuilder.Append(fieldName);
                }

            }

            string commandText = commandBuilder.ToString();

            if (isPaging)
                commandText = $" SELECT * {(String.IsNullOrWhiteSpace(_temporaryTableName) ? string.Empty : $"INTO {_temporaryTableName}")} FROM ( {commandText} ) OT1 WHERE OT1.[RowNum] > {_pageRequest.Skip} AND OT1.[RowNum] <= {_pageRequest.Take + _pageRequest.Skip} ORDER BY OT1.[RowNum] ";

            var sqlParameters = getWhereParameters();

            if (!String.IsNullOrWhiteSpace(_temporaryTableName))
            {
                conn.ExecuteCommand(commandText, base.getTimeout(), sqlParameters);
                return new SelectResult<TResult>() { DataList = null };
            }

            List<TResult> resultList = new List<TResult>();

            var reader = conn.GetDataReader(commandText, base.getTimeout(), sqlParameters);
            while (reader.Read())
            {
                TResult newItem;

                if (_targetTypeInitializer == null || typeof(TResult).Equals(typeof(T)))
                {
                    newItem = Activator.CreateInstance<TResult>();

                    for (int i = 0; i < _destinyProperties.Length; i++)
                    {
                        var destinyProp = _destinyProperties[i];
                        if (_ignoreColumnsNames != null && _ignoreColumnsNames.Any(c => c?.Trim().ToUpper() == destinyProp.Name.Trim().ToUpper()))
                            continue;

                        destinyProp.SetValue(newItem, getConvertedValue(reader[destinyProp.Name], destinyProp.PropertyType));
                    }

                    resultList.Add(newItem);
                }
                else
                {
                    var sourceItem = Activator.CreateInstance<T>();

                    if (_customSourcePropertyInfos.Any())
                    {
                        for (int i = 0; i < _customSourcePropertyInfos.Count; i++)
                        {
                            var sourceProp = _customSourcePropertyInfos[i];
                            if (_ignoreColumnsNames != null && _ignoreColumnsNames.Any(c => c?.Trim().ToUpper() == sourceProp.Name.Trim().ToUpper()))
                                continue;

                            sourceProp.SetValue(sourceItem, getConvertedValue(reader[sourceProp.Name], sourceProp.PropertyType));
                        }
                    }
                    else
                    {
                        for (int i = 0; i < _destinyProperties.Length; i++)
                        {
                            var destinyProp = _destinyProperties[i];
                            var sourceProp = _sourceProperties[destinyProp.Name] ?? _sourceProperties.Cast<PropertyDescriptor>().FirstOrDefault(p => p.Name.ToLower() == destinyProp.Name.ToLower());

                            if (_ignoreColumnsNames != null && _ignoreColumnsNames.Any(c => c?.Trim().ToUpper() == sourceProp.Name.Trim().ToUpper()))
                                continue;

                            sourceProp.SetValue(sourceItem, getConvertedValue(reader[sourceProp.Name], sourceProp.PropertyType));
                        }
                    }

                    newItem = _targetTypeInitializer(sourceItem);

                    resultList.Add(newItem);
                }

            }
            reader.Close();

            return new SelectResult<TResult>() { DataList = resultList };

        }

        public static explicit operator CountCommand<T>(SelectCommand<T, TResult> selectCommand)
        {
            var countCommand = new CountCommand<T>(customConnectionString: selectCommand._customConnectionString, existentConnection: selectCommand.getConnection(), factory: selectCommand._factory);

            if (selectCommand._conditions != null)
            {
                for (int i = 0; i < selectCommand._conditions.Count; i++)
                {
                    var condition = selectCommand._conditions[i];
                    countCommand.where(condition);
                }
            }

            return countCommand;
        }
    }
}
