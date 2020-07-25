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
    public class UpdateConditionalCommand<T> : ConditionalCommand<ChangeResult, T>
    {

        PropertyDescriptorCollection _properties;
        PropertyDescriptor _pkProperty;
        Dictionary<string, SpecificField<T>> _specificFields = new Dictionary<string, SpecificField<T>>();

        internal UpdateConditionalCommand(string customConnectionString, SQLDatabaseConnection existentConnection, Factory factory) : base(customConnectionString, existentConnection, factory)
        {
            _properties = TypeDescriptor.GetProperties(typeof(T));
            if (_properties == null || _properties.Count <= 0)
                throw new Exception("Invalid class type.");

            _pkProperty = _properties.Cast<PropertyDescriptor>().FirstOrDefault(p => (p.Attributes?.Cast<Attribute>()?.Any(a => a.GetType().Equals(typeof(System.ComponentModel.DataAnnotations.KeyAttribute))) ?? false));
            if (_pkProperty == null)
                throw new Exception("It is not possible to update a table without a defined primary key.");
        }

        public UpdateConditionalCommand<T> Set<TProperty>(Expression<Func<T, TProperty>> propertySelector, TProperty value)
        {
            var specificField = SpecificField<T>.Create(propertySelector, value);

            if (_specificFields.ContainsKey(specificField.GetProperty().Name))
                _specificFields[specificField.GetProperty().Name] = specificField;
            else
                _specificFields.Add(specificField.GetProperty().Name, specificField);

            return this;
        }

        public void ResetChanges()
        {
            _specificFields.Clear();
        }

        protected override ChangeResult executeInternal(SQLDatabaseConnection conn)
        {
            if (_conditions == null || !_conditions.Any())
                return new ChangeResult() { Exception = new Exception("Command without conditions.") };

            if (_specificFields == null || _specificFields.Count <= 0)
                return new ChangeResult() { Exception = new Exception("Command without set's.") };

            var tableName = typeof(T).Name;


            StringBuilder builder = new StringBuilder();

            var parameters = SqlCommandBuilder.BuildUpdateSetStatement<T>(ref builder, (from f in _specificFields select f.Value).ToArray(), _pkProperty, _properties);

            builder.Append($" WHERE [{_pkProperty.Name}] IN (SELECT [{_pkProperty.Name}] FROM [{tableName}] {base.getWhereClause()}) ");

            var sqlParameters = getWhereParameters() ?? new Microsoft.Data.SqlClient.SqlParameter[0];

            var additionalParameters = parameters.Select(p => new Microsoft.Data.SqlClient.SqlParameter(p.Item1, SqlCommandBuilder.GetSqlRawValue(p.Item3, p.Item2)) { SqlDbType = MicroORM.Core.SqlCommandBuilder.GetSqlFieldType(p.Item3) }).ToList();

            if (additionalParameters.Count > 0)
                sqlParameters = sqlParameters.Concat(additionalParameters).ToArray();

            object commandResult = null;

            commandResult = conn.ExecuteScalarCommand(builder.ToString(), getTimeout(), sqlParameters);

            ResetChanges();

            return new ChangeResult() { UpdatedCount = 1 };
        }

    }
}
