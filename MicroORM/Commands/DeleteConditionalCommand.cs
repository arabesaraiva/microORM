using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MicroORM.Core;
using MicroORM.Internal;
using MicroORM.Results;

namespace MicroORM.Commands
{
    public class DeleteConditionalCommand<T> : ConditionalCommand<ChangeResult, T>
    {

        PropertyDescriptorCollection _properties;
        PropertyDescriptor _pkProperty;

        internal DeleteConditionalCommand(string customConnectionString, SQLDatabaseConnection existentConnection, Factory factory) : base(customConnectionString, existentConnection, factory)
        {
            _properties = TypeDescriptor.GetProperties(typeof(T));
            if (_properties == null || _properties.Count <= 0)
                throw new Exception("Invalid class type.");

            _pkProperty = _properties.Cast<PropertyDescriptor>().FirstOrDefault(p => (p.Attributes?.Cast<Attribute>()?.Any(a => a.GetType().Equals(typeof(System.ComponentModel.DataAnnotations.KeyAttribute))) ?? false));
            if (_pkProperty == null)
                throw new Exception("It is not possible to update a table without a defined primary key.");
        }

        protected override ChangeResult executeInternal(SQLDatabaseConnection conn)
        {
            if (_conditions == null || !_conditions.Any())
                return new ChangeResult() { Exception = new Exception("Command without conditions.") };

            var tableName = typeof(T).Name;

            StringBuilder commandBuilder = new StringBuilder($"DELETE FROM [{tableName}] WHERE [{_pkProperty.Name}] IN (SELECT [{_pkProperty.Name}] FROM [{tableName}] {base.getWhereClause()}) ");

            var sqlParameters = getWhereParameters();

            int commandResult = conn.ExecuteCommand(commandBuilder.ToString(), getTimeout(), sqlParameters);
            return new ChangeResult() { DeletedCount = commandResult, AffectedCount = commandResult };
        }

    }
}
