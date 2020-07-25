using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using MicroORM.Core;
using MicroORM.Internal;
using MicroORM.Results;

namespace MicroORM.Commands
{
    public class ExistsCommand<T> : ConditionalCommand<ExistsResult, T>
    {

        internal ExistsCommand(string customConnectionString, SQLDatabaseConnection existentConnection, Factory factory) : base(customConnectionString,existentConnection, factory) { }

        protected override int getTimeout()
        {
            return 10;
        }

        protected override ExistsResult executeInternal(SQLDatabaseConnection conn)
        {
            var tableName = typeof(T).Name;

            StringBuilder commandBuilder = new StringBuilder($"SELECT TOP 1 1 FROM {tableName} {base.getWhereClause()} ");

            var sqlParameters = getWhereParameters();

            object commandResult = null;
            
            commandResult = conn.ExecuteScalarCommand(commandBuilder.ToString(), getTimeout(), sqlParameters);

            return new ExistsResult() { Result = commandResult != null && commandResult.Equals(1) };
        }
    }
}
