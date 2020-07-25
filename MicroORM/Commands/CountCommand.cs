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
    public class CountCommand<T> : ConditionalCommand<CountResult, T>
    {

        internal CountCommand(string customConnectionString, SQLDatabaseConnection existentConnection, Factory factory) : base(customConnectionString,existentConnection, factory) { }

        protected override int getTimeout()
        {
            return 10;
        }

        protected override CountResult executeInternal(SQLDatabaseConnection conn)
        {
            var tableName = typeof(T).Name;

            StringBuilder commandBuilder = new StringBuilder($"SELECT COUNT(*) FROM {tableName} {base.getWhereClause()} ");

            var sqlParameters = getWhereParameters();

            object commandResult = null;

            commandResult = conn.ExecuteScalarCommand(commandBuilder.ToString(), getTimeout(), sqlParameters);

            long count;
            if (!long.TryParse(commandResult?.ToString() ?? "0", out count)) count = 0;

            return new CountResult() { Result = count };
        }
    }
}
