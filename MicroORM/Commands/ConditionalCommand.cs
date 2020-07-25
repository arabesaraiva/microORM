using MicroORM.Core;
using MicroORM.Internal;
using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace MicroORM.Commands
{
    public abstract class ConditionalCommand<TResult, TModel> : ModelCommand<TResult, TModel>
        where TResult : AdoResult, new()
    {

        protected List<Condition> _conditions = new List<Condition>(10);

        internal ConditionalCommand(string customConnectionString, SQLDatabaseConnection existentConnection, Factory factory) : base(customConnectionString, existentConnection, factory) { }

        public ConditionalCommand<TResult, TModel> WhereIn<TProperty>(Expression<Func<TModel, TProperty>> propertySelector, TProperty[] inValues, ConditionType type = ConditionType.Equals)
        {          
            PropertyInfo propertyInfo = null;

            var unaryExpression = propertySelector.Body as UnaryExpression;
            if (unaryExpression != null)
            {
                var unaryMemberExpression = unaryExpression.Operand as MemberExpression;
                propertyInfo = unaryMemberExpression?.Member as PropertyInfo;
            }
            else
            {
                var memberExpression = propertySelector.Body as MemberExpression;
                propertyInfo = memberExpression?.Member as PropertyInfo;
            }

            if (String.IsNullOrWhiteSpace(propertyInfo?.Name))
                throw new Exception("Invalid expression.");

            _conditions.RemoveAll(c => c.FieldName.Trim().ToLower() == propertyInfo.Name.Trim().ToLower() && c.ConditionType == ConditionType.Equals);

            var newCondition = new Condition() { ConditionType = type, FieldName = propertyInfo.Name, FieldType = propertyInfo.PropertyType, EmptyInValues= inValues == null || inValues.Length <= 0, InValues = inValues?.Cast<object>()?.ToArray() };

            return where(newCondition);
        }

        public ConditionalCommand<TResult, TModel> Where<TProperty>(Expression<Func<TModel, TProperty>> propertySelector, TProperty value, ConditionType type = ConditionType.Equals)
        {
            return Where<TProperty>(propertySelector, value, value, type: type);
        }

        public ConditionalCommand<TResult, TModel> Where<TProperty>(Expression<Func<TModel, TProperty>> propertySelector, TProperty minValue, TProperty maxValue, ConditionType type = ConditionType.Equals)
        {
            PropertyInfo propertyInfo = null;

            var unaryExpression = propertySelector.Body as UnaryExpression;
            if (unaryExpression != null)
            {
                var unaryMemberExpression = unaryExpression.Operand as MemberExpression;
                propertyInfo = unaryMemberExpression?.Member as PropertyInfo;
            }
            else
            {
                var memberExpression = propertySelector.Body as MemberExpression;
                propertyInfo = memberExpression?.Member as PropertyInfo;
            }

            if (String.IsNullOrWhiteSpace(propertyInfo?.Name))
                throw new Exception("Invalid expression");

            _conditions.RemoveAll(c => c.FieldName.Trim().ToLower() == propertyInfo.Name.Trim().ToLower() && c.ConditionType == ConditionType.Equals);

            var newCondition = new Condition() { ConditionType = type, FieldName = propertyInfo.Name, FieldType = propertyInfo.PropertyType, MinValue = (object)minValue ?? DBNull.Value, MaxValue = (object)maxValue ?? DBNull.Value };

            return where(newCondition);
        }

        protected internal ConditionalCommand<TResult, TModel> where(Condition newCondition)
        {
            _conditions.Add(newCondition);

            return this;
        }

        public ConditionalCommand<TResult, TModel> RemoveCondition<TProperty>(Expression<Func<TModel, TProperty>> propertySelector)
        {
            var memberExpression = propertySelector.Body as MemberExpression;
            if (String.IsNullOrWhiteSpace(memberExpression?.Member?.Name))
                throw new Exception("Invalid expression");

            var propertyInfo = (PropertyInfo)memberExpression.Member;

            _conditions.RemoveAll(c => c.FieldName.Trim().ToLower() == propertyInfo.Name.Trim().ToLower());

            return this;
        }

        protected string getWhereClause()
        {
            StringBuilder commandBuilder = new StringBuilder($" WHERE 1=1 ");

            if (_conditions != null && _conditions.Any())
            {
                foreach (var c in _conditions)
                {
                    commandBuilder.Append(" AND ");

                    if (c.ConditionType == ConditionType.Not)
                        commandBuilder.Append("NOT ");

                    if (c.InValues != null && c.InValues.Length > 0)
                    {
                        commandBuilder.Append(" (");

                        var op = c.ConditionType == ConditionType.Like ? "LIKE" : "=";

                        for (int i = 0; i < c.InValues.Length; i++)
                        {
                            if (i > 0)
                                commandBuilder.Append(" OR ");

                            commandBuilder.Append($" ({c.FieldName} {op} @{c.FieldName}_{i}) ");
                        }

                        commandBuilder.Append(") ");
                    }
                    else if (c.EmptyInValues)                    
                        commandBuilder.Append(" (0 = 1) ");
                    
                    else if (Equals(c.MinValue, c.MaxValue))
                    {
                        var op = c.ConditionType == ConditionType.Like ? "LIKE" : "=";
                        if (c.MinValue == null || c.MinValue == DBNull.Value)
                            commandBuilder.Append($" ({c.FieldName} IS NULL) ");
                        else
                            commandBuilder.Append($" ({c.FieldName} {op} @{c.FieldName}) ");
                    }
                    else
                        commandBuilder.Append($" ({c.FieldName} >= @min_{c.FieldName} AND {c.FieldName} <= @max_{c.FieldName}) ");
                }
            }

            return commandBuilder.ToString();
        }

        protected SqlParameter[] getWhereParameters()
        {
            if (_conditions == null || !_conditions.Any())
                return new SqlParameter[0];

            var parameters = new List<SqlParameter>();

            for (int i = 0; i < _conditions.Count; i++)
            {
                var c = _conditions[i];

                if (c.InValues != null && c.InValues.Length > 0)
                {
                    for (int j = 0; j < c.InValues.Length; j++)
                    {
                        var p = new SqlParameter($"{c.FieldName}_{j}", c.InValues[j]) { SqlDbType = MicroORM.Core.SqlCommandBuilder.GetSqlFieldType(c.FieldType) };
                        if (p.SqlDbType == System.Data.SqlDbType.Char)
                            p.Size = Math.Max(1, Math.Min(6000, ((int?)c.InValues.Select(v => v?.ToString()?.Length ?? 1)?.Max()) ?? 1));

                        parameters.Add(p);
                    }
                }
                else if (c.EmptyInValues)
                    continue;

                else if (Equals(c.MinValue, c.MaxValue))
                {
                    var p = new SqlParameter(c.FieldName, c.MinValue) { SqlDbType = MicroORM.Core.SqlCommandBuilder.GetSqlFieldType(c.FieldType) };
                    if (p.SqlDbType == System.Data.SqlDbType.Char)
                        p.Size = Math.Max(1, Math.Min(6000, ((int?)c.MinValue?.ToString()?.Length) ?? 1));

                    parameters.Add(p);
                }
                else
                {
                    var p = new SqlParameter($"min_{c.FieldName}", c.MinValue) { SqlDbType = MicroORM.Core.SqlCommandBuilder.GetSqlFieldType(c.FieldType) };
                    if (p.SqlDbType == System.Data.SqlDbType.Char)
                        p.Size = Math.Max(1, Math.Min(6000, ((int?)c.MinValue?.ToString()?.Length) ?? 1));

                    parameters.Add(p);

                    p = new SqlParameter($"max_{c.FieldName}", c.MaxValue) { SqlDbType = MicroORM.Core.SqlCommandBuilder.GetSqlFieldType(c.FieldType) };
                    if (p.SqlDbType == System.Data.SqlDbType.Char)
                        p.Size = Math.Max(1, Math.Min(6000, ((int?)c.MaxValue?.ToString()?.Length) ?? 1));

                    parameters.Add(p);
                }
            }

            return parameters.ToArray();
        }

    }
}
