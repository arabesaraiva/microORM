using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MicroORM.Internal;

namespace MicroORM.Core
{
    internal static class SqlCommandBuilder
    {
        public static SqlDbType GetSqlFieldType(Type clrFieldType)
        {
            if (clrFieldType.IsCharacterType())
                return SqlDbType.VarChar;
            else if (clrFieldType.IsBooleanType())
                return SqlDbType.Bit;
            else if (clrFieldType.IsDateTimeType())
                return SqlDbType.DateTime;
            else if (clrFieldType.IsLongType())
                return SqlDbType.BigInt;
            else if (clrFieldType.IsIntegerType())
                return SqlDbType.Int;
            else if (clrFieldType.IsDecimalType())
                return SqlDbType.Decimal;
            else if (clrFieldType.Equals(typeof(object)))
                return SqlDbType.Text;
            else
                return SqlDbType.Variant;
        }

        public static object GetSqlRawValue(Type propertyType, object value)
        {
            if (propertyType == typeof(string))
            {
                if (value == null || String.IsNullOrWhiteSpace(value?.ToString()))
                    return string.Empty;

                else return value.ToString();
            }

            if (propertyType.IsDateTimeType())
                return value == null ? DBNull.Value : (object)((DateTime)value);

            if (propertyType.IsBooleanType())
                return value == null ? false : ((bool)value);

            if (propertyType.IsDecimalType())
                return value == null ? 0 : ((decimal)value);

            if (propertyType.IsIntegerType())
                return value == null ? 0 : ((int)value);

            return value;
        }

        private static string getSqlValue(PropertyDescriptor property, object value)
        {
            if (property.PropertyType == typeof(string))
            {
                if (value == null || String.IsNullOrWhiteSpace(value?.ToString()))
                    return "''";

                string strValue = value.ToString();
                string strFinalValue = strValue.Replace("'", "''");
                return $"'{strFinalValue}'";
            }

            if (value == null)
                return "NULL";

            if (property.PropertyType.IsDateTimeType())
                return $"'{((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss")}'";

            if (property.PropertyType.IsBooleanType())
                return ((bool)value) ? "1" : "0";

            return value.ToString();
        }
        private static string getSqlValue<T>(PropertyDescriptor property, T instance)
        {
            var value = property.GetValue(instance);

            return getSqlValue(property, value);
        }

        private static void buildUpdateHeader<T>(ref StringBuilder builder)
        {
            builder.Append($" UPDATE [{typeof(T).Name}] SET ");
        }

        private static Tuple<string, object, Type> buildUpdateSetValue(ref StringBuilder builder, PropertyDescriptor currentProperty, object sqlValue, bool includeComma)
        {
            if (includeComma)
                builder.Append(", ");

            string paramName = $"@p_{MicroORM.Internal.Utils.GetUniqueId()}";

            builder.Append($"[{currentProperty.Name}] = {paramName}");

            return new Tuple<string, object, Type>(paramName, sqlValue, currentProperty.PropertyType);
        }

        private static void buildUpdateCondition(ref StringBuilder builder, PropertyDescriptor pkProperty, string sqlValue)
        {
            builder.Append($" WHERE [{pkProperty.Name}] = { sqlValue } ");
        }

        internal static List<Tuple<string, object, Type>> BuildUpdateSetStatement<T>(ref StringBuilder builder, SpecificField<T>[] specificFields, PropertyDescriptor pkProperty, PropertyDescriptorCollection properties)
        {
            buildUpdateHeader<T>(ref builder);

            int? pkIndex = null;

            var parameters = new List<Tuple<string, object, Type>>();

            for (int i = 0; i < specificFields.Length; i++)
            {
                var specificField = specificFields[i];
                var propInfo = specificField.GetProperty();

                var currentProperty = properties[propInfo.Name];
                if (currentProperty == pkProperty)
                {
                    pkIndex = i;
                    continue;
                }

                parameters.Add(buildUpdateSetValue(ref builder, currentProperty, specificField.GetValue(), (i > 1 || (i == 1 && pkIndex == null))));
            }

            return parameters;
        }

        internal static ParameterCommand GetUpdateCommand<T>(PropertyDescriptor pkProperty, PropertyDescriptorCollection properties, object pkValue, SpecificField<T>[] specificFields)
        {
            if (specificFields == null && specificFields.Length <= 0)
                throw new ArgumentNullException(nameof(specificFields));

            if (pkProperty == null)
                throw new ArgumentNullException(nameof(pkProperty));

            if (properties == null || properties.Count <= 1)
                return null;

            StringBuilder builder = new StringBuilder();

            var parameters = BuildUpdateSetStatement(ref builder, specificFields, pkProperty, properties);

            buildUpdateCondition(ref builder, pkProperty, getSqlValue(pkProperty, pkValue));

            return new ParameterCommand(builder.ToString(), parameters);
        }

        internal static ParameterCommand GetUpdateCommand<T>(T dbModel, PropertyDescriptor pkProperty, PropertyDescriptorCollection properties, object pkValue)
        {
            if (dbModel == null)
                throw new ArgumentNullException(nameof(dbModel));

            if (pkProperty == null)
                throw new ArgumentNullException(nameof(pkProperty));

            if (properties == null || properties.Count <= 1)
                return null;

            var parameters = new List<Tuple<string, object, Type>>();

            StringBuilder builder = new StringBuilder();
            buildUpdateHeader<T>(ref builder);

            for (int i = 0; i < properties.Count; i++)
            {
                var currentProperty = properties[i];
                if (currentProperty == pkProperty) continue;

                parameters.Add(buildUpdateSetValue(ref builder, currentProperty, currentProperty.GetValue(dbModel), (i > 1 || (i == 1 && pkProperty != properties[0]))));
            }

            buildUpdateCondition(ref builder, pkProperty, getSqlValue(pkProperty, pkValue));

            return new ParameterCommand(builder.ToString(), parameters);
        }

        internal static ParameterCommand GetInsertCommand<T>(object pkValue, PropertyDescriptor pkProperty, PropertyDescriptorCollection properties, SpecificField<T>[] specificFields, bool isPkIdentity)
        {
            if (specificFields == null && specificFields.Length <= 0)
                throw new ArgumentNullException(nameof(specificFields));

            if (properties == null || properties.Count <= 0)
                return null;

            List<Tuple<string, object, Type>> parameters = new List<Tuple<string, object, Type>>();

            StringBuilder builder = new StringBuilder($" INSERT INTO [{typeof(T).Name}] (");

            if (!isPkIdentity)
                builder.Append($"[{pkProperty.Name}]");

            for (int i = 0; i < specificFields.Length; i++)
            {
                var specificField = specificFields[i];
                var propInfo = specificField.GetProperty();

                var currentProperty = properties[propInfo.Name];

                if (currentProperty == pkProperty) continue;

                if (i > 0 || !isPkIdentity)
                    builder.Append(", ");

                builder.Append($"[{currentProperty.Name}]");
            }

            builder.Append($") VALUES (");

            if (!isPkIdentity)
                builder.Append(getSqlValue(pkProperty, pkValue));

            for (int i = 0; i < specificFields.Length; i++)
            {
                var specificField = specificFields[i];
                var propInfo = specificField.GetProperty();

                var currentProperty = properties[propInfo.Name];

                if (currentProperty == pkProperty) continue;

                string paramName = $"@p_{MicroORM.Internal.Utils.GetUniqueId()}";
                parameters.Add(new Tuple<string, object, Type>(paramName, specificField.GetValue(), currentProperty.PropertyType));

                if (i > 0 || !isPkIdentity)
                    builder.Append(", ");

                builder.Append($"{paramName}");
            }

            builder.Append(") ");

            return new ParameterCommand(builder.ToString(), parameters);
        }

        internal static string GetSelectHeaderCommand(string tableName, PropertyDescriptorCollection properties)
        {
            if (properties == null || properties.Count <= 0)
                return string.Empty;

            StringBuilder builder = new StringBuilder($" SELECT ");

            for (int i = 0; i < properties.Count; i++)
            {
                var currentProperty = properties[i];

                if (i > 0)
                    builder.Append(", ");

                builder.Append($"[{currentProperty.Name}]");
            }

            builder.Append($" FROM [{tableName}] ");

            return builder.ToString();
        }

        internal static string GetInsertHeaderCommand(string tableName, PropertyDescriptorCollection properties)
        {
            if (properties == null || properties.Count <= 0)
                return string.Empty;

            StringBuilder builder = new StringBuilder($" INSERT INTO [{ tableName}] (");

            for (int i = 0; i < properties.Count; i++)
            {
                var currentProperty = properties[i];

                if (i > 0)
                    builder.Append(", ");

                builder.Append($"[{currentProperty.Name}]");
            }

            builder.Append(") ");

            return builder.ToString();
        }

        internal static List<Tuple<string, object, Type>> GetInsertValuesCommand<T>(ref StringBuilder builder, T dbModel, PropertyDescriptorCollection properties)
        {
            if (dbModel == null)
                throw new ArgumentNullException(nameof(dbModel));

            if (properties == null || properties.Count <= 0)
                return null;

            var parameters = new List<Tuple<string, object, Type>>();

            builder.Append(" (");

            for (int i = 0; i < properties.Count; i++)
            {
                var currentProperty = properties[i];

                if (i > 0)
                    builder.Append(", ");

                string paramName = $"@p_{MicroORM.Internal.Utils.GetUniqueId()}";

                builder.Append(paramName);

                parameters.Add(new Tuple<string, object, Type>(paramName, currentProperty.GetValue(dbModel), currentProperty.PropertyType));
            }

            builder.Append(") ");

            return parameters;
        }

        internal static ParameterCommand GetDeleteCommand<T>(object pkValue, PropertyDescriptor pkProperty)
        {
            if (pkValue == null || String.IsNullOrWhiteSpace(pkValue.ToString()))
                throw new ArgumentNullException(nameof(pkValue), "It is not possible to execute a DELETE command without a PK.");

            if (pkProperty == null)
                throw new ArgumentNullException(nameof(pkProperty));

            string command = $" DELETE FROM [{typeof(T).Name}] WHERE [{pkProperty.Name}] = {getSqlValue(pkProperty, pkValue)} ";

            return new ParameterCommand(command, null);
        }

        internal static string GetExistsByPkCommand<T>(PropertyDescriptor pkProperty)
        {
            if (pkProperty == null)
                throw new ArgumentNullException(nameof(pkProperty));

            string command = $" SELECT TOP 1 1 FROM [{typeof(T).Name}] WHERE [{pkProperty.Name}] = @{pkProperty.Name} ";

            return command;
        }

    }
}
