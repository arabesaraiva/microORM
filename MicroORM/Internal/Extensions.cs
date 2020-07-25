using MicroORM.Middleware;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MicroORM.Internal
{
    public static class Extensions
    {

        private static bool isType(Type type, params Type[] validTypes)
        {
            return type != null && validTypes != null && validTypes.Length > 0 && (validTypes).Contains(type);
        }

        public static bool IsIntegerType(this Type type)
        {
            return isType(type, typeof(Int16), typeof(Int32), typeof(Nullable<Int16>), typeof(Nullable<Int32>));
        }

        public static bool IsLongType(this Type type)
        {
            return isType(type, typeof(Int64), typeof(Nullable<Int64>));
        }

        public static bool IsDecimalType(this Type type)
        {
            return isType(type, typeof(decimal), typeof(decimal?), typeof(float), typeof(float?), typeof(double), typeof(double?), typeof(Single), typeof(Single?));
        }

        public static bool IsCharacterType(this Type type)
        {
            return isType(type, typeof(char), typeof(string));
        }

        public static bool IsDateTimeType(this Type type)
        {
            return isType(type, typeof(DateTime), typeof(Nullable<DateTime>));
        }

        public static bool IsBooleanType(this Type type)
        {
            return isType(type, typeof(bool), typeof(Nullable<bool>));
        }

        public static Type GetNonNullableType(this Type type)
        {
            if (!type.Equals(typeof(Nullable<>)))
                return type;

            return type.GenericTypeArguments[0];
        }

        public static bool IsLockRequestTimeoutException(this Exception exception)
        {
            if (exception == null) return false;

            var sqlException = exception as SqlException;
            if (sqlException != null && sqlException.Number == 1222) return true;

            return IsLockRequestTimeoutException(exception.InnerException);
        }

        public static bool In<T>(this T value, params T[] trueValues)
        {
            if (trueValues == null || trueValues.Length < 1)
                return false;

            return trueValues.Contains(value);
        }

        public static void AddMicroORM(this Microsoft.Extensions.DependencyInjection.IServiceCollection services, string connectionString, ILog logger = null)
        {
            services.
                AddTransient<ILog>(p => logger ?? new ConsoleLogWriter()).
                AddSingleton<SQLDatabaseOptions>((p) => new SQLDatabaseOptions() { ConnectionString = connectionString }).
                AddTransient<Factory>();
        }

    }
}
