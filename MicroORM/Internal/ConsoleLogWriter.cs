using MicroORM.Middleware;
using System;
using System.Collections.Generic;
using System.Text;

namespace MicroORM.Internal
{
    public class ConsoleLogWriter : ILog
    {
        public string GetModelLogField(Type type)
        {
            return string.Empty;
        }

        public bool IsModelSavingLog(Type type)
        {
            return true;
        }

        public void Log(ELogType type, string message, Exception exception =null)
        {
            Console.WriteLine(message);
        }

        public void LogError(Exception exception)
        {
            Log(ELogType.Error, exception.Message, exception);
        }
    }
}
