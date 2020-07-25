using System;
using System.Collections.Generic;
using System.Text;

namespace MicroORM.Middleware
{
    public interface ILog
    {
        void Log(ELogType type, string message, Exception exception = null);

        void LogError(Exception exception);

        bool IsModelSavingLog(Type type);

        string GetModelLogField(Type type); 

    }
}
