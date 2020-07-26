using System;
using System.Collections.Generic;
using System.Text;

namespace MicroORM.Internal
{
    internal static class Utils
    {

        internal static string GetUniqueId()
        {
            return Guid.NewGuid().ToString().Replace("-", "");
        }

    }
}
