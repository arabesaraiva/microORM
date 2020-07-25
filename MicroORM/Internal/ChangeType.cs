using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MicroORM.Core
{
    public enum ChangeType
    {
        Insert = 0,
        Update = 1,
        Delete = 2,
        InsertOrUpdate = 3
    }
}
