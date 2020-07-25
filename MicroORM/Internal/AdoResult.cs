using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MicroORM.Core
{
    public abstract class AdoResult
    {
        internal AdoResult()
        {

        }

        public Exception Exception { get; internal set; }

        public bool Success { get { return Exception == null; } }
    }
}
