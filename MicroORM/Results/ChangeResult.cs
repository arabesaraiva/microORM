using MicroORM.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MicroORM.Results
{
    public class ChangeResult : AdoResult
    {
        public int InsertedCount { get; internal set; }
        public int UpdatedCount { get; internal set; }
        public int DeletedCount { get; internal set; }
        public int AffectedCount { get; internal set; }
    }
}
