using MicroORM.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MicroORM.Results
{
  public  class TemporaryTableResult:AdoResult
    {
        public string TableName { get; set; }
    }
}
