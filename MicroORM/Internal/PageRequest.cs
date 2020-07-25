using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MicroORM.Core
{
    public class PageRequest
    {
        public int Skip { get; set; }
        public int Take { get; set; }
        public string[] OrderByFields { get; set; }
    }
}
