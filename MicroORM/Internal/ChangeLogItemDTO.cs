using System;
using System.Collections.Generic;
using System.Text;

namespace MicroORM.Internal
{
    public class ChangeLogItemDTO
    {
        public object Model { get; internal set; }
        public string TableName { get; internal set; }
        public string PKName { get; internal set; }
        public string PKValue { get; internal set; }
        public string State { get; internal set; }
    }
}
