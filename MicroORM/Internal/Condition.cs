using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace MicroORM.Core
{
    public class Condition
    {
        public string FieldName { get; internal set; }
        public Type FieldType { get; internal set; }
        public object MinValue { get; internal set; }
        public object MaxValue { get; internal set; }
        public ConditionType ConditionType { get; internal set; }
        public object[] InValues { get; internal set; }
        public bool EmptyInValues { get; internal set; }
    }
}
