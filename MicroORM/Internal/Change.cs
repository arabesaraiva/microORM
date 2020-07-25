using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MicroORM.Core
{
    public class Change<T>
    {

        internal Change(object pkValue)
        {
            this.PKValue = pkValue;
        }

        public ChangeType Type { get; internal set; }
        public T Model { get; internal set; }
        public SpecificField<T>[] SpecificFields { get; internal set; }
        public object PKValue { get; }
    }
}
