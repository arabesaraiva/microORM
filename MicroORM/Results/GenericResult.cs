﻿using MicroORM.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MicroORM.Results
{
    public class GenericResult<T> : AdoResult
    {
        public T Result { get; set; }
    }

}
