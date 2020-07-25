﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MicroORM.Core;

namespace MicroORM.Results
{
    public class SelectResult<T> : AdoResult
    {
        public List<T> DataList { get; set; }
    }
}
