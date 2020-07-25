using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MicroORM.Core
{
    internal class ParameterCommand
    {
        internal ParameterCommand(string sqlStatement, List<Tuple<string, object, Type>> parameters)
        {
            SqlStatement = sqlStatement;
            Parameters = parameters ??new List<Tuple<string, object, Type>>();
        }

        internal string SqlStatement { get; set; }
        internal List<Tuple<string, object, Type>> Parameters { get; set; }
    }
}
