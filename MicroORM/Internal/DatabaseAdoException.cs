using System;
using System.Collections.Generic;
using System.Text;

namespace MicroORM.Internal
{
    public class DatabaseAdoException : Exception
    {
        private List<ChangeLogItemDTO> list;

        public DatabaseAdoException(List<ChangeLogItemDTO> list, Exception ex):base("Database error has occurred.", ex)
        {
            this.list = list;
        }
    }
}
