using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace MicroORM.Samples.Models
{
    public class Client
    {
        [Key(), System.ComponentModel.DataAnnotations.Schema.DatabaseGenerated(System.ComponentModel.DataAnnotations.Schema.DatabaseGeneratedOption.Identity)]
        public int ID { get; set; }

        [MaxLength(50)]
        public string Name { get; set; }

        public bool IsActive { get; set; }

        public DateTime? LastBuyDate { get; set; }
    }
}
