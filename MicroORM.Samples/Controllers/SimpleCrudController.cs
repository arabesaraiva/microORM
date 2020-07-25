using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using MicroORM.Samples.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;

namespace MicroORM.Samples.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class SimpleCrudController : ControllerBase
    {

        private readonly Factory microOrm;
        public SimpleCrudController(Factory microOrm)
        {
            this.microOrm = microOrm;
        }

        #region Selects

        /// <summary>
        /// Simple select with all table fields using the table model
        /// </summary>
        [HttpGet()]
        public JsonResult Get()
        {
            var selectResult = microOrm.Select<Client>().Execute();

            return new JsonResult(selectResult);
        }


        private class clientIdAndNameModel
        {
            [JsonIgnore()]
            public int ID { get; set; }
            [JsonIgnore()]
            public string Name { get; set; }
            public string IdAndName { get => $"{ID} - {Name}"; }
        }

        /// <summary>
        /// Select only fields defined in the <see cref="clientIdAndNameModel"/> from the table
        /// </summary>
        [HttpGet("GetIdAndNames")]
        public JsonResult GetIdAndNames()
        {
            var selectResult = microOrm.Select<Client, clientIdAndNameModel>().Execute();

            return new JsonResult(selectResult);
        }

        /// <summary>
        /// Select only fields defined in the anonymous type, without needing to create a model for a specif select case
        /// </summary>
        [HttpGet("GetNames")]
        public JsonResult GetNames()
        {
            var selectResult = microOrm.Complex<Client>().Select(c => new { c.Name }).Execute();

            return new JsonResult(selectResult);
        }

        private class clientSummaryItemModel
        {
            public bool IsActive { get; set; }
            public int Count { get; set; }
        }

        /// <summary>
        /// Use any kind of select command, from simple to complex queries and get the results as a pre-defined model
        /// </summary>
        [HttpGet("GetSummary")]
        public JsonResult GetSummary()
        {
            var selectResult = microOrm.PrepareSql("select IsActive, [Count] = count(1) FROM Client GROUP BY IsActive ").ExecuteQuery<clientSummaryItemModel>();

            return new JsonResult(selectResult);
        }


        /// <summary>
        /// Use any kind of select command, from simple to complex queries and get the results as a <see cref="Microsoft.Data.SqlClient.SqlDataReader"/>
        /// </summary>
        [HttpGet("GetLastId")]
        public JsonResult GetLastId()
        {
            var selectResult = microOrm.PrepareSql("select max(id) FROM Client ").ExecuteQueryAsDataReader();
            var reader = selectResult.Result;

            if (!selectResult.Success)
                return new JsonResult(selectResult);

            try
            {
                if (!reader.Read())
                    return new JsonResult(0);
                else
                    return new JsonResult(reader[0]);
            }
            finally
            {
                if (reader != null && !reader.IsClosed)
                    reader.Dispose();
            }
        }


        /// <summary>
        /// Use any kind of select command, from simple to complex queries and get the results as a <see cref="System.Data.DataTable"/>
        /// </summary>
        [HttpGet("GetOrdered")]
        public JsonResult GetOrdered()
        {
            var selectResult = microOrm.PrepareSql("select ROW_NUMBER() OVER ( ORDER BY Name ) as [Order], * FROM Client ORDER by 1 ").ExecuteQueryAsDataTable();
            if (!selectResult.Success)
                return new JsonResult(selectResult);

            var datatable = selectResult.Result;

            return new JsonResult(datatable.Select().Select(r => datatable.Columns.Cast<DataColumn>().Select(c => new { Field = c.ColumnName, Value = r[c.ColumnName] })));
        }


        #endregion

        #region Conditions

        /// <summary>
        /// Filter by a specific value in a field
        /// </summary>
        [HttpGet("{id}")]
        public Client Get(int id)
        {
            var selectResult = microOrm.Select<Client>().Where(c => c.ID, id).Execute();
            if (!selectResult.Success)
                return null;

            return selectResult.DataList?.FirstOrDefault();
        }

        /// <summary>
        /// Filter by a range of date or any other value
        /// </summary>
        [HttpGet("BuyedLast10Years")]
        public JsonResult GetBuyedLast10Years()
        {
            var selectResult = microOrm.Select<Client>().Where(c => c.LastBuyDate, DateTime.Today.AddYears(-10), DateTime.Today.AddDays(1)).Execute();

            return new JsonResult(selectResult);
        }

        /// <summary>
        /// Filter using the 'like' operator with wildcards
        /// </summary>
        [HttpGet("FirstLetterM")]
        public JsonResult GetFirstLetterM()
        {
            var selectResult = microOrm.Select<Client>().Where(c => c.Name, "M%", Core.ConditionType.Like).Execute();

            return new JsonResult(selectResult);
        }

        /// <summary>
        /// Filter with the 'not' operator
        /// </summary>
        [HttpGet("NotActive")]
        public JsonResult GetNotActive()
        {
            var selectResult = microOrm.Select<Client>().Where(c => c.IsActive, true, Core.ConditionType.Not).Execute();

            return new JsonResult(selectResult);
        }

        /// <summary>
        /// Filter by more than one condition
        /// </summary>
        [HttpGet("BuyedLast10YearsAndActive")]
        public JsonResult GetBuyedLast10YearsAndActive()
        {
            var selectResult = microOrm.Select<Client>().Where(c => c.LastBuyDate, DateTime.Today.AddYears(-10), DateTime.Today.AddDays(1)).Where(c => c.IsActive, true).Execute();

            return new JsonResult(selectResult);
        }

        private class clientSummaryByYear
        {
            public int Year { get; set; }
            public int Count { get; set; }
        }

        /// <summary>
        /// Filter complex queries with parameters
        /// </summary>
        [HttpGet("SummaryByYear")]
        public JsonResult GetSummaryByYear([FromQuery()]int minCount = 0)
        {
            var selectResult = microOrm.PrepareSql("select DATEPART(year, lastbuydate) as Year, count(1) as Count FROM Client GROUP BY DATEPART(year, lastbuydate) having count(1) >= @pMinCount ").AddParameter("pMinCount", minCount).ExecuteQuery<clientSummaryByYear>();

            return new JsonResult(selectResult);
        }

        #endregion







    }
}