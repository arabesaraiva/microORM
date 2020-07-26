using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Security.Cryptography.Xml;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using MicroORM.Samples.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing.Template;
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

        /// <summary>
        /// Select with Top, optionally ordering by some field(s)
        /// </summary>
        [HttpGet("Top1")]
        public JsonResult GetTop1()
        {
            var selectResult = microOrm.Select<Client>().Top(1, c => c.LastBuyDate, c => c.ID).Execute();

            return new JsonResult(selectResult);
        }

        /// <summary>
        /// Select with Top, optionally ordering by some field(s)
        /// </summary>
        [HttpGet("Page/{page}")]
        public JsonResult GetPage(int page)
        {
            var selectResult = microOrm.Select<Client>().Page(1 * Math.Max(page - 1, 0), 1, c => c.LastBuyDate, c => c.ID).Execute();

            return new JsonResult(selectResult);
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
        /// Filter with the 'IN' operator
        /// </summary>
        [HttpGet("ActiveAndInactive")]
        public JsonResult GetActiveAndInactive()
        {
            var selectResult = microOrm.Select<Client>().WhereIn(c => c.IsActive, new bool[2] { true, false }).Execute();

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

        #region Exists & Count

        /// <summary>
        /// Check if a record exists easily and with good performance
        /// </summary>
        [HttpGet("Exists/{id}")]
        public JsonResult Exists(int id)
        {
            var existsResult = microOrm.Exists<Client>().Where(c => c.ID, id).Execute();

            return new JsonResult(existsResult);
        }

        /// <summary>
        /// Count the records with any conditions
        /// </summary>
        [HttpGet("Count")]
        public JsonResult Count()
        {
            var countResult = microOrm.Count<Client>().Where(c => c.IsActive, true).Execute();

            return new JsonResult(countResult);
        }

        #endregion

        #region Updates

        /// <summary>
        /// Update a record setting all the model's property values
        /// </summary>
        [HttpGet("Update/{id}")]
        public JsonResult Update(int id, [FromQuery()]string name = "")
        {
            if (string.IsNullOrWhiteSpace(name))
                return new JsonResult("Set the new name!");

            var selectResult = microOrm.Select<Client>().Where(c => c.ID, id).Execute();
            if (!selectResult.Success)
                return new JsonResult(selectResult);

            var model = selectResult.DataList?.FirstOrDefault();

            if (model == null)
                return new JsonResult("ID not found!");

            model.Name = name;

            var changeResult = microOrm.Change<Client>().Update(model).Execute();

            return new JsonResult(changeResult);
        }

        /// <summary>
        /// Update a specific field filtering by the ID, without needing to select the model first
        /// </summary>
        [HttpGet("UpdateSpecificField/{id}")]
        public JsonResult UpdateSpecificField(int id, [FromQuery()]string name = "")
        {
            if (string.IsNullOrWhiteSpace(name))
                return new JsonResult("Set the new name!");

            var changeCommand = microOrm.Change<Client>();

            var changeResult = changeCommand.UpdateSpecificFields(id, changeCommand.Set(c => c.Name, name)).Execute();

            return new JsonResult(changeResult);
        }

        /// <summary>
        /// Update a specific field with a condition, without needing to select the model first
        /// </summary>
        [HttpGet("UpdateName/{currentName}")]
        public JsonResult UpdateName(string currentName, [FromQuery()]string newName = "")
        {
            if (string.IsNullOrWhiteSpace(newName))
                return new JsonResult("Set the new name!");

            var changeResult = microOrm.UpdateWhere<Client>().Set(c => c.Name, newName).Where(c => c.Name, currentName).Execute();
            return new JsonResult(changeResult);
        }

        #endregion

        #region Inserts

        /// <summary>
        /// Insert a record setting all the model's property values
        /// </summary>
        [HttpGet("Insert")]
        public JsonResult Insert([FromQuery()]string name = "", DateTime? lastBuyDate = null, bool isActive = true)
        {
            if (string.IsNullOrWhiteSpace(name))
                return new JsonResult("Set the new name!");

            var model = new Client() { IsActive = isActive, LastBuyDate = lastBuyDate, Name = name };

            var changeResult = microOrm.Change<Client>().Insert(model).Execute();

            return new JsonResult(changeResult);
        }

        /// <summary>
        /// Insert a record setting only specific fields
        /// </summary>
        [HttpGet("InsertSpecificField")]
        public JsonResult InsertSpecificField(int id, [FromQuery()]string name = "")
        {
            if (string.IsNullOrWhiteSpace(name))
                return new JsonResult("Set the new name!");

            var changeCommand = microOrm.Change<Client>();

            var changeResult = changeCommand.InsertSpecificFields(id, changeCommand.Set(c => c.Name, name)).Execute();

            return new JsonResult(changeResult);
        }

        #endregion

        #region Delete

        /// <summary>
        /// Delete a record by passing it's model as a parameter
        /// </summary>
        [HttpGet("DeleteModel/{id}")]
        public JsonResult DeleteModel(int id)
        {
            var selectResult = microOrm.Select<Client>().Where(c => c.ID, id).Execute();
            if (!selectResult.Success)
                return new JsonResult(selectResult);

            var model = selectResult.DataList?.FirstOrDefault();

            if (model == null)
                return new JsonResult("ID not found!");

            var changeResult = microOrm.Change<Client>().Delete(model).Execute();

            return new JsonResult(changeResult);
        }

        /// <summary>
        /// Delete a record filtering by the PK, without needing to have the model first
        /// </summary>
        [HttpGet("DeleteById/{id}")]
        public JsonResult DeleteById(int id)
        {
            var changeResult = microOrm.Change<Client>().DeleteByPk(id).Execute();

            return new JsonResult(changeResult);
        }

        /// <summary>
        /// Delete one or many records with a condition
        /// </summary>
        [HttpGet("DeleteName/{name}")]
        public JsonResult DeleteName(string name)
        {
            var changeResult = microOrm.DeleteWhere<Client>().Where(c => c.Name, name).Execute();
            return new JsonResult(changeResult);
        }

        #endregion

        #region Others

        /// <summary>
        /// Execute any kind of SQL command
        /// </summary>
        [HttpGet("DuplicateTable")]
        public JsonResult DuplicateTable()
        {
            var result = microOrm.PrepareSql("INSERT INTO Client (Name, LastBuyDate, IsActive) SELECT Name, LastBuyDate, IsActive FROM Client").ExecuteCommand();
            return new JsonResult(result);
        }

        /// <summary>
        /// Share a connection and a transaction between the commands
        /// </summary>
        [HttpGet("CommandsWithTransaction")]
        public JsonResult CommandsWithTransaction([FromQuery()] bool throwException = false)
        {
            using (var conn = microOrm.GetNewConnection())
            {
                conn.BeginTransaction();

                var selectResult = microOrm.Select<Client>(existentConnection: conn).Where(c => c.IsActive, true).Execute();
                if (!selectResult.Success)
                    return new JsonResult(selectResult);

                var selectedRecords = selectResult.DataList;

                var deleteResult = microOrm.DeleteWhere<Client>(existentConnection: conn).Where(c => c.IsActive, true).Execute();
                if (!deleteResult.Success)
                    return new JsonResult(deleteResult);

                selectedRecords.ForEach(c => c.Name = c.Name + "_");

                if (throwException)
                    throw new Exception("Unexpected exception that Rollbacks the transaction!");

                var insertResult = microOrm.Change<Client>(existentConnection: conn).Insert(selectedRecords.ToArray()).Execute();
                if (!insertResult.Success)
                    return new JsonResult(insertResult);

                var commitResult = conn.Commit();
                return new JsonResult(commitResult);
            }
        }

        private class filterNamesTempTable
        {
            public string Name { get; set; }
        }

        /// <summary>
        /// Create and work with Temporary Tables
        /// </summary>
        [HttpGet("SelectWithTempTable")]
        public JsonResult SelectWithTempTable(string tableName = "")
        {
            using (var conn = microOrm.GetNewConnection())
            {
                using (var tempTable = microOrm.TemporaryTable<filterNamesTempTable>(existentConnection: conn).SetTableName(tableName))
                {
                    var tempTableResult = tempTable.Create();
                    if (!tempTableResult.Success)
                        return new JsonResult(tempTableResult);

                    tempTableResult= tempTable.BulkInsert(new filterNamesTempTable() { Name = "SAP" }, new filterNamesTempTable() { Name = "Facebook" });
                    if (!tempTableResult.Success)
                        return new JsonResult(tempTableResult);

                    string query = $"SELECT c.* FROM Client c WHERE EXISTS (SELECT TOP 1 1 FROM [{tempTableResult.TableName}] t WHERE t.Name = c.Name) ";
                    var selectResult = microOrm.PrepareSql(query, existentConnection: conn).ExecuteQuery<Client>(true);
                    return new JsonResult(selectResult);

                    //Optionally executes the Drop command, because the Dispose() already calls it
                    //tempTableResult = tempTable.Drop();
                    //if (!tempTableResult.Success)
                    //    return new JsonResult(tempTableResult);
                }
            }
        }

        #endregion


    }
}