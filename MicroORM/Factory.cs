using MicroORM.Commands;
using MicroORM.Internal;
using MicroORM.Middleware;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace MicroORM
{
    public class Factory
    {

        internal ILog _logger;
        SQLDatabaseOptions _dbOptions;
        public Factory(ILog logger, SQLDatabaseOptions dbOptions)
        {
            _logger = logger ?? new ConsoleLogWriter();
            _dbOptions = dbOptions;
        }


        public SQLDatabaseConnection GetNewConnection(string customConnectionString = "")
        {
            var connection = !String.IsNullOrWhiteSpace(customConnectionString) ? new SQLDatabaseConnection(customConnectionString) :
                             new SQLDatabaseConnection(_dbOptions.ConnectionString);

            return connection;
        }
        public Results.GenericResult<DateTime> GetServerDateTime(string customConnectionString = "", SQLDatabaseConnection existentConnection = null)
        {
            using (var command = new AdoCommand(customConnectionString, existentConnection, this))
            {
                return command.GetServerDateTime();
            }
        }

        public RawSqlCommand PrepareSql(string sqlCommand, string customConnectionString = "", SQLDatabaseConnection existentConnection = null)
        {
            return new RawSqlCommand(sqlCommand, customConnectionString, existentConnection: existentConnection, factory: this);
        }

        public ExistsCommand<T> Exists<T>(string customConnectionString = "", SQLDatabaseConnection existentConnection = null)
        {
            return new ExistsCommand<T>(customConnectionString, existentConnection: existentConnection, factory: this);
        }

        public CountCommand<T> Count<T>(string customConnectionString = "", SQLDatabaseConnection existentConnection = null)
        {
            return new CountCommand<T>(customConnectionString, existentConnection: existentConnection, factory: this);
        }

        public SelectCommand<T, T> Select<T>(string customConnectionString = "", SQLDatabaseConnection existentConnection = null)
        {
            return new SelectCommand<T, T>(customConnectionString, null, existentConnection: existentConnection, factory: this);
        }

        public SelectCommand<T, TResult> Select<T, TResult>(string customConnectionString = "", SQLDatabaseConnection existentConnection = null) where TResult : new()
        {
            return new SelectCommand<T, TResult>(customConnectionString, null, existentConnection: existentConnection, factory: this);
        }

        public ChangeCommand<T> Change<T>(string customConnectionString = "", SQLDatabaseConnection existentConnection = null)
        {
            return new ChangeCommand<T>(customConnectionString, existentConnection: existentConnection, factory: this);
        }

        public DeleteConditionalCommand<T> DeleteWhere<T>(string customConnectionString = "", SQLDatabaseConnection existentConnection = null)
        {
            return new DeleteConditionalCommand<T>(customConnectionString, existentConnection, factory: this);
        }

        public UpdateConditionalCommand<T> UpdateWhere<T>(string customConnectionString = "", SQLDatabaseConnection existentConnection = null)
        {
            return new UpdateConditionalCommand<T>(customConnectionString, existentConnection, factory: this);
        }

        public BulkCommand<T> Bulk<T>(string customConnectionString = "", SQLDatabaseConnection existentConnection = null)
        {
            return new BulkCommand<T>(customConnectionString, existentConnection, factory: this);
        }

        public TemporaryTableCommand<T> TemporaryTable<T>(string customConnectionString = "", SQLDatabaseConnection existentConnection = null)
        {
            return new TemporaryTableCommand<T>(customConnectionString, existentConnection, factory: this);
        }

        public static Results.EmptyResult ValidateModel<T>(T model)
        {
            try
            {
                if (model == null) return new Results.EmptyResult() { Exception = new ArgumentNullException(nameof(model), "Model is null") };

                var context = new System.ComponentModel.DataAnnotations.ValidationContext(model, serviceProvider: null, items: null);
                var results = new List<System.ComponentModel.DataAnnotations.ValidationResult>();

                var isValid = System.ComponentModel.DataAnnotations.Validator.TryValidateObject(model, context, results, true);
                if (isValid)
                    return new Results.EmptyResult();

                return new Results.EmptyResult() { Exception = new Exception(string.Join(" / ", results.Select(r => r.ErrorMessage).Distinct())) };
            }
            catch (Exception ex)
            {
                return new Results.EmptyResult() { Exception = ex };
            }
        }

        public Factory<T> Complex<T>()
        {
            return new Factory<T>(this);
        }

    }

    public class Factory<T>
    {

        Factory _factory;
        internal Factory(Factory factory)
        {
            _factory = factory;
        }

        public SelectCommand<T, TResult> Select<TResult>(Expression<Func<T, TResult>> targetTypeInitializer, string customConnectionString = "", SQLDatabaseConnection existentConnection = null)
        {
            return new SelectCommand<T, TResult>(customConnectionString, targetTypeInitializer, existentConnection, factory: _factory);
        }

    }

}
