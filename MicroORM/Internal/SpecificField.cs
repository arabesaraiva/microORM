using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace MicroORM.Core
{
    public class SpecificField<T>
    {

        public static SpecificField<T> Create<TProperty>(Expression<Func<T, TProperty>> propertySelector, TProperty value)
        {
            var memberExpression = propertySelector.Body as MemberExpression;
            if (String.IsNullOrWhiteSpace(memberExpression?.Member?.Name))
                throw new Exception("Invalid expression");

            var propertyInfo = (PropertyInfo)memberExpression.Member;

            var specificField = new SpecificField<T>(propertyInfo, value);

            return specificField;
        }

        private readonly PropertyInfo _property;
        private readonly object _value;
        private SpecificField(PropertyInfo property, object value)
        {
            _property = property;
            _value = value;
        }

        internal PropertyInfo GetProperty()
        {
            return _property;
        }

        internal object GetValue()
        {
            return _value;
        }

    }
}
