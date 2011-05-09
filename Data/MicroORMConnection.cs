using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;

namespace MicroORM.Data
{
    public class MicroOrmConnection : IDisposable
    {
        #region Fields

        private static readonly IDictionary<Type, DbType> ParameterTypes
            = new Dictionary<Type, DbType>
                  {
                      {typeof (string), DbType.AnsiString},
                      {typeof (short), DbType.Int16},
                      {typeof (int), DbType.Int32},
                      {typeof (long), DbType.Int64},
                      {typeof (double), DbType.Double},
                      {typeof (decimal), DbType.Currency},
                      {typeof (byte), DbType.Byte},
                      {typeof (sbyte), DbType.SByte},
                      {typeof (ushort), DbType.UInt16},
                      {typeof (uint), DbType.UInt32},
                      {typeof (ulong), DbType.UInt64},
                      {typeof (DateTime), DbType.DateTime},
                      {typeof (Guid), DbType.Guid},
                      {typeof (bool), DbType.Boolean},
                      {typeof (float), DbType.Single},
                      {typeof (short?), DbType.Int16},
                      {typeof (int?), DbType.Int32},
                      {typeof (long?), DbType.Int64},
                      {typeof (double?), DbType.Double},
                      {typeof (decimal?), DbType.Currency},
                      {typeof (byte?), DbType.Byte},
                      {typeof (sbyte?), DbType.SByte},
                      {typeof (ushort?), DbType.UInt16},
                      {typeof (uint?), DbType.UInt32},
                      {typeof (ulong?), DbType.UInt64},
                      {typeof (DateTime?), DbType.DateTime},
                      {typeof (Guid?), DbType.Guid},
                      {typeof (bool?), DbType.Boolean},
                      {typeof (float?), DbType.Single},
                  };

        private readonly IDbConnection _connection;
        private readonly IDictionary<Type, object> _entityConfigurations;
        private readonly IDictionary<Identity, Identity> _identityCache;
        private readonly IDictionary<Identity, IDictionary<object, ICollection<Tuple<DbType, string, object>>>> _parameterCache;

        #endregion

        #region Constructors

        public MicroOrmConnection(IDbConnection connection)
        {
            _connection = connection;
            _entityConfigurations = new Dictionary<Type, object>();
            _identityCache = new ConcurrentDictionary<Identity, Identity>();
            _parameterCache = new Dictionary<Identity, IDictionary<object, ICollection<Tuple<DbType, string, object>>>>();
        }

        #endregion

        #region Public Methods

        public void Open()
        {
            _connection.Open();
        }
        
        public EntityConfiguration<T> Entity<T>()
        {
            var configuration = new EntityConfiguration<T>();
            
            _entityConfigurations[typeof(T)] = configuration;

            return configuration;
        }

        /// <summary>
        /// Gets the first element from the resultset of executing given SQL statement on the specified connection.
        /// </summary>
        /// <typeparam name="T">The generic type to which the resultset should be mapped.</typeparam>
        /// <param name="connection">The connection.</param>
        /// <param name="sql">The SQL statement.</param>
        /// <param name="parameters">The parameters supplied via the properties of an anonymous type.</param>
        /// <returns></returns>
        /// <example>
        /// Order myOrder = myConnection.First&gt;Order&lt;("SELECT * FROM Orders WHERE OrderID = @OrderID", new { OrderID = "12345" });
        /// </example>
        public T First<T>(string sql, object parameters = null)
        {
            var identity = GetOrCreateIdentity(sql, typeof(T));

            using (IDbCommand command = CreateCommand(identity, parameters))
            using (IDataReader reader = command.ExecuteReader())
            {
                if (reader.Read())
                {
                    return DeserializeToEntity<T>(identity, reader);
                }

                return default(T);
            }
        }

        /// <summary>
        /// Queries the IDbConnection with the provided SQL statement and returns an enumeration of the resultset mapped to the specified generic Type.
        /// Optionally parameters can be supplied via an anonymous type.
        /// </summary>
        /// <typeparam name="T">The generic type to which the resultset should be mapped.</typeparam>
        /// <param name="connection">The connection.</param>
        /// <param name="sql">The SQL statement.</param>
        /// <param name="parameters">The parameters supplied via the properties of an anonymous type.</param>
        /// <returns></returns>
        /// <example>
        /// IEnumerable&gt;Order&lt; myOrders = myConnection.Query&gt;Order&lt;("SELECT * FROM Orders WHERE OrderID = @OrderID", new { OrderID = "12345" });
        /// </example>
        public IEnumerable<T> Query<T>(string sql, object parameters = null)
        {
            var identity = GetOrCreateIdentity(sql, typeof(T));

            using (IDbCommand command = CreateCommand(identity, parameters))
            using (IDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    yield return DeserializeToEntity<T>(identity, reader);
                }
            }
        }

        public IEnumerable<object[]> Query(string sql, string parameterName = null, string parameterValue = null)
        {
            using (IDbCommand command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                
                if (parameterName != null || parameterValue != null)
                {
                    var parameter = command.CreateParameter();
                    parameter.ParameterName = parameterName;
                    parameter.Value = parameterValue;

                    command.Parameters.Add(parameter);
                }

                using (IDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var values = new object[20];

                        reader.GetValues(values);

                        yield return values;
                    }
                }
            }
        }

        #endregion

        #region Private Methods

        private Identity GetOrCreateIdentity(string sql, Type entityType)
        {
            var identity = new Identity(sql, entityType);

            Identity cachedIdentity;
            if (!_identityCache.TryGetValue(identity, out cachedIdentity))
            {
                _identityCache.Add(identity, identity);
            }

            return cachedIdentity ?? identity;
        }
        private IDbCommand CreateCommand(Identity identity, object parameters)
        {
            IDbCommand command = _connection.CreateCommand();

            command.CommandText = identity.Sql;

            foreach (Tuple<DbType, string, object> parameter in CreateParameterCollection(identity, parameters))
            {
                IDbDataParameter dbParameter = command.CreateParameter();

                dbParameter.DbType = parameter.Item1;
                dbParameter.ParameterName = parameter.Item2;
                dbParameter.Value = parameter.Item3;

                command.Parameters.Add(dbParameter);
            }

            return command;
        }
        private IEnumerable<Tuple<DbType, string, object>> CreateParameterCollection(Identity identity, object parameters)
        {
            if (parameters == null)
            {
                return new List<Tuple<DbType, string, object>>();
            }

            IDictionary<object, ICollection<Tuple<DbType, string, object>>> parameterCache;
            if (!_parameterCache.TryGetValue(identity, out parameterCache))
            {
                parameterCache = new Dictionary<object, ICollection<Tuple<DbType, string, object>>>();
                _parameterCache[identity] = parameterCache;
            }

            ICollection<Tuple<DbType, string, object>> parameterCollection;
            if (!parameterCache.TryGetValue(parameters, out parameterCollection))
            {
                parameterCollection = new List<Tuple<DbType, string, object>>();

                foreach (PropertyInfo property in parameters.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public))
                {
                    parameterCollection.Add(
                        new Tuple<DbType, string, object>(
                            ParameterTypes[property.PropertyType],
                            string.Format("@{0}", property.Name),
                            property.GetValue(parameters, null)
                            )
                        );
                }

                parameterCache[parameters] = parameterCollection;
            }

            return parameterCollection;
        }
        private T DeserializeToEntity<T>(Identity identity, IDataReader reader)
        {
            Type entityType = typeof(T);

            Delegate deserializer;
            if (!identity.TryGetDeserializer(entityType, out deserializer))
            {
                deserializer = CreateDeserializer(entityType, reader);

                identity.CacheDeserializer(entityType, deserializer);
            }

            return (T)deserializer.DynamicInvoke(reader);
            //((MapEntityImplementation<T>)deserializer)(reader);
        }
        private Delegate CreateDeserializer(Type entityType, IDataReader reader, int offset = 0, int endOffset = -1)
        {
            PropertyInfo[] properties = entityType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);

            // Remark: perhaps adjust the fluent api to allow property mappings, so we can distinguish column mappings here.
            // Match reader's columns to the entity's columns
            var matches = from index in Enumerable.Range(offset, endOffset == -1 ? reader.FieldCount - 1 : endOffset)
                          from column in new[] { new { Index = index, Name = reader.GetName(index) } }
                          let info = properties.FirstOrDefault(p => string.Equals(column.Name, p.Name, StringComparison.InvariantCulture))
                                  ?? properties.FirstOrDefault(p => string.Equals(column.Name, p.Name, StringComparison.InvariantCultureIgnoreCase))
                          where info != null
                          select new { column.Index, Info = info };

            MethodInfo getItem = typeof(IDataRecord).GetMethod("GetValue", new[] { typeof(int) });

            // Our method MapEntity will have returntype T and one parameter DbDataReader
            var mapEntity = new DynamicMethod("MapEntity", entityType, new[] { typeof(IDataReader) }, true);

            ILGenerator il = mapEntity.GetILGenerator();
            // T instance = new T();
            il.Emit(OpCodes.Newobj, entityType.GetConstructor(Type.EmptyTypes)); // Stack: [ T ]
            foreach (var match in matches)
            {
                Label isDbNullLabel = il.DefineLabel();
                Label continueLabel = il.DefineLabel();

                il.Emit(OpCodes.Dup); // Stack: [ T | T ]
                il.Emit(OpCodes.Ldarg_0); // Stack: [ T | T | Reader]

                // object value = reader[Index];
                il.Emit(OpCodes.Ldc_I4, match.Index); // Stack: [ T | T | Reader | Index ]
                il.Emit(OpCodes.Callvirt, getItem); // Stack: [ T | T | ValueAsObject ]

                // if (value != DBNull.Value)
                il.Emit(OpCodes.Dup); // Stack: [ T | T | ValueAsObject | ValueAsObject ]
                il.Emit(OpCodes.Isinst, typeof(DBNull)); // Stack: [ T | T | ValueAsObject | DBNullOrNull ]
                il.Emit(OpCodes.Brtrue_S, isDbNullLabel); // Stack: [ T | T | ValueAsObject ]

                // instance.Property = (PropertyType)value;
                il.Emit(OpCodes.Unbox_Any, match.Info.PropertyType); // Stack: [ T | T | CastedValue ]
                il.Emit(OpCodes.Callvirt, match.Info.GetSetMethod(true)); // Stack: [ T ]
                il.Emit(OpCodes.Br_S, continueLabel); // Stack: [ T ]

                il.MarkLabel(isDbNullLabel); // Stack: [ T | T | ValueAsObject ]
                il.Emit(OpCodes.Pop); // Stack: [ T | T ]
                il.Emit(OpCodes.Pop); // Stack: [ T ]

                il.MarkLabel(continueLabel); // Stack: [ T ]
            }
            // return instance;
            il.Emit(OpCodes.Ret); // And return it off the stack

            return mapEntity.CreateDelegate(typeof (MapEntityImplementation<>).MakeGenericType(entityType));
        }

        #endregion

        #region Nested type: Identity

        private class Identity : IEquatable<Identity>
        {
            private readonly int _identityHash;
            private IDictionary<Type, Delegate> _deserializerCache;
            private IDictionary<Type, Delegate> DeserializerCache
            {
                get
                {
                    if (_deserializerCache == null)
                    {
                        _deserializerCache = new ConcurrentDictionary<Type, Delegate>();
                    }

                    return _deserializerCache;
                }
            }

            public string Sql { get; private set; }

            public Identity(string sql, Type entityType)
            {
                Sql = sql;

                _identityHash = sql.GetHashCode() ^ entityType.GetHashCode();
            }

            #region Public Members

            public void CacheDeserializer<T>(MapEntityImplementation<T> deserializer)
            {
                DeserializerCache.Add(typeof(T), deserializer);
            }
            public void CacheDeserializer(Type entityType, Delegate deserializer)
            {
                DeserializerCache.Add(entityType, deserializer);
            }
            public bool TryGetDeserializer<T>(out MapEntityImplementation<T> deserializer)
            {
                Delegate cachedDeserializer;
                bool isCached = DeserializerCache.TryGetValue(typeof (T), out cachedDeserializer);
                deserializer = (MapEntityImplementation<T>)cachedDeserializer;

                return isCached;
            }
            public bool TryGetDeserializer(Type entityType, out Delegate deserializer)
            {
                Delegate cachedDeserializer;
                bool isCached = DeserializerCache.TryGetValue(entityType, out cachedDeserializer);
                deserializer = cachedDeserializer;

                return isCached;
            }
            public override int GetHashCode()
            {
                return _identityHash;
            }

            #endregion

            #region IEquatable<Identity> Members

            public bool Equals(Identity other)
            {
                return GetHashCode() == other.GetHashCode();
            }

            #endregion
        }

        #endregion

        #region Nested type: MapEntityImplementation

        private delegate T MapEntityImplementation<out T>(IDataReader reader);

        #endregion

        #region Nested types: EntityConfiguration

        public class EntityConfiguration<TEntity>
        {
            public string TableName { get; private set; }
            public Expression KeyExpression { get; private set; }
            public IDictionary<Type, object> MemberConfigurations { get; private set; }

            internal EntityConfiguration()
            {
                MemberConfigurations = new Dictionary<Type, object>();
            }

            public EntityConfiguration<TEntity> MapsToTable(string tableName)
            {
                TableName = tableName;

                return this;
            }

            public EntityConfiguration<TEntity> HasKey<TKey>(Expression<Func<TEntity, TKey>> key)
            {
                KeyExpression = key;

                return this;
            }
            public LeftOneRelationship<TEntity, TMember> HasOne<TMember>(Expression<Func<TEntity, TMember>> memberExpression)
            {
                return new LeftOneRelationship<TEntity, TMember>(memberExpression);
            }
            public LeftManyRelationship<TEntity, TMember> HasMany<TMember>(Expression<Func<TEntity, ICollection<TMember>>> memberExpression)
            {
                return new LeftManyRelationship<TEntity, TMember>(memberExpression);
            }
        }
        public class LeftOneRelationship<TLeft, TRight>
        {
            public Expression<Func<TLeft, TRight>> LeftMemberExpression { get; private set; }

            internal LeftOneRelationship(Expression<Func<TLeft, TRight>> memberExpression)
            {
                LeftMemberExpression = memberExpression;
            }

            public OneToOneRelationship<TLeft, TRight> WithOne(Expression<Func<TRight, TLeft>> rightMemberExpression)
            {
                return new OneToOneRelationship<TLeft, TRight>(LeftMemberExpression, rightMemberExpression);
            }
            public OneToManyRelationship<TLeft, TRight> WithMany(Expression<Func<TRight, ICollection<TLeft>>> rightMemberExpression)
            {
                return new OneToManyRelationship<TLeft, TRight>(LeftMemberExpression, rightMemberExpression);
            }
        }
        public class OneToOneRelationship<TLeft, TRight>
        {
            public Expression<Func<TLeft, TRight>> LeftMemberExpression { get; private set; }
            public Expression<Func<TRight, TLeft>> RightMemberExpression { get; private set; }

            internal OneToOneRelationship(Expression<Func<TLeft, TRight>> leftMemberExpression, Expression<Func<TRight, TLeft>> rightMemberExpression)
            {
                LeftMemberExpression = leftMemberExpression;
                RightMemberExpression = rightMemberExpression;
            }
        }
        public class OneToManyRelationship<TLeft, TRight>
        {
            public Expression<Func<TLeft, TRight>> LeftMemberExpression { get; private set; }
            public Expression<Func<TRight, ICollection<TLeft>>> RightMemberExpression { get; private set; }

            internal OneToManyRelationship(Expression<Func<TLeft, TRight>> leftMemberExpression, Expression<Func<TRight, ICollection<TLeft>>> rightMemberExpression)
            {
                LeftMemberExpression = leftMemberExpression;
                RightMemberExpression = rightMemberExpression;
            }

            public void HasForeignKey<TKey>(Expression<Func<TLeft, TKey>> keyExpression)
            {

            }
        }

        public class LeftManyRelationship<TLeft, TRight>
        {
            public Expression<Func<TLeft, ICollection<TRight>>> LeftMemberExpression { get; private set; }

            internal LeftManyRelationship(Expression<Func<TLeft, ICollection<TRight>>> memberExpression)
            {
                LeftMemberExpression = memberExpression;
            }

            public ManyToOneRelationship<TLeft, TRight> WithOne(Expression<Func<TRight, TLeft>> rightMemberExpression)
            {
                return new ManyToOneRelationship<TLeft, TRight>(LeftMemberExpression, rightMemberExpression);
            }
            public ManyToManyRelationship<TLeft, TRight> WithMany(Expression<Func<TRight, ICollection<TLeft>>> rightMemberExpression)
            {
                return new ManyToManyRelationship<TLeft, TRight>(LeftMemberExpression, rightMemberExpression);
            }
        }
        public class ManyToOneRelationship<TLeft, TRight>
        {
            public Expression<Func<TLeft, ICollection<TRight>>> LeftMemberExpression { get; private set; }
            public Expression<Func<TRight, TLeft>> RightMemberExpression { get; private set; }

            internal ManyToOneRelationship(Expression<Func<TLeft, ICollection<TRight>>> leftMemberExpression, Expression<Func<TRight, TLeft>> rightMemberExpression)
            {
                LeftMemberExpression = leftMemberExpression;
                RightMemberExpression = rightMemberExpression;
            }

            public void HasForeignKey<TKey>(Expression<Func<TRight, TKey>> keyExpression)
            {
                
            }
        }
        public class ManyToManyRelationship<TLeft, TRight>
        {
            public Expression<Func<TLeft, ICollection<TRight>>> LeftMemberExpression { get; private set; }
            public Expression<Func<TRight, ICollection<TLeft>>> RightMemberExpression { get; private set; }

            internal ManyToManyRelationship(Expression<Func<TLeft, ICollection<TRight>>> leftMemberExpression, Expression<Func<TRight, ICollection<TLeft>>> rightMemberExpression)
            {
                LeftMemberExpression = leftMemberExpression;
                RightMemberExpression = rightMemberExpression;
            }
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            _connection.Dispose();
        }

        #endregion
    }
}