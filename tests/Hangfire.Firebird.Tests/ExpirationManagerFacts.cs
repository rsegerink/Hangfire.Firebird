﻿using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using FirebirdSql.Data.FirebirdClient;
using Xunit;

namespace Hangfire.Firebird.Tests
{
    public class ExpirationManagerFacts
    {
        private readonly CancellationToken _token;
        private readonly FirebirdStorageOptions _options;

        public ExpirationManagerFacts()
        {
            var cts = new CancellationTokenSource();
            _token = cts.Token;
            _options = new FirebirdStorageOptions();
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => new ExpirationManager(null, _options));
        }

        [Fact, CleanDatabase]
        public void Execute_RemovesOutdatedRecords()
        {
            using (var connection = CreateConnection())
            {
                var entryId = CreateExpirationEntry(connection, _options, DateTime.UtcNow.AddMonths(-1));
                var manager = CreateManager(connection);

                manager.Execute(_token);

                Assert.True(IsEntryExpired(connection, _options, entryId));
            }
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithNoExpirationTimeSet()
        {
            using (var connection = CreateConnection())
            {
                var entryId = CreateExpirationEntry(connection, _options, null);
                var manager = CreateManager(connection);

                manager.Execute(_token);

                Assert.False(IsEntryExpired(connection, _options, entryId));
            }
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithFreshExpirationTime()
        {
            using (var connection = CreateConnection())
            {
                var entryId = CreateExpirationEntry(connection, _options, DateTime.Now.AddMonths(1));
                var manager = CreateManager(connection);

                manager.Execute(_token);

                Assert.False(IsEntryExpired(connection, _options, entryId));
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_CounterTable()
        {
            using (var connection = CreateConnection())
            {
                // Arrange
                string createSql = string.Format(@"
                    INSERT INTO ""{0}.COUNTER"" (""KEY"", ""VALUE"", expireat) 
                    VALUES ('key', 1, @expireAt)", _options.Prefix);
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<long>(string.Format(@"SELECT COUNT(*) FROM ""{0}.COUNTER""", _options.Prefix)).Single());
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_JobTable()
        {
            using (var connection = CreateConnection())
            {
                // Arrange
                string createSql = string.Format(CultureInfo.InvariantCulture, @"
                    INSERT INTO ""{0}.JOB"" (invocationdata, arguments, createdat, expireat) 
                    VALUES ('', '', DATEADD(minute, -{1:N5}*60, current_timestamp), @expireAt)", _options.Prefix, _options.UtcOffset);
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<long>(string.Format(@"SELECT COUNT(*) FROM ""{0}.JOB""", _options.Prefix)).Single());
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_ListTable()
        {
            using (var connection = CreateConnection())
            {
                // Arrange
                string createSql = string.Format(@"
                    INSERT INTO ""{0}.LIST"" (""KEY"", expireat) 
                    values ('key', @expireAt)", _options.Prefix);
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<long>(string.Format(@"SELECT COUNT(*) FROM ""{0}.LIST""", _options.Prefix)).Single());
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_SetTable()
        {
            using (var connection = CreateConnection())
            {
                // Arrange
                string createSql = string.Format(@"
                    INSERT INTO ""{0}.SET"" (""KEY"", score, ""VALUE"", expireat) 
                    VALUES ('key', 0, '', @expireAt)", _options.Prefix);
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<long>(string.Format(@"SELECT COUNT(*) FROM ""{0}.SET""", _options.Prefix)).Single());
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_HashTable()
        {
            using (var connection = CreateConnection())
            {
                // Arrange
                string createSql = string.Format(@"
                    INSERT INTO ""{0}.HASH"" (""KEY"", field, ""VALUE"", expireat) 
                    VALUES ('key', 'field', '', @expireAt)", _options.Prefix);
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<long>(string.Format(@"SELECT COUNT(*) FROM ""{0}.HASH""", _options.Prefix)).Single());
            }
        }

        private static int CreateExpirationEntry(FbConnection connection, FirebirdStorageOptions options, DateTime? expireAt)
        {
            string insertSqlNull = @"
                INSERT INTO """ + options.Prefix + @".COUNTER"" (""KEY"", ""VALUE"", expireat)
                VALUES ('key', 1, null) 
                RETURNING id;";

            string insertSqlValue = @"
                INSERT INTO """ + options.Prefix + @".COUNTER"" (""KEY"", ""VALUE"", expireat) 
                VALUES ('key', 1, DATEADD(second, {0:N5}, " + string.Format(CultureInfo.InvariantCulture, @"DATEADD(minute, -{0:N5}*60, current_timestamp))) ", options.UtcOffset) + 
                "RETURNING id;";

            string insertSql = expireAt == null ? insertSqlNull : string.Format(insertSqlValue, ((long)(expireAt.Value - DateTime.UtcNow).TotalSeconds).ToString(CultureInfo.InvariantCulture));

            var recordId = connection.ExecuteScalar<int>(insertSql);
            return recordId;
        }

        private static bool IsEntryExpired(FbConnection connection, FirebirdStorageOptions options, int entryId)
        {
            var count = connection.Query<long>(string.Format(@"
                SELECT COUNT(*) FROM ""{0}.COUNTER"" WHERE id = @id;", options.Prefix), 
                new { id = entryId }).Single();
            
            return count == 0;
        }

        private FbConnection CreateConnection()
        {
            return ConnectionUtils.CreateConnection();
        }

        private ExpirationManager CreateManager(FbConnection connection)
        {
            var storage = new FirebirdStorage(connection, _options);
            return new ExpirationManager(storage, _options, TimeSpan.Zero);
        }
    }
}
