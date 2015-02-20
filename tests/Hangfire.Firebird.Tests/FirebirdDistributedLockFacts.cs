// This file is part of Hangfire.Firebird

// Copyright © 2015 Rob Segerink <https://github.com/rsegerink/Hangfire.Firebird>.
// 
// Hangfire.Firebird is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire.Firebird is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire.Firebird. If not, see <http://www.gnu.org/licenses/>.
//
// This work is based on the work of Sergey Odinokov, author of 
// Hangfire. <http://hangfire.io/>
//   
//    Special thanks goes to him.

﻿using System;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using Dapper;
using Moq;
using FirebirdSql.Data.FirebirdClient;
using Xunit;

namespace Hangfire.Firebird.Tests
{
    public class FirebirdDistributedLockFacts
    {
        private readonly TimeSpan _timeout = TimeSpan.FromSeconds(5);

        [Fact]
        public void Ctor_ThrowsAnException_WhenResourceIsNullOrEmpty()
        {
            FirebirdStorageOptions options = new FirebirdStorageOptions();

            var exception = Assert.Throws<ArgumentNullException>(
                () => new FirebirdDistributedLock("", _timeout, new Mock<IDbConnection>().Object, options));

            Assert.Equal("resource", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            FirebirdStorageOptions options = new FirebirdStorageOptions();

            var exception = Assert.Throws<ArgumentNullException>(
                () => new FirebirdDistributedLock("hello", _timeout, null, options));

            Assert.Equal("connection", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new FirebirdDistributedLock("hi", _timeout, new Mock<IDbConnection>().Object, null));

            Assert.Equal("options", exception.ParamName);
        }

        /*[Fact, CleanDatabase]
        public void Ctor_AcquiresExclusiveApplicationLock_WithUseNativeDatabaseTransactions_OnSession()
        {
            PostgreSqlStorageOptions options = new PostgreSqlStorageOptions()
            {
                SchemaName = GetSchemaName(),
                UseNativeDatabaseTransactions = true
            };

            UseConnection(connection =>
            {
                // ReSharper disable once UnusedVariable
                var distributedLock = new PostgreSqlDistributedLock("hello", _timeout, connection, options);

                var lockCount = connection.Query<long>(
                    @"select count(*) from """ + GetSchemaName() + @""".""lock"" where ""resource"" = @resource", new { resource = "hello" }).Single();

                Assert.Equal(lockCount, 1);
                //Assert.Equal("Exclusive", lockMode);
            });
        }*/

        [Fact, CleanDatabase]
        public void Ctor_AcquiresExclusiveApplicationLock_WithoutUseNativeDatabaseTransactions_OnSession()
        {
            FirebirdStorageOptions options = new FirebirdStorageOptions()
            {
                //UseNativeDatabaseTransactions = false
            };

            UseConnection(connection =>
            {
                var distributedLock = new FirebirdDistributedLock("hello", _timeout, connection, options);

                var lockCount = connection.Query<long>(
                    string.Format(@"SELECT COUNT(*) FROM ""{0}.LOCK"" WHERE resource = @resource;", options.Prefix) , new { resource = "hello" }).Single();

                Assert.Equal(lockCount, 1);
                //Assert.Equal("Exclusive", lockMode);
            });
        }

        /*[Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_IfLockCanNotBeGranted_WithUseNativeDatabaseTransactions()
        {
            PostgreSqlStorageOptions options = new PostgreSqlStorageOptions()
            {
                SchemaName = GetSchemaName(),
                UseNativeDatabaseTransactions = true
            };

            var releaseLock = new ManualResetEventSlim(false);
            var lockAcquired = new ManualResetEventSlim(false);

            var thread = new Thread(
                () => UseConnection(connection1 =>
                {
                    using (new PostgreSqlDistributedLock("exclusive", _timeout, connection1, options))
                    {
                        lockAcquired.Set();
                        releaseLock.Wait();
                    }
                }));
            thread.Start();

            lockAcquired.Wait();

            UseConnection(connection2 =>
                Assert.Throws<PostgreSqlDistributedLockException>(
                    () => new PostgreSqlDistributedLock("exclusive", _timeout, connection2, options)));

            releaseLock.Set();
            thread.Join();
        }*/

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_IfLockCanNotBeGranted_WithoutUseNativeDatabaseTransactions()
        {
            FirebirdStorageOptions options = new FirebirdStorageOptions()
            {
                //UseNativeDatabaseTransactions = false
            };

            var releaseLock = new ManualResetEventSlim(false);
            var lockAcquired = new ManualResetEventSlim(false);

            var thread = new Thread(
                () => UseConnection(connection1 =>
                {
                    using (new FirebirdDistributedLock("exclusive", _timeout, connection1, options))
                    {
                        lockAcquired.Set();
                        releaseLock.Wait();
                    }
                }));
            thread.Start();

            lockAcquired.Wait();

            UseConnection(connection2 =>
                Assert.Throws<FirebirdDistributedLockException>(
                    () => new FirebirdDistributedLock("exclusive", _timeout, connection2, options)));

            releaseLock.Set();
            thread.Join();
        }

        /*[Fact, CleanDatabase]
        public void Dispose_ReleasesExclusiveApplicationLock_WithUseNativeDatabaseTransactions()
        {
            PostgreSqlStorageOptions options = new PostgreSqlStorageOptions()
            {
                SchemaName = GetSchemaName(),
                UseNativeDatabaseTransactions = true
            };

            UseConnection(connection =>
            {
                var distributedLock = new PostgreSqlDistributedLock("hello", _timeout, connection, options);
                distributedLock.Dispose();

                var lockCount = connection.Query<long>(
                    @"select count(*) from """ + GetSchemaName() + @""".""lock"" where ""resource"" = @resource", new { resource = "hello" }).Single();

                Assert.Equal(lockCount, 0);
            });
        }*/

        [Fact, CleanDatabase]
        public void Dispose_ReleasesExclusiveApplicationLock_WithoutUseNativeDatabaseTransactions()
        {
            FirebirdStorageOptions options = new FirebirdStorageOptions()
            {
                //UseNativeDatabaseTransactions = false
            };

            UseConnection(connection =>
            {
                var distributedLock = new FirebirdDistributedLock("hello", _timeout, connection, options);
                distributedLock.Dispose();

                var lockCount = connection.Query<long>(
                    string.Format(@"SELECT COUNT(*) FROM ""{0}.LOCK"" WHERE resource = @resource;", options.Prefix) , new { resource = "hello" }).Single();

                Assert.Equal(lockCount, 0);
            });
        }

        private void UseConnection(Action<IDbConnection> action)
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                action(connection);
            }
        }
    }
}
