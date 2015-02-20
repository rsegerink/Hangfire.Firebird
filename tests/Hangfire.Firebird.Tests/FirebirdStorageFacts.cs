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

using System;
using System.Linq;
using Xunit;

namespace Hangfire.Firebird.Tests
{
    public class FirebirdStorageFacts
    {
        private readonly FirebirdStorageOptions _options;

        public FirebirdStorageFacts()
        {
            _options = new FirebirdStorageOptions { PrepareSchemaIfNecessary = false };
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionStringIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new FirebirdStorage(nameOrConnectionString: null));

            Assert.Equal("nameOrConnectionString", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsValueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new FirebirdStorage("hello", null));

            Assert.Equal("options", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Ctor_CanCreateSqlServerStorage_WithExistingConnection()
        {
            var connection = ConnectionUtils.CreateConnection();
            var storage = new FirebirdStorage(connection, _options);

            Assert.NotNull(storage);
        }

        [Fact, CleanDatabase]
        public void Ctor_InitializesDefaultJobQueueProvider_AndPassesCorrectOptions()
        {
            var storage = CreateStorage();
            var providers = storage.QueueProviders;

            var provider = (FirebirdJobQueueProvider)providers.GetProvider("default");

            Assert.Same(_options, provider.Options);
        }

        [Fact, CleanDatabase]
        public void GetConnection_ReturnsExistingConnection_WhenStorageUsesIt()
        {
            var connection = ConnectionUtils.CreateConnection();
            var storage = new FirebirdStorage(connection, _options);

            using (var storageConnection = (FirebirdConnection)storage.GetConnection())
            {
                Assert.Same(connection, storageConnection.Connection);
                Assert.False(storageConnection.OwnsConnection);
            }
        }

        [Fact, CleanDatabase]
        public void GetMonitoringApi_ReturnsNonNullInstance()
        {
            var storage = CreateStorage();
            var api = storage.GetMonitoringApi();
            Assert.NotNull(api);
        }

        [Fact, CleanDatabase]
        public void GetConnection_ReturnsNonNullInstance()
        {
            var storage = CreateStorage();
            using (var connection = (FirebirdConnection)storage.GetConnection())
            {
                Assert.NotNull(connection);
                Assert.True(connection.OwnsConnection);
            }
        }

        [Fact, CleanDatabase]
        public void GetComponents_ReturnsAllNeededComponents()
        {
            var storage = CreateStorage();

            var components = storage.GetComponents();

            var componentTypes = components.Select(x => x.GetType()).ToArray();
            Assert.Contains(typeof(ExpirationManager), componentTypes);
        }

        private FirebirdStorage CreateStorage()
        {
            return new FirebirdStorage(
                ConnectionUtils.GetConnectionString(),
                _options);
        }
    }
}
