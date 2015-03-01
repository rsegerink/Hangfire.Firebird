using System;
using System.Linq;
using Hangfire.Firebird;
using Hangfire.Firebird.Msmq;
using Hangfire.States;
using Xunit;

namespace Hangfire.Msmq.Tests
{
    public class MsmqSqlServerStorageExtensionsFacts
    {
        private readonly FirebirdStorage _storage;

        public MsmqSqlServerStorageExtensionsFacts()
        {
             _storage = new FirebirdStorage(
                @"User=SYSDBA;Password=masterkey;Database=S:\Source\Hangfire.Firebird\HANGFIRE_MSMQ_TESTS.FDB;Packet Size=8192;DataSource=localhost;Port=3050;Dialect=3;Charset=NONE;ServerType=1;ClientLibrary=S:\Source\Hangfire.Firebird\Firebird\fbembed.dll;",
                new FirebirdStorageOptions { PrepareSchemaIfNecessary = false });
        }

        [Fact]
        public void UseMsmqQueues_ThrowsAnException_WhenStorageIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => MsmqSqlServerStorageExtensions.UseMsmqQueues(null, CleanMsmqQueueAttribute.PathPattern));
            
            Assert.Equal("storage", exception.ParamName);
        }

        [Fact]
        public void UseMsmqQueues_AddsMsmqJobQueueProvider()
        {
            _storage.UseMsmqQueues(CleanMsmqQueueAttribute.PathPattern);

            var providerTypes = _storage.QueueProviders.Select(x => x.GetType());
            Assert.Contains(typeof(MsmqJobQueueProvider), providerTypes);
        }
    }
}
