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
using System.Data;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.Server;

namespace Hangfire.Firebird
{
    internal class ExpirationManager : IServerComponent
    {
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromSeconds(1);
        private const int NumberOfRecordsInSinglePass = 1000;

        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();
        private static readonly string[] ProcessedTables =
        {
            "COUNTER",
            "JOB",
            "LIST",
            "SET",
            "HASH",
        };

        private readonly FirebirdStorage _storage;
        private readonly TimeSpan _checkInterval;
        private readonly FirebirdStorageOptions _options;

        public ExpirationManager(FirebirdStorage storage, FirebirdStorageOptions options)
            : this(storage, options, TimeSpan.FromHours(1))
        {
        }

        public ExpirationManager(FirebirdStorage storage, FirebirdStorageOptions options, TimeSpan checkInterval)
        {
            if (storage == null) throw new ArgumentNullException("storage");
            if (options == null) throw new ArgumentNullException("options");

            _options = options;
            _storage = storage;
            _checkInterval = checkInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            
            foreach (var table in ProcessedTables)
            {
                Logger.DebugFormat("Removing outdated records from table '{0}'...", table);

                int removedCount = 0;

                do
                {
                    using (var storageConnection = (FirebirdConnection)_storage.GetConnection())
                    {
                        using (var transaction = storageConnection.Connection.BeginTransaction(IsolationLevel.ReadCommitted))
                        {
                            removedCount = storageConnection.Connection.Execute(
                                string.Format(@"
                                    DELETE 
                                    FROM ""{0}.{1}""
                                    WHERE expireat < @now
                                    ROWS @count;", _options.Prefix, table),
                                new { now = DateTime.UtcNow, count = NumberOfRecordsInSinglePass }, transaction);

                            transaction.Commit();
                        }
                    }

                    if (removedCount > 0)
                    {
                        Logger.Info(String.Format("Removed {0} outdated record(s) from '{1}' table.", removedCount,
                            table));

                        cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                } while (removedCount != 0);
            }

            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }

        public override string ToString()
        {
            return "SQL Records Expiration Manager";
        }
    }
}
