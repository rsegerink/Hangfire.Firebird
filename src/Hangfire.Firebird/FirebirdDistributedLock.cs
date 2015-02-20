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
using System.Diagnostics;
using System.Threading;
using Dapper;

namespace Hangfire.Firebird
{
    internal class FirebirdDistributedLock : IDisposable
    {
        private readonly IDbConnection _connection;
        private readonly string _resource;
        private readonly FirebirdStorageOptions _options;
        private bool _completed;

        public FirebirdDistributedLock(string resource, TimeSpan timeout, IDbConnection connection,
            FirebirdStorageOptions options)
        {
            if (String.IsNullOrEmpty(resource)) throw new ArgumentNullException("resource");
            if (connection == null) throw new ArgumentNullException("connection");
            if (options == null) throw new ArgumentNullException("options");

            _resource = resource;
            _connection = connection;
            _options = options;

            Init(resource, timeout, connection, options);
        }

        private void Init(string resource, TimeSpan timeout, IDbConnection connection, FirebirdStorageOptions options)
        {
            Stopwatch lockAcquiringTime = new Stopwatch();
            lockAcquiringTime.Start();

            bool tryAcquireLock = true;

            while (tryAcquireLock)
            {
                try
                {
                    int rowsAffected = -1;
                    using (var trx = _connection.BeginTransaction(IsolationLevel.RepeatableRead))
                    {
                        rowsAffected = _connection.Execute(string.Format(@"
                            INSERT INTO ""{0}.LOCK"" (resource) 
                            SELECT @resource
                            FROM rdb$database
                            WHERE NOT EXISTS (
                                SELECT 1 FROM ""{0}.LOCK"" 
                                WHERE resource = @resource
                            );
                            ", _options.Prefix), new
                             {
                                 resource = resource
                             }, trx);

                        trx.Commit();
                    }
                    if (rowsAffected > 0) return;
                }
                catch (Exception)
                {
                }

                if (lockAcquiringTime.ElapsedMilliseconds > timeout.TotalMilliseconds)
                    tryAcquireLock = false;
                else
                {
                    int sleepDuration = (int)(timeout.TotalMilliseconds - lockAcquiringTime.ElapsedMilliseconds);
                    if (sleepDuration > 1000) sleepDuration = 1000;
                    if (sleepDuration > 0)
                        Thread.Sleep(sleepDuration);
                    else
                        tryAcquireLock = false;
                }
            }

            throw new FirebirdDistributedLockException(
                string.Format(
                "Could not place a lock on the resource '{0}': {1}.",
                _resource,
                "Lock timeout"));

        }

        public void Dispose()
        {
            if (_completed) return;

            _completed = true;

            int rowsAffected = _connection.Execute(string.Format(@"
                DELETE FROM ""{0}.LOCK"" 
                WHERE resource = @resource;
                ", _options.Prefix), new
                 {
                     resource = _resource
                 });


            if (rowsAffected <= 0)
            {
                throw new FirebirdDistributedLockException(
                    string.Format(
                        "Could not release a lock on the resource '{0}'. Lock does not exists.",
                        _resource));
            }
        }
    }
}
