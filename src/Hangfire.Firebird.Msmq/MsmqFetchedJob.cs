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
using System.Messaging;
using Hangfire.Storage;

namespace Hangfire.Firebird.Msmq
{
    internal class MsmqFetchedJob : IFetchedJob
    {
        private readonly MessageQueueTransaction _transaction;

        public MsmqFetchedJob(MessageQueueTransaction transaction, string jobId)
        {
            if (transaction == null) throw new ArgumentNullException("transaction");
            if (jobId == null) throw new ArgumentNullException("jobId");

            _transaction = transaction;

            JobId = jobId;
        }

        public string JobId { get; private set; }

        public void RemoveFromQueue()
        {
            _transaction.Commit();
        }

        public void Requeue()
        {
            _transaction.Abort();
        }

        public void Dispose()
        {
            _transaction.Dispose();
        }
    }
}
