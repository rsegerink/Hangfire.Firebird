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
using Hangfire.States;

namespace Hangfire.Firebird.Msmq
{
    public static class MsmqSqlServerStorageExtensions
    {
        public static FirebirdStorage UseMsmqQueues(this FirebirdStorage storage, string pathPattern)
        {
            return UseMsmqQueues(storage, pathPattern, new []{ EnqueuedState.DefaultQueue });
        }

        public static FirebirdStorage UseMsmqQueues(this FirebirdStorage storage, string pathPattern, params string[] queues)
        {
            if (storage == null) throw new ArgumentNullException("storage");

            var provider = new MsmqJobQueueProvider(pathPattern, queues);
            storage.QueueProviders.Add(provider, queues);

            return storage;
        }
    }
}