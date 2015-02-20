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
using System.Collections;
using System.Collections.Generic;

namespace Hangfire.Firebird
{
    public class PersistentJobQueueProviderCollection : IEnumerable<IPersistentJobQueueProvider>
    {
        private readonly List<IPersistentJobQueueProvider> _providers
            = new List<IPersistentJobQueueProvider>(); 
        private readonly Dictionary<string, IPersistentJobQueueProvider> _providersByQueue
            = new Dictionary<string, IPersistentJobQueueProvider>(StringComparer.OrdinalIgnoreCase);

        private readonly IPersistentJobQueueProvider _defaultProvider;

        public PersistentJobQueueProviderCollection(IPersistentJobQueueProvider defaultProvider)
        {
            if (defaultProvider == null) throw new ArgumentNullException("defaultProvider");

            _defaultProvider = defaultProvider;

            _providers.Add(_defaultProvider);
        }

        public void Add(IPersistentJobQueueProvider provider, IEnumerable<string> queues)
        {
            if (provider == null) throw new ArgumentNullException("provider");
            if (queues == null) throw new ArgumentNullException("queues");

            _providers.Add(provider);

            foreach (var queue in queues)
            {
                _providersByQueue.Add(queue, provider);
            }
        }

        public IPersistentJobQueueProvider GetProvider(string queue)
        {
            return _providersByQueue.ContainsKey(queue) 
                ? _providersByQueue[queue]
                : _defaultProvider;
        }

        public IEnumerator<IPersistentJobQueueProvider> GetEnumerator()
        {
            return _providers.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}