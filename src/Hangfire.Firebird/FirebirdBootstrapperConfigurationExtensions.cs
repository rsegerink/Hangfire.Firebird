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

namespace Hangfire.Firebird
{
    public static class FirebirdBootstrapperConfigurationExtensions
    {
        /// <summary>
        /// Tells the bootstrapper to use Fireibrd as a job storage,
        /// that can be accessed using the given connection string or 
        /// its name.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="nameOrConnectionString">Connection string or its name</param>
        public static FirebirdStorage UseFirebirdStorage(
            this IBootstrapperConfiguration configuration,
            string nameOrConnectionString)
        {
            var storage = new FirebirdStorage(nameOrConnectionString);
            configuration.UseStorage(storage);

            return storage;
        }

        /// <summary>
        /// Tells the bootstrapper to use Firebird as a job storage
        /// with the given options, that can be accessed using the specified
        /// connection string or its name.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="nameOrConnectionString">Connection string or its name</param>
        /// <param name="options">Advanced options</param>
        public static FirebirdStorage UseFirebirdStorage(
            this IBootstrapperConfiguration configuration,
            string nameOrConnectionString,
            FirebirdStorageOptions options)
        {
            var storage = new FirebirdStorage(nameOrConnectionString, options);
            configuration.UseStorage(storage);

            return storage;
        }
    }
}
