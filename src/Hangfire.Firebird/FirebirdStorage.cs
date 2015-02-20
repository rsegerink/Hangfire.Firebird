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
using System.Collections.Generic;
using System.Configuration;
using System.Text;
using System.IO;
using Hangfire.Annotations;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;
using FirebirdSql.Data.FirebirdClient;

namespace Hangfire.Firebird
{
    public class FirebirdStorage : JobStorage
    {
        private readonly FbConnection _existingConnection;
        private readonly FirebirdStorageOptions _options;
        private readonly string _connectionString;

        public FirebirdStorage(string nameOrConnectionString)
            : this(nameOrConnectionString, new FirebirdStorageOptions())
        {
        }

        /// <summary>
        /// Initializes FirebirdStorage from the provided FirebirdStorageOptions and either the provided connection
        /// string or the connection string with provided name pulled from the application config file.       
        /// </summary>
        /// <param name="nameOrConnectionString">Either a Firebird connection string or the name of 
        /// a Firebird connection string located in the connectionStrings node in the application config</param>
        /// <param name="options"></param>
        /// <exception cref="ArgumentNullException"><paramref name="nameOrConnectionString"/> argument is null.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="options"/> argument is null.</exception>
        /// <exception cref="ArgumentException"><paramref name="nameOrConnectionString"/> argument is neither 
        /// a valid Firebird connection string nor the name of a connection string in the application
        /// config file.</exception>
        public FirebirdStorage(string nameOrConnectionString, FirebirdStorageOptions options)
        {
            if (nameOrConnectionString == null) throw new ArgumentNullException("nameOrConnectionString");
            if (options == null) throw new ArgumentNullException("options");

            _options = options;

            if (IsConnectionString(nameOrConnectionString))
            {
                _connectionString = nameOrConnectionString;
            }
            else if (IsConnectionStringInConfiguration(nameOrConnectionString))
            {
                _connectionString = ConfigurationManager.ConnectionStrings[nameOrConnectionString].ConnectionString;
            }
            else
            {
                throw new ArgumentException(
                    string.Format("Could not find connection string with name '{0}' in application config file",
                                  nameOrConnectionString));
            }

            if (options.PrepareSchemaIfNecessary)
            {
                var connectionStringBuilder = new FbConnectionStringBuilder(_connectionString);
                if (!File.Exists(connectionStringBuilder.Database))
                    FbConnection.CreateDatabase(_connectionString, 16384, true, false);

                using (var connection = CreateAndOpenConnection())
                {
                    FirebirdObjectsInstaller.Install(connection);
                }
            }

            InitializeQueueProviders();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FirebirdStorage"/> class with
        /// explicit instance of the <see cref="FbConnection"/> class that will be used
        /// to query the data.
        /// </summary>
        /// <param name="existingConnection">Existing connection</param>
        /// <param name="options">FirebirdStorageOptions</param>
        public FirebirdStorage(FbConnection existingConnection, FirebirdStorageOptions options)
        {
            if (existingConnection == null) throw new ArgumentNullException("existingConnection");
            if (options == null) throw new ArgumentNullException("options");
            //var connectionStringBuilder = new FbConnectionStringBuilder(existingConnection.ConnectionString);
            //if (connectionStringBuilder.Enlist) throw new ArgumentException("FirebirdSql is not fully compatible with TransactionScope yet, only connections without Enlist = true are accepted.");

            _existingConnection = existingConnection;
            _options = new FirebirdStorageOptions();

            InitializeQueueProviders();
        }

        public PersistentJobQueueProviderCollection QueueProviders { get; private set; }

        public override IMonitoringApi GetMonitoringApi()
        {
            return new FirebirdMonitoringApi(_connectionString, _options, QueueProviders);
        }

        public override IStorageConnection GetConnection()
        {
            var connection = _existingConnection ?? CreateAndOpenConnection();
            return new FirebirdConnection(connection, QueueProviders, _options, _existingConnection == null);
        }

        public override IEnumerable<IServerComponent> GetComponents()
        {
            yield return new ExpirationManager(this, _options);
        }

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for SQL Server job storage:");
            logger.InfoFormat("    Queue poll interval: {0}.", _options.QueuePollInterval);
            logger.InfoFormat("    Invisibility timeout: {0}.", _options.InvisibilityTimeout);
        }

        public override string ToString()
        {
            const string canNotParseMessage = "<Connection string can not be parsed>";

            try
            {
                var connectionStringBuilder = new FbConnectionStringBuilder(_connectionString);
                var builder = new StringBuilder();

                builder.Append("Data Source: ");
                builder.Append(connectionStringBuilder.DataSource);
                builder.Append(", Server Type: ");
                builder.Append(connectionStringBuilder.ServerType);
                builder.Append(", Database: ");
                builder.Append(connectionStringBuilder.Database);

                return builder.Length != 0
                    ? string.Format("Firebird Server: {0}", builder)
                    : canNotParseMessage;
            }
            catch (Exception)
            {
                return canNotParseMessage;
            }
        }

        internal FbConnection CreateAndOpenConnection()
        {
            var connection = new FbConnection(_connectionString);
            connection.Open();

            return connection;
        }

        private void InitializeQueueProviders()
        {
            var defaultQueueProvider = new FirebirdJobQueueProvider(_options);
            QueueProviders = new PersistentJobQueueProviderCollection(defaultQueueProvider);
        }

        private bool IsConnectionString(string nameOrConnectionString)
        {
            return nameOrConnectionString.Contains(";");
        }

        private bool IsConnectionStringInConfiguration(string connectionStringName)
        {
            var connectionStringSetting = ConfigurationManager.ConnectionStrings[connectionStringName];

            return connectionStringSetting != null;
        }
    }
}
