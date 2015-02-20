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
using FirebirdSql.Data.FirebirdClient;

namespace Hangfire.Firebird.Tests
{
    public static class ConnectionUtils
    {
        private const string ClientLibraryVariable = "Hangfire_Firebird_ClientLibrary";
        private const string DatabaseVariable = "Hangfire_Firebird_DatabaseName";
        private const string SchemaVariable = "Hangfire_Firebird_SchemaName";
        private const string ConnectionStringTemplateVariable = "Hangfire_Firebird_ConnectionStringTemplate";

        private const string DefaultClientLibrary = @"S:\Source\Hangfire.Firebird\Firebird\fbembed.dll";
        private const string DefaultDatabaseName = @"S:\Source\Hangfire.Firebird\HANGFIRE_TESTS.FDB";
        private const string DefaultConnectionStringTemplate
            = @"User=SYSDBA;Password=masterkey;Database={0};Packet Size=8192;DataSource=localhost;Port=3050;Dialect=3;Charset=NONE;ServerType=0";//;ClientLibrary={1};"; //Firebird embedded

        public static string GetClientLibrary()
        {
            return Environment.GetEnvironmentVariable(ClientLibraryVariable) ?? DefaultClientLibrary;
        }

        public static string GetDatabaseName()
        {
            return Environment.GetEnvironmentVariable(DatabaseVariable) ?? DefaultDatabaseName;
        }

        public static string GetConnectionString()
        {
            return String.Format(GetConnectionStringTemplate(), GetDatabaseName(), GetClientLibrary());
        }

        private static string GetConnectionStringTemplate()
        {
            return Environment.GetEnvironmentVariable(ConnectionStringTemplateVariable)
                   ?? DefaultConnectionStringTemplate;
        }

        public static FbConnection CreateConnection()
        {
            string test = GetConnectionString();
            var connection = new FbConnection(GetConnectionString());
            connection.Open();

            return connection;
        }
    }
}
