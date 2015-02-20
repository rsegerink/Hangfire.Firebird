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

﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.IO;
using Dapper;
using FirebirdSql.Data.FirebirdClient;
using Xunit;

namespace Hangfire.Firebird.Tests
{
    public class FirebirdInstallerFacts
    {
        [Fact]
        public void InstallingSchemaShouldNotThrowAnException()
        {
            Assert.DoesNotThrow(() =>
            {
                DropDatabase();

                CreateDatabase();

                UseConnection(connection =>
                {
                    string prefix = "hangfire_tests_" + System.Guid.NewGuid().ToString().Replace("-", "_").ToLower();

                    FirebirdObjectsInstaller.Install(connection);
                });

                //DropDatabase();
            });
        }

        private static void CreateDatabase()
        {
            FbConnection.CreateDatabase(ConnectionUtils.GetConnectionString(), 16384, true, false);
        }

        private static void DropDatabase()
        {
            FbConnection.ClearAllPools();

            var connectionStringBuilder = new FbConnectionStringBuilder(ConnectionUtils.GetConnectionString());
            if (File.Exists(connectionStringBuilder.Database))
                FbConnection.DropDatabase(connectionStringBuilder.ConnectionString);
        }

        private static void UseConnection(Action<FbConnection> action)
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                action(connection);
            }
        }
    }
}
