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
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Reflection;
using Dapper;
using Hangfire.Logging;
using FirebirdSql.Data.FirebirdClient;
using FirebirdSql.Data.Isql;

namespace Hangfire.Firebird
{
    [ExcludeFromCodeCoverage]
    internal static class FirebirdObjectsInstaller
    {
        private static readonly ILog Log = LogProvider.GetLogger(typeof(FirebirdStorage));

        public static void Install(FbConnection connection)
        {
            if (connection == null) throw new ArgumentNullException("connection");

            Log.Info("Start installing Hangfire SQL objects...");

            int version = 1;
            bool scriptFound = true;

            do
            {
                try
                {
                    var script = GetStringResource(
                        typeof(FirebirdObjectsInstaller).Assembly,
                        string.Format("Hangfire.Firebird.Install.v{0}.sql",
                            version.ToString(CultureInfo.InvariantCulture)));

                    if (!VersionAlreadyApplied(connection, version))
                    {
                        FbScript fbScript = new FbScript(script);
                        fbScript.Parse();

                        FbBatchExecution fbBatch = new FbBatchExecution(connection, fbScript);
                        fbBatch.Execute(true);

                        UpdateVersion(connection, version);
                    }
                }
                catch (FbException)
                {
                    throw;
                }
                catch (Exception)
                {
                    scriptFound = false;
                }

                version++;
            } while (scriptFound);

            Log.Info("Hangfire SQL objects installed.");
        }

        private static string GetStringResource(Assembly assembly, string resourceName)
        {
            using (var stream = assembly.GetManifestResourceStream(resourceName))
            {
                if (stream == null)
                {
                    throw new InvalidOperationException(String.Format(
                        "Requested resource `{0}` was not found in the assembly `{1}`.",
                        resourceName,
                        assembly));
                }

                using (var reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }

        private static bool VersionAlreadyApplied(FbConnection connection, int version)
        {
            bool alreadyApplied = false;

            bool tableExists = Convert.ToBoolean(connection.ExecuteScalar(@"SELECT count(*) FROM rdb$relations where rdb$relation_name = 'HANGFIRE.SCHEMA';"));

            if (tableExists)
                alreadyApplied = Convert.ToBoolean(connection.ExecuteScalar(string.Format(@"SELECT 1 FROM ""HANGFIRE.SCHEMA"" WHERE ""VERSION"" = {0};", version)));
              
            return alreadyApplied;
        }

        private static void UpdateVersion(FbConnection connection, int version)
        {
            if (version == 1)
            {
                connection.Execute(string.Format(@"INSERT INTO ""HANGFIRE.SCHEMA"" (""VERSION"") VALUES ({0});", version));
            }
            else
            {
                connection.Execute(string.Format(@"UPDATE ""HANGFIRE.SCHEMA"" SET ""VERSION"" = {0};", version));
            }
        }
    }
}
