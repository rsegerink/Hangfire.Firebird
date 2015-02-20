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
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Firebird.Entities;
using Hangfire.Storage;
using FirebirdSql.Data.FirebirdClient;

namespace Hangfire.Firebird
{
    internal class FirebirdConnection : IStorageConnection
    {
        private readonly FbConnection _connection;
        private readonly PersistentJobQueueProviderCollection _queueProviders;
        private readonly FirebirdStorageOptions _options;

        public FirebirdConnection(
            FbConnection connection,
            PersistentJobQueueProviderCollection queueProviders,
            FirebirdStorageOptions options)
            : this(connection, queueProviders, options, true)
        {
        }

        public FirebirdConnection(
            FbConnection connection, 
            PersistentJobQueueProviderCollection queueProviders,
            FirebirdStorageOptions options,
            bool ownsConnection)
        {
            if (connection == null) throw new ArgumentNullException("connection");
            if (queueProviders == null) throw new ArgumentNullException("queueProviders");
            if (options == null) throw new ArgumentNullException("options");

            _connection = connection;
            _queueProviders = queueProviders;
            _options = options;
            OwnsConnection = ownsConnection;
        }

        public FbConnection Connection { get { return _connection; } }
        public bool OwnsConnection { get; private set; }

        public void Dispose()
        {
            if (OwnsConnection)
            {
                _connection.Dispose();
            }
        }

        public IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new FirebirdWriteOnlyTransaction(_connection, _options, _queueProviders);
        }

        public IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return new FirebirdDistributedLock(
                String.Format("HangFire:{0}", resource),
                timeout,
                _connection,
                _options);
        }

        public IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0) throw new ArgumentNullException("queues");

            var providers = queues
                .Select(queue => _queueProviders.GetProvider(queue))
                .Distinct()
                .ToArray();

            if (providers.Length != 1)
            {
                throw new InvalidOperationException(String.Format(
                    "Multiple provider instances registered for queues: {0}. You should choose only one type of persistent queues per server instance.",
                    String.Join(", ", queues)));
            }

            var persistentQueue = providers[0].GetJobQueue(_connection);
            return persistentQueue.Dequeue(queues, cancellationToken);
        }

        public string CreateExpiredJob(
            Job job,
            IDictionary<string, string> parameters, 
            DateTime createdAt,
            TimeSpan expireIn)
        {
            if (job == null) throw new ArgumentNullException("job");
            if (parameters == null) throw new ArgumentNullException("parameters");

            string createJobSql = string.Format(@"INSERT INTO ""{0}.JOB"" (invocationdata, arguments, createdat, expireat) 
                VALUES (@invocationData, @arguments, @createdAt, @expireAt)
                RETURNING id;", _options.Prefix);

            var invocationData = InvocationData.Serialize(job);

            var jobId = _connection.ExecuteScalar<int>(
                createJobSql,
                new
                {
                    invocationData = JobHelper.ToJson(invocationData),
                    arguments = invocationData.Arguments,
                    createdAt = createdAt,
                    expireAt = createdAt.Add(expireIn)
                }).ToString();
               
            if (parameters.Count > 0)
            {
                var parameterArray = new object[parameters.Count];
                int parameterIndex = 0;
                foreach (var parameter in parameters)
                {
                    parameterArray[parameterIndex++] = new
                    {
                        jobId = Convert.ToInt32(jobId, CultureInfo.InvariantCulture),
                        name = parameter.Key,
                        value = parameter.Value
                    };
                }

                string insertParameterSql = string.Format(@"INSERT INTO ""{0}.JOBPARAMETER"" (jobid, name, ""VALUE"")
                    VALUES (@jobId, @name, @value);", _options.Prefix);

                _connection.Execute(insertParameterSql, parameterArray);
            }

            return jobId;
        }

        public JobData GetJobData(string id)
        {
            if (id == null) throw new ArgumentNullException("id");

            string sql = string.Format(@"SELECT invocationdata, statename, arguments, createdat
                FROM ""{0}.JOB"" 
                WHERE id = @id;", _options.Prefix);

            var jobData = _connection.Query<SqlJob>(sql, new { id = Convert.ToInt32(id, CultureInfo.InvariantCulture) })
                .SingleOrDefault();

            if (jobData == null) return null;

            // TODO: conversion exception could be thrown.
            var invocationData = JobHelper.FromJson<InvocationData>(jobData.InvocationData);
            invocationData.Arguments = jobData.Arguments;

            Job job = null;
            JobLoadException loadException = null;

            try
            {
                job = invocationData.Deserialize();
            }
            catch (JobLoadException ex)
            {
                loadException = ex;
            }

            return new JobData
            {
                Job = job,
                State = jobData.StateName,
                CreatedAt = jobData.CreatedAt,
                LoadException = loadException
            };
        }

        public StateData GetStateData(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException("jobId");

            string sql = string.Format(@"SELECT s.name, s.reason, s.data
                FROM ""{0}.STATE"" s
                INNER JOIN ""{0}.JOB"" j on j.stateid = s.id
                WHERE j.id = @jobid", _options.Prefix);

            var sqlState = _connection.Query<SqlState>(sql, new { jobId = Convert.ToInt32(jobId, CultureInfo.InvariantCulture) }).SingleOrDefault();
            if (sqlState == null)
            {
                return null;
            }

            var data = new Dictionary<string, string>(
                JobHelper.FromJson<Dictionary<string, string>>(sqlState.Data),
                StringComparer.OrdinalIgnoreCase);

            return new StateData
            {
                Name = sqlState.Name,
                Reason = sqlState.Reason,
                Data = data
            };
        }

        public void SetJobParameter(string id, string name, string value)
        {
            if (id == null) throw new ArgumentNullException("id");
            if (name == null) throw new ArgumentNullException("name");

            _connection.Execute(string.Format(@"MERGE INTO ""{0}.JOBPARAMETER"" target
                USING (SELECT CAST(@jobId AS INTEGER), CAST(@name AS VARCHAR(40) CHARACTER SET UNICODE_FSS), CAST(@value AS BLOB SUB_TYPE 1 SEGMENT SIZE 80 CHARACTER SET UNICODE_FSS) FROM rdb$database) source (jobid, name, ""VALUE"")
                ON target.jobid = source.jobid AND target.name = source.name
                WHEN MATCHED THEN UPDATE SET target.""VALUE"" = source.""VALUE""
                WHEN NOT MATCHED THEN INSERT (jobid, name, ""VALUE"") VALUES (source.jobid, source.name, source.""VALUE"");", _options.Prefix),
                new { jobId = Convert.ToInt32(id, CultureInfo.InvariantCulture), name, value });
        }

        public string GetJobParameter(string id, string name)
        {
            if (id == null) throw new ArgumentNullException("id");
            if (name == null) throw new ArgumentNullException("name");

            return _connection.Query<string>(
                string.Format(@"SELECT ""VALUE"" FROM ""{0}.JOBPARAMETER"" WHERE jobid = @id AND name = @name;", _options.Prefix),
                new { id = Convert.ToInt32(id, CultureInfo.InvariantCulture), name = name })
                .SingleOrDefault();
        }

        public HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            var result = _connection.Query<string>(
                string.Format(@"SELECT ""VALUE"" FROM ""{0}.SET"" WHERE ""KEY"" = @key", _options.Prefix),
                new { key });
            
            return new HashSet<string>(result);
        }

        public string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (toScore < fromScore) throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");

            return _connection.Query<string>(
                string.Format(@"SELECT ""VALUE"" FROM ""{0}.SET"" WHERE ""KEY"" = @key AND score BETWEEN @from AND @to ORDER BY score ROWS 1;", _options.Prefix),
                new { key, from = fromScore, to = toScore })
                .SingleOrDefault();
        }

        public void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (keyValuePairs == null) throw new ArgumentNullException("keyValuePairs");

            string sql = string.Format(@"
                MERGE INTO ""{0}.HASH"" target
                USING (SELECT CAST(@key AS VARCHAR(100) CHARACTER SET UNICODE_FSS), CAST(@field AS VARCHAR(100) CHARACTER SET UNICODE_FSS), CAST(@value AS BLOB SUB_TYPE 1 SEGMENT SIZE 80 CHARACTER SET UNICODE_FSS) FROM rdb$database) source(""KEY"", field, ""VALUE"") 
                ON target.""KEY"" = source.""KEY"" AND target.field = source.field
                WHEN MATCHED THEN UPDATE SET target.""VALUE"" = source.""VALUE""
                WHEN NOT MATCHED THEN INSERT (""KEY"", field, ""VALUE"") VALUES (source.""KEY"", source.field, source.""VALUE"");", _options.Prefix);

            using (var transaction = _connection.BeginTransaction(IsolationLevel.Serializable))
            {
                foreach (var keyValuePair in keyValuePairs)
                {
                    _connection.Execute(sql, new { key = key, field = keyValuePair.Key, value = keyValuePair.Value }, transaction);
                }

                transaction.Commit();
            }
        }

        public Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            var result = _connection.Query<SqlHash>(
                string.Format(@"SELECT ""FIELD"", ""VALUE"" FROM ""{0}.HASH"" WHERE ""KEY"" = @key", _options.Prefix),
                new { key })
                .ToDictionary(x => x.Field, x => x.Value);

            return result.Count != 0 ? result : null;
        }

        public void AnnounceServer(string serverId, ServerContext context)
        {
            if (serverId == null) throw new ArgumentNullException("serverId");
            if (context == null) throw new ArgumentNullException("context");

            var data = new ServerData
            {
                WorkerCount = context.WorkerCount,
                Queues = context.Queues,
                StartedAt = DateTime.UtcNow,
            };

            _connection.Execute(string.Format(@"
                MERGE INTO ""{0}.SERVER"" target
                USING (SELECT CAST(@id AS VARCHAR(50) CHARACTER SET UNICODE_FSS), CAST(@data AS BLOB SUB_TYPE 1 SEGMENT SIZE 80 CHARACTER SET UNICODE_FSS), CAST(@heartbeat AS TIMESTAMP) FROM rdb$database) source (id, data, heartbeat)
                ON target.id = source.id
                WHEN MATCHED THEN UPDATE SET target.data = source.data, target.lastheartbeat = source.heartbeat
                WHEN NOT MATCHED THEN INSERT (id, data, lastheartbeat) VALUES (source.id, source.data, source.heartbeat);", _options.Prefix),
                new { id = serverId, data = JobHelper.ToJson(data), heartbeat = DateTime.UtcNow });
        }

        public void RemoveServer(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException("serverId");

            _connection.Execute(
                string.Format(@"DELETE FROM ""{0}.SERVER"" WHERE id = @id;", _options.Prefix),
                new { id = serverId });
        }

        public void Heartbeat(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException("serverId");

            _connection.Execute(
                string.Format(@"UPDATE ""{0}.SERVER"" SET lastheartbeat = @now WHERE id = @id;", _options.Prefix),
                new { now = DateTime.UtcNow, id = serverId });
        }

        public int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException("The `timeOut` value must be positive.", "timeOut");
            }

            return _connection.Execute(
                string.Format(@"DELETE FROM ""{0}.SERVER"" WHERE lastheartbeat < @timeOutAt", _options.Prefix),
                new { timeOutAt = DateTime.UtcNow.Add(timeOut.Negate()) });
        }
    }
}
