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
using Dapper;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using FirebirdSql.Data.FirebirdClient;

namespace Hangfire.Firebird
{
    internal class FirebirdWriteOnlyTransaction : IWriteOnlyTransaction
    {
        private readonly Queue<Action<FbConnection, FbTransaction>> _commandQueue
            = new Queue<Action<FbConnection, FbTransaction>>();

        private readonly FbConnection _connection;
        private readonly PersistentJobQueueProviderCollection _queueProviders;
        private readonly FirebirdStorageOptions _options;

        public FirebirdWriteOnlyTransaction( 
            FbConnection connection,
            FirebirdStorageOptions options,
            PersistentJobQueueProviderCollection queueProviders)
        {
            if (connection == null) throw new ArgumentNullException("connection");
            if (queueProviders == null) throw new ArgumentNullException("queueProviders");
            if (options == null) throw new ArgumentNullException("options");

            _connection = connection;
            _options = options;
            _queueProviders = queueProviders;
        }

        public void Dispose()
        {
        }

        public void Commit()
        {
            using (var transaction = _connection.BeginTransaction(IsolationLevel.RepeatableRead))
            {
                foreach (var command in _commandQueue)
                {
                    try
                    {
                        command(_connection, transaction);
                    }
                    catch
                    {
                        throw;
                    }
                }
                transaction.Commit();
            }
        }

        public void ExpireJob(string jobId, TimeSpan expireIn)
        {
            string sql = string.Format(@"UPDATE ""{0}.JOB"" SET expireat = @expireAt WHERE id = @id;", _options.Prefix);

            QueueCommand((con, trx) => con.Execute(
                sql,
                new { expireAt = DateTime.UtcNow.Add(expireIn), id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture) }, trx));
        }

        public void PersistJob(string jobId)
        {
            string sql = string.Format(@"UPDATE ""{0}.JOB"" SET expireat = NULL WHERE id = @id;", _options.Prefix);

            QueueCommand((con, trx) => con.Execute(
                sql,
                new { id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture) }, trx));
        }

        public void SetJobState(string jobId, IState state)
        {
            string addAndSetStateSql = string.Format(@"
                EXECUTE BLOCK (jobid INTEGER = @jobid,
                    name VARCHAR(20) CHARACTER SET UNICODE_FSS = @name,
                    reason VARCHAR(100) CHARACTER SET UNICODE_FSS = @reason,
                    createdat TIMESTAMP = @createdat,
                    data BLOB SUB_TYPE 1 SEGMENT SIZE 80 CHARACTER SET UNICODE_FSS = @data,
                    id INTEGER = @id)
                AS 
                    DECLARE VARIABLE stateid int; 
                BEGIN
                    INSERT INTO ""{0}.STATE"" (jobid, name, reason, createdat, data) 
                    VALUES (:jobid, :name, :reason, :createdat, :data) RETURNING id INTO :stateid;
                    
                    UPDATE ""{0}.JOB"" 
                    SET stateid = :stateid, statename = :name WHERE id = :id;
                    
                    SUSPEND;
                END", _options.Prefix);
                                
            QueueCommand((con, trx) => con.Execute(
                addAndSetStateSql,
                new
                {
                    jobId = jobId,
                    name = state.Name,
                    reason = state.Reason,
                    createdAt = DateTime.UtcNow,
                    data = JobHelper.ToJson(state.SerializeData()),
                    id = jobId
                }, trx));
        }

        public void AddJobState(string jobId, IState state)
        {
            string addStateSql = string.Format(@"INSERT INTO ""{0}.STATE"" (jobid, name, reason, createdat, data)
                VALUES (@jobId, @name, @reason, @createdAt, @data);", _options.Prefix);

            QueueCommand((con, trx) => con.Execute(
                addStateSql,
                new
                {
                    jobId = jobId, 
                    name = state.Name,
                    reason = state.Reason,
                    createdAt = DateTime.UtcNow, 
                    data = JobHelper.ToJson(state.SerializeData())
                }, trx));
        }

        public void AddToQueue(string queue, string jobId)
        {
            var provider = _queueProviders.GetProvider(queue);
            var persistentQueue = provider.GetJobQueue(_connection);

            QueueCommand((con, trx) => persistentQueue.Enqueue(trx, queue, jobId));
        }

        public void IncrementCounter(string key)
        {
            QueueCommand((con, trx) => con.Execute(
                string.Format(@"INSERT INTO ""{0}.COUNTER"" (""KEY"", ""VALUE"") VALUES (@key, @value);", _options.Prefix),
                new { key, value = +1 }, trx));
        }

        public void IncrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand((con, trx) => con.Execute(
                string.Format(@"INSERT INTO ""{0}.COUNTER"" (""KEY"", ""VALUE"", expireat) VALUES (@key, @value, @expireAt);", _options.Prefix),
                new { key, value = +1, expireAt = DateTime.UtcNow.Add(expireIn) }, trx));
        }

        public void DecrementCounter(string key)
        {
            QueueCommand((con, trx) => con.Execute(
                string.Format(@"INSERT INTO ""{0}.COUNTER"" (""KEY"", ""VALUE"") VALUES (@key, @value);", _options.Prefix),
                new { key, value = -1 }, trx));
        }

        public void DecrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand((con, trx) => con.Execute(
                string.Format(@"INSERT INTO ""{0}.COUNTER"" (""KEY"", ""VALUE"", expireat) VALUES (@key, @value, @expireAt);", _options.Prefix),
                new { key, value = -1, expireAt = DateTime.UtcNow.Add(expireIn) }, trx));
        }

        public void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public void AddToSet(string key, string value, double score)
        {
            string addSql = string.Format(@"
                MERGE INTO ""{0}.SET"" target
                USING (SELECT CAST(@key AS VARCHAR(100) CHARACTER SET UNICODE_FSS), CAST(@value AS VARCHAR(256) CHARACTER SET UNICODE_FSS), CAST(@score AS FLOAT) FROM rdb$database) source(""KEY"", ""VALUE"", score)
                ON target.""KEY"" = source.""KEY"" AND target.""VALUE"" = source.""VALUE""
                WHEN MATCHED THEN UPDATE SET target.score = source.score
                WHEN NOT MATCHED THEN INSERT (""KEY"", ""VALUE"", score) VALUES (source.""KEY"", source.""VALUE"", source.score);", _options.Prefix);

            QueueCommand((con, trx) => con.Execute(
                addSql,
                new { key, value, score }, trx));
        }

        public void RemoveFromSet(string key, string value)
        {
            QueueCommand((con, trx) => con.Execute(
                string.Format(@"DELETE FROM ""{0}.SET"" WHERE ""KEY"" = @key AND ""VALUE"" = @value;", _options.Prefix),
                new { key, value }, trx));
        }

        public void InsertToList(string key, string value)
        {
            QueueCommand((con, trx) => con.Execute(
                string.Format(@"INSERT INTO ""{0}.LIST"" (""KEY"", ""VALUE"") VALUES (@key, @value);", _options.Prefix),
                new { key, value }, trx));
        }

        public void RemoveFromList(string key, string value)
        {
            QueueCommand((con, trx) => con.Execute(
                string.Format(@"DELETE FROM ""{0}.LIST"" WHERE ""KEY"" = @key AND ""VALUE"" = @value;", _options.Prefix),
                new { key, value }, trx));
        }

        public void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            string trimSql = string.Format(@"
                EXECUTE BLOCK (""key"" VARCHAR(100) CHARACTER SET UNICODE_FSS = @key, ""start"" int = @start, ""end"" int = @end) 
                AS
                BEGIN
                    UPDATE ""{0}.LIST"" source
                    SET source.remove = 1
                    WHERE source.""KEY"" = :""key""
                    AND source.id NOT IN (
                        SELECT keep.id
                        FROM ""{0}.LIST"" keep
                        WHERE keep.""KEY"" = source.""KEY""
                        ORDER BY keep.id
                        ROWS :""start"" TO :""end"");

                    DELETE FROM ""{0}.LIST""
                    WHERE remove = 1; 
                    
                    SUSPEND;
                END", _options.Prefix);                       

            QueueCommand((con, trx) => con.Execute(
                trimSql,
                new { key = key, start = keepStartingFrom + 1, end = keepEndingAt + 1 }, trx));
        }

        public void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (keyValuePairs == null) throw new ArgumentNullException("keyValuePairs");

            string sql = string.Format(@"
                MERGE INTO ""{0}.HASH"" target
                USING (SELECT CAST(@key AS VARCHAR(100) CHARACTER SET UNICODE_FSS), CAST(@field AS VARCHAR(100) CHARACTER SET UNICODE_FSS), CAST(@value AS BLOB SUB_TYPE 1 SEGMENT SIZE 80 CHARACTER SET UNICODE_FSS) FROM rdb$database) source(""KEY"", field, ""VALUE"")
                ON target.""KEY"" = source.""KEY"" AND target.field = source.field
                WHEN MATCHED THEN UPDATE SET ""VALUE"" = source.""VALUE""
                WHEN NOT MATCHED THEN INSERT (""KEY"", field, ""VALUE"") VALUES (source.""KEY"", source.field, source.""VALUE"");", _options.Prefix);

            foreach (var keyValuePair in keyValuePairs)
            {
                var pair = keyValuePair;

                QueueCommand((con, trx) => con.Execute(sql, new { key = key, field = pair.Key, value = pair.Value }, trx));
            }
        }

        public void RemoveHash(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            QueueCommand((con, trx) => con.Execute(
                string.Format(@"DELETE FROM ""{0}.HASH"" WHERE ""KEY"" = @key;", _options.Prefix),
                new { key }, trx));
        }

        internal void QueueCommand(Action<FbConnection, FbTransaction> action)
        {
            _commandQueue.Enqueue(action);
        }
    }
}
